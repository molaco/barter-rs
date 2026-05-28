use crate::{
    Identifier,
    books::{Level, OrderBook},
    error::DataError,
    event::MarketEvent,
    exchange::{Connector, ExchangeSub, kraken::Kraken},
    subscription::{
        Map,
        book::{OrderBookEvent, OrderBooksL2},
    },
    transformer::ExchangeTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use super::super::{channel::KrakenChannel, message::KrakenMessage};

/// Terse type alias for a Kraken v2 book WebSocket message.
pub type KrakenOrderBookL2 = KrakenMessage<KrakenBookPayload>;

/// Kraken v2 book data message.
///
/// ### Raw Payload Example (snapshot)
/// ```json
/// {
///     "channel": "book",
///     "type": "snapshot",
///     "data": [{
///         "symbol": "BTC/USD",
///         "bids": [{"price": 66650.0, "qty": 0.001}],
///         "asks": [{"price": 66651.0, "qty": 2.69}],
///         "checksum": 933610345,
///         "timestamp": "2026-03-27T12:17:06.420182Z"
///     }]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenBookPayload {
    pub channel: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: Vec<KrakenBookData>,
}

impl Identifier<Option<SubscriptionId>> for KrakenBookPayload {
    fn id(&self) -> Option<SubscriptionId> {
        self.data
            .first()
            .map(|d| ExchangeSub::from((KrakenChannel::ORDER_BOOK_L2, &d.symbol)).id())
    }
}

/// A single Kraken v2 book snapshot or update.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenBookData {
    pub symbol: String,
    pub bids: Vec<KrakenBookLevel>,
    pub asks: Vec<KrakenBookLevel>,
    pub checksum: u32,
    pub timestamp: DateTime<Utc>,
}

/// A Kraken v2 book level with numeric price and quantity.
#[derive(Clone, Copy, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenBookLevel {
    pub price: f64,
    pub qty: f64,
}

impl From<KrakenBookLevel> for Level {
    fn from(level: KrakenBookLevel) -> Self {
        Self {
            price: Decimal::try_from(level.price).unwrap_or_default(),
            amount: Decimal::try_from(level.qty).unwrap_or_default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Transformer
// ---------------------------------------------------------------------------

#[derive(Debug, Constructor)]
pub struct KrakenOrderBookL2Meta<InstrumentKey> {
    pub key: InstrumentKey,
    pub initialized: bool,
}

#[derive(Debug)]
pub struct KrakenOrderBooksL2Transformer<InstrumentKey> {
    instrument_map: Map<KrakenOrderBookL2Meta<InstrumentKey>>,
}

impl<InstrumentKey> ExchangeTransformer<Kraken, InstrumentKey, OrderBooksL2>
    for KrakenOrderBooksL2Transformer<InstrumentKey>
where
    InstrumentKey: Clone + PartialEq + Send + Sync,
{
    fn init(
        instrument_map: Map<InstrumentKey>,
        initial_snapshots: &[MarketEvent<InstrumentKey, OrderBookEvent>],
        _: UnboundedSender<WsMessage>,
    ) -> impl std::future::Future<Output = Result<Self, DataError>> + Send {
        let instrument_map = instrument_map
            .0
            .into_iter()
            .map(|(sub_id, instrument_key)| {
                let has_snapshot = initial_snapshots
                    .iter()
                    .any(|s| s.instrument == instrument_key);
                (
                    sub_id,
                    KrakenOrderBookL2Meta::new(instrument_key, has_snapshot),
                )
            })
            .collect();

        async move { Ok(Self { instrument_map }) }
    }

    fn insert_map_entries(&mut self, entries: Vec<(SubscriptionId, InstrumentKey)>) {
        for (id, key) in entries {
            if self.instrument_map.0.contains_key(&id) {
                continue;
            }
            self.instrument_map
                .insert(id, KrakenOrderBookL2Meta::new(key, false));
        }
    }

    fn remove_map_entries(&mut self, subscription_ids: &[SubscriptionId]) {
        for id in subscription_ids {
            self.instrument_map.remove(id);
        }
    }
}

impl<InstrumentKey> Transformer for KrakenOrderBooksL2Transformer<InstrumentKey>
where
    InstrumentKey: Clone,
{
    type Error = DataError;
    type Input = KrakenOrderBookL2;
    type Output = MarketEvent<InstrumentKey, OrderBookEvent>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let payload = match input {
            KrakenOrderBookL2::Data(payload) => payload,
            KrakenOrderBookL2::Event(_) => return vec![],
        };

        let subscription_id = match Identifier::<Option<SubscriptionId>>::id(&payload) {
            Some(id) => id,
            None => return vec![],
        };

        let instrument = match self.instrument_map.find_mut(&subscription_id) {
            Ok(instrument) => instrument,
            Err(unidentifiable) => return vec![Err(DataError::from(unidentifiable))],
        };

        let mut results = Vec::new();

        for book_data in payload.data {
            let is_snapshot = payload.msg_type == "snapshot";
            let event = if is_snapshot {
                instrument.initialized = true;
                OrderBookEvent::Snapshot(OrderBook::new(
                    0, // Kraken has no sequence number
                    Some(book_data.timestamp),
                    book_data.bids,
                    book_data.asks,
                ))
            } else {
                if !instrument.initialized {
                    tracing::debug!("Kraken book update before snapshot");
                    continue;
                }
                OrderBookEvent::Update(OrderBook::new(
                    0,
                    Some(book_data.timestamp),
                    book_data.bids,
                    book_data.asks,
                ))
            };

            results.push(Ok(MarketEvent {
                time_exchange: book_data.timestamp,
                time_received: Utc::now(),
                exchange: Kraken::ID,
                instrument: instrument.key.clone(),
                kind: event,
            }));
        }

        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_v2_book_snapshot_deserialize() {
        let input = r#"{
            "channel": "book",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [
                    {"price": 66650.0, "qty": 0.001},
                    {"price": 66649.0, "qty": 0.5}
                ],
                "asks": [
                    {"price": 66651.0, "qty": 2.69},
                    {"price": 66652.0, "qty": 1.0}
                ],
                "checksum": 933610345,
                "timestamp": "2026-03-27T12:17:06.420182Z"
            }]
        }"#;
        let msg: KrakenOrderBookL2 = serde_json::from_str(input).unwrap();
        match msg {
            KrakenOrderBookL2::Data(payload) => {
                assert_eq!(payload.msg_type, "snapshot");
                assert_eq!(payload.data[0].bids.len(), 2);
                assert_eq!(payload.data[0].asks.len(), 2);
                assert_eq!(payload.data[0].bids[0].price, 66650.0);
            }
            _ => panic!("expected Data variant"),
        }
    }

    #[test]
    fn test_kraken_v2_book_update_deserialize() {
        let input = r#"{
            "channel": "book",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "bids": [{"price": 66636.4, "qty": 0.75}],
                "asks": [],
                "checksum": 1905155190,
                "timestamp": "2026-03-27T12:17:06.686377Z"
            }]
        }"#;
        let msg: KrakenOrderBookL2 = serde_json::from_str(input).unwrap();
        match msg {
            KrakenOrderBookL2::Data(payload) => {
                assert_eq!(payload.msg_type, "update");
                assert_eq!(payload.data[0].bids.len(), 1);
                assert!(payload.data[0].asks.is_empty());
            }
            _ => panic!("expected Data variant"),
        }
    }
}
