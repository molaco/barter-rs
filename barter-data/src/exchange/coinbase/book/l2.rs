use crate::{
    Identifier,
    books::{Level, OrderBook},
    error::DataError,
    event::MarketEvent,
    exchange::{Connector, ExchangeSub, coinbase::Coinbase},
    subscription::{
        Map,
        book::{OrderBookEvent, OrderBooksL2},
    },
    transformer::ExchangeTransformer,
};
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use chrono::{DateTime, Utc};
use derive_more::Constructor;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;

use super::super::channel::CoinbaseChannel;

/// Coinbase L2 snapshot message.
///
/// ```json
/// {"type":"snapshot","product_id":"BTC-USD","bids":[["price","size"],...],"asks":[...]}
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct CoinbaseL2Snapshot {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub product_id: String,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

/// Coinbase L2 update message.
///
/// ```json
/// {"type":"l2update","product_id":"BTC-USD","changes":[["buy","price","size"],...],"time":"..."}
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct CoinbaseL2Update {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub product_id: String,
    pub changes: Vec<[String; 3]>,
    pub time: DateTime<Utc>,
}

/// Combined Coinbase L2 message (snapshot or update).
///
/// Uses `#[serde(tag = "type")]` to discriminate.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CoinbaseL2Message {
    Snapshot {
        product_id: String,
        bids: Vec<[String; 2]>,
        asks: Vec<[String; 2]>,
    },
    #[serde(rename = "l2update")]
    L2Update {
        product_id: String,
        changes: Vec<[String; 3]>,
        time: DateTime<Utc>,
    },
    /// Subscription confirmations, heartbeats, etc.
    #[serde(other)]
    Other,
}

impl Identifier<Option<SubscriptionId>> for CoinbaseL2Message {
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            CoinbaseL2Message::Snapshot { product_id, .. } => Some(
                ExchangeSub::from((CoinbaseChannel::ORDER_BOOK_L2, product_id)).id(),
            ),
            CoinbaseL2Message::L2Update { product_id, .. } => Some(
                ExchangeSub::from((CoinbaseChannel::ORDER_BOOK_L2, product_id)).id(),
            ),
            CoinbaseL2Message::Other => None,
        }
    }
}

fn parse_levels(raw: &[[String; 2]]) -> Vec<Level> {
    raw.iter()
        .filter_map(|[price, size]| {
            let p: Decimal = price.parse().ok()?;
            let s: Decimal = size.parse().ok()?;
            Some(Level { price: p, amount: s })
        })
        .take(50) // Truncate to top 50
        .collect()
}

fn parse_change_levels(changes: &[[String; 3]], side: &str) -> Vec<Level> {
    changes
        .iter()
        .filter(|[s, _, _]| s == side)
        .filter_map(|[_, price, size]| {
            let p: Decimal = price.parse().ok()?;
            let s: Decimal = size.parse().ok()?;
            Some(Level { price: p, amount: s })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Transformer
// ---------------------------------------------------------------------------

#[derive(Debug, Constructor)]
pub struct CoinbaseOrderBookL2Meta<InstrumentKey> {
    pub key: InstrumentKey,
    pub initialized: bool,
}

#[derive(Debug)]
pub struct CoinbaseOrderBooksL2Transformer<InstrumentKey> {
    instrument_map: Map<CoinbaseOrderBookL2Meta<InstrumentKey>>,
}

impl<InstrumentKey> ExchangeTransformer<Coinbase, InstrumentKey, OrderBooksL2>
    for CoinbaseOrderBooksL2Transformer<InstrumentKey>
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
                (sub_id, CoinbaseOrderBookL2Meta::new(instrument_key, has_snapshot))
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
                .insert(id, CoinbaseOrderBookL2Meta::new(key, false));
        }
    }

    fn remove_map_entries(&mut self, subscription_ids: &[SubscriptionId]) {
        for id in subscription_ids {
            self.instrument_map.remove(id);
        }
    }
}

impl<InstrumentKey> Transformer for CoinbaseOrderBooksL2Transformer<InstrumentKey>
where
    InstrumentKey: Clone,
{
    type Error = DataError;
    type Input = CoinbaseL2Message;
    type Output = MarketEvent<InstrumentKey, OrderBookEvent>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let subscription_id = match input.id() {
            Some(id) => id,
            None => return vec![],
        };

        let instrument = match self.instrument_map.find_mut(&subscription_id) {
            Ok(instrument) => instrument,
            Err(unidentifiable) => return vec![Err(DataError::from(unidentifiable))],
        };

        match input {
            CoinbaseL2Message::Snapshot {
                bids, asks, ..
            } => {
                instrument.initialized = true;
                let bids_parsed = parse_levels(&bids);
                let asks_parsed = parse_levels(&asks);
                let now = Utc::now();

                vec![Ok(MarketEvent {
                    time_exchange: now,
                    time_received: now,
                    exchange: Coinbase::ID,
                    instrument: instrument.key.clone(),
                    kind: OrderBookEvent::Snapshot(OrderBook::new(
                        0,
                        None,
                        bids_parsed,
                        asks_parsed,
                    )),
                })]
            }
            CoinbaseL2Message::L2Update {
                changes, time, ..
            } => {
                if !instrument.initialized {
                    tracing::debug!("Coinbase L2 update before snapshot");
                    return vec![];
                }

                let bids = parse_change_levels(&changes, "buy");
                let asks = parse_change_levels(&changes, "sell");

                vec![Ok(MarketEvent {
                    time_exchange: time,
                    time_received: Utc::now(),
                    exchange: Coinbase::ID,
                    instrument: instrument.key.clone(),
                    kind: OrderBookEvent::Update(OrderBook::new(
                        0,
                        Some(time),
                        bids,
                        asks,
                    )),
                })]
            }
            CoinbaseL2Message::Other => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coinbase_l2_snapshot_deserialize() {
        let input = r#"{
            "type": "snapshot",
            "product_id": "BTC-USD",
            "bids": [["66650.00", "0.001"], ["66649.00", "0.5"]],
            "asks": [["66651.00", "2.69"], ["66652.00", "1.0"]]
        }"#;
        let msg: CoinbaseL2Message = serde_json::from_str(input).unwrap();
        match msg {
            CoinbaseL2Message::Snapshot { product_id, bids, asks } => {
                assert_eq!(product_id, "BTC-USD");
                assert_eq!(bids.len(), 2);
                assert_eq!(asks.len(), 2);
            }
            _ => panic!("expected Snapshot"),
        }
    }

    #[test]
    fn test_coinbase_l2_update_deserialize() {
        let input = r#"{
            "type": "l2update",
            "product_id": "BTC-USD",
            "changes": [["buy", "66650.00", "0.5"], ["sell", "66651.00", "0"]],
            "time": "2026-03-27T12:00:00.000000Z"
        }"#;
        let msg: CoinbaseL2Message = serde_json::from_str(input).unwrap();
        match msg {
            CoinbaseL2Message::L2Update { product_id, changes, .. } => {
                assert_eq!(product_id, "BTC-USD");
                assert_eq!(changes.len(), 2);
            }
            _ => panic!("expected L2Update"),
        }
    }

    #[test]
    fn test_coinbase_l2_other_message() {
        let input = r#"{"type": "subscriptions", "channels": []}"#;
        let msg: CoinbaseL2Message = serde_json::from_str(input).unwrap();
        assert!(matches!(msg, CoinbaseL2Message::Other));
    }
}
