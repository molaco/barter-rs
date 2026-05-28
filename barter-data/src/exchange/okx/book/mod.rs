use crate::{
    books::{Level, OrderBook},
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::book::OrderBookEvent,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::trade::OkxMessage;

/// Level 2 OrderBook transformer with sequencing.
pub mod l2;

/// REST L2 snapshot fetcher.
pub mod snapshot;

/// Terse type alias for an OKX `books5` WebSocket message (legacy stateless).
pub type OkxOrderBookMessage = OkxMessage<OkxOrderBookSnapshot>;

/// OKX `books` channel WebSocket message with action field.
///
/// Used for stateful channels (`books`, `books50-l2-tbt`) that send
/// snapshot + incremental updates with sequence validation.
///
/// ### Raw Payload Example
/// ```json
/// {
///   "arg": {"channel": "books", "instId": "BTC-USDT"},
///   "action": "snapshot",
///   "data": [{
///     "asks": [["67756","0.0441","0","3"]],
///     "bids": [["67755.9","2.4429","0","27"]],
///     "ts": "1774607207558",
///     "checksum": -1839238657,
///     "seqId": 74318423271,
///     "prevSeqId": -1
///   }]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct OkxBookMessage {
    #[serde(
        rename = "arg",
        deserialize_with = "super::trade::de_okx_message_arg_as_subscription_id"
    )]
    pub subscription_id: SubscriptionId,
    pub action: OkxBookAction,
    pub data: Vec<OkxOrderBookUpdate>,
}

impl crate::Identifier<Option<SubscriptionId>> for OkxBookMessage {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

/// OKX book channel action type.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OkxBookAction {
    Snapshot,
    Update,
}

/// OKX orderbook update/snapshot data payload (used by `books` channel).
///
/// Contains `prevSeqId` for gap detection (absent in `books5`).
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct OkxOrderBookUpdate {
    pub asks: Vec<OkxLevel>,
    pub bids: Vec<OkxLevel>,
    #[serde(
        rename = "ts",
        deserialize_with = "barter_integration::de::de_str_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: chrono::DateTime<Utc>,
    #[serde(rename = "seqId", default)]
    pub seq_id: u64,
    #[serde(rename = "prevSeqId", default)]
    pub prev_seq_id: i64,
    #[serde(default)]
    pub checksum: i32,
}

/// A single `books5` snapshot payload from OKX (legacy).
///
/// ### Raw Payload Example
/// ```json
/// {
///   "asks": [["41006.8", "0.60038921", "0", "1"]],
///   "bids": [["41006.3", "0.30178218", "0", "2"]],
///   "ts": "1630048897897",
///   "seqId": 1234567890
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct OkxOrderBookSnapshot {
    pub asks: Vec<OkxLevel>,
    pub bids: Vec<OkxLevel>,
    #[serde(
        rename = "ts",
        deserialize_with = "barter_integration::de::de_str_u64_epoch_ms_as_datetime_utc"
    )]
    pub time: chrono::DateTime<Utc>,
    #[serde(rename = "seqId", default)]
    pub seq_id: u64,
}

/// OKX orderbook level: `["price", "size", "deprecated", "numOrders"]`.
///
/// OKX sends 4-element arrays but we only need price and size.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Serialize)]
pub struct OkxLevel {
    pub price: Decimal,
    pub amount: Decimal,
}

impl From<OkxLevel> for Level {
    fn from(level: OkxLevel) -> Self {
        Self {
            price: level.price,
            amount: level.amount,
        }
    }
}

impl<'de> Deserialize<'de> for OkxLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        // OKX sends ["price", "size", "deprecated", "numOrders"] — we take first two.
        let arr: Vec<&str> = Deserialize::deserialize(deserializer)?;
        if arr.len() < 2 {
            return Err(serde::de::Error::custom(format!(
                "expected at least 2 elements in OKX level array, got {}",
                arr.len()
            )));
        }
        let price = arr[0]
            .parse::<Decimal>()
            .map_err(serde::de::Error::custom)?;
        let amount = arr[1]
            .parse::<Decimal>()
            .map_err(serde::de::Error::custom)?;
        Ok(OkxLevel { price, amount })
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, OkxOrderBookMessage)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, message): (ExchangeId, InstrumentKey, OkxOrderBookMessage),
    ) -> Self {
        // OKX books5 sends full snapshots on every push
        message
            .data
            .into_iter()
            .map(|snap| {
                let orderbook = OrderBook::new(snap.seq_id, Some(snap.time), snap.bids, snap.asks);
                Ok(MarketEvent {
                    time_exchange: snap.time,
                    time_received: Utc::now(),
                    exchange,
                    instrument: instrument.clone(),
                    kind: OrderBookEvent::Snapshot(orderbook),
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_okx_level_deserialize_4_elements() {
        let input = r#"["41006.8", "0.60038921", "0", "1"]"#;
        let level: OkxLevel = serde_json::from_str(input).unwrap();
        assert_eq!(level.price, dec!(41006.8));
        assert_eq!(level.amount, dec!(0.60038921));
    }

    #[test]
    fn test_okx_level_deserialize_2_elements() {
        let input = r#"["100.5", "2.0"]"#;
        let level: OkxLevel = serde_json::from_str(input).unwrap();
        assert_eq!(level.price, dec!(100.5));
        assert_eq!(level.amount, dec!(2.0));
    }

    #[test]
    fn test_okx_orderbook_snapshot_deserialize() {
        let input = r#"{
            "asks": [["41006.8", "0.60038921", "0", "1"]],
            "bids": [["41006.3", "0.30178218", "0", "2"]],
            "ts": "1630048897897",
            "seqId": 123
        }"#;
        let snap: OkxOrderBookSnapshot = serde_json::from_str(input).unwrap();
        assert_eq!(snap.asks.len(), 1);
        assert_eq!(snap.bids.len(), 1);
        assert_eq!(snap.seq_id, 123);
        assert_eq!(snap.asks[0].price, dec!(41006.8));
        assert_eq!(snap.bids[0].price, dec!(41006.3));
    }

    #[test]
    fn test_okx_book_message_snapshot_deserialize() {
        let input = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "snapshot",
            "data": [{
                "asks": [["67756","0.0441","0","3"]],
                "bids": [["67755.9","2.4429","0","27"]],
                "ts": "1774607207558",
                "checksum": -1839238657,
                "seqId": 74318423271,
                "prevSeqId": -1
            }]
        }"#;
        let msg: OkxBookMessage = serde_json::from_str(input).unwrap();
        assert_eq!(msg.action, OkxBookAction::Snapshot);
        assert_eq!(msg.data.len(), 1);
        assert_eq!(msg.data[0].seq_id, 74318423271);
        assert_eq!(msg.data[0].prev_seq_id, -1);
    }

    #[test]
    fn test_okx_book_message_update_deserialize() {
        let input = r#"{
            "arg": {"channel": "books", "instId": "BTC-USDT"},
            "action": "update",
            "data": [{
                "asks": [["67760","0.123","0","2"]],
                "bids": [],
                "ts": "1774607207668",
                "checksum": 437829102,
                "seqId": 74318423280,
                "prevSeqId": 74318423271
            }]
        }"#;
        let msg: OkxBookMessage = serde_json::from_str(input).unwrap();
        assert_eq!(msg.action, OkxBookAction::Update);
        assert_eq!(msg.data[0].prev_seq_id, 74318423271);
    }
}
