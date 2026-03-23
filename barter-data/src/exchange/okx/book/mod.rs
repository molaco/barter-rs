use crate::{
    books::{Level, OrderBook},
    event::{MarketEvent, MarketIter},
    subscription::book::OrderBookEvent,
};
use barter_instrument::exchange::ExchangeId;
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::trade::OkxMessage;

/// Terse type alias for an OKX `books5` WebSocket message.
pub type OkxOrderBookMessage = OkxMessage<OkxOrderBookSnapshot>;

/// A single `books5` snapshot payload from OKX.
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
    #[serde(rename = "ts", deserialize_with = "barter_integration::de::de_str_u64_epoch_ms_as_datetime_utc")]
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
            return Err(serde::de::Error::custom(
                format!("expected at least 2 elements in OKX level array, got {}", arr.len()),
            ));
        }
        let price = arr[0].parse::<Decimal>().map_err(serde::de::Error::custom)?;
        let amount = arr[1].parse::<Decimal>().map_err(serde::de::Error::custom)?;
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
                let orderbook = OrderBook::new(
                    snap.seq_id,
                    Some(snap.time),
                    snap.bids,
                    snap.asks,
                );
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
}
