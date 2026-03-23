use super::channel::HyperliquidChannel;
use crate::{
    Identifier,
    books::{Level, OrderBook},
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::book::OrderBookEvent,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{de::datetime_utc_from_epoch_duration, subscription::SubscriptionId};
use chrono::Utc;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;
use std::time::Duration;

/// Hyperliquid L2 book WebSocket message.
///
/// ### Raw Payload Example
/// ```json
/// {
///     "channel": "l2Book",
///     "data": {
///         "coin": "BTC",
///         "time": 1672502400000,
///         "levels": [
///             [{"px": "42000.5", "sz": "1.5", "n": 10}],
///             [{"px": "42001.0", "sz": "2.0", "n": 5}]
///         ]
///     }
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub struct HyperliquidOrderBooks {
    pub subscription_id: SubscriptionId,
    pub data: HyperliquidBookData,
}

#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HyperliquidBookData {
    pub coin: SmolStr,
    pub time: u64,
    /// `levels[0]` = bids, `levels[1]` = asks
    pub levels: Vec<Vec<HyperliquidLevel>>,
}

/// A single price level in a Hyperliquid L2 book.
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HyperliquidLevel {
    #[serde(with = "rust_decimal::serde::str")]
    pub px: Decimal,
    #[serde(with = "rust_decimal::serde::str")]
    pub sz: Decimal,
    pub n: u32,
}

impl From<HyperliquidLevel> for Level {
    fn from(level: HyperliquidLevel) -> Self {
        Self {
            price: level.px,
            amount: level.sz,
        }
    }
}

impl<'de> Deserialize<'de> for HyperliquidOrderBooks {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Raw {
            #[allow(dead_code)]
            channel: String,
            data: HyperliquidBookData,
        }

        let raw = Raw::deserialize(deserializer)?;

        let subscription_id = ExchangeSub::from((
            HyperliquidChannel(SmolStr::new_static("l2Book")),
            raw.data.coin.as_str(),
        ))
        .id();

        Ok(HyperliquidOrderBooks {
            subscription_id,
            data: raw.data,
        })
    }
}

impl Identifier<Option<SubscriptionId>> for HyperliquidOrderBooks {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, HyperliquidOrderBooks)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from(
        (exchange, instrument, message): (ExchangeId, InstrumentKey, HyperliquidOrderBooks),
    ) -> Self {
        let data = message.data;
        let time = datetime_utc_from_epoch_duration(Duration::from_millis(data.time));

        let bids = data.levels.first().cloned().unwrap_or_default();
        let asks = data.levels.get(1).cloned().unwrap_or_default();

        // data.time (epoch ms) used as sequence — Hyperliquid has no dedicated sequence ID
        let orderbook = OrderBook::new(data.time, Some(time), bids, asks);

        Self(vec![Ok(MarketEvent {
            time_exchange: time,
            time_received: Utc::now(),
            exchange,
            instrument,
            kind: OrderBookEvent::Snapshot(orderbook),
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_hyperliquid_level_deserialize() {
        let input = r#"{"px": "42000.5", "sz": "1.5", "n": 10}"#;
        let level: HyperliquidLevel = serde_json::from_str(input).unwrap();
        assert_eq!(level.px, dec!(42000.5));
        assert_eq!(level.sz, dec!(1.5));
        assert_eq!(level.n, 10);
    }

    #[test]
    fn test_hyperliquid_orderbooks_deserialize() {
        let input = r#"{
            "channel": "l2Book",
            "data": {
                "coin": "BTC",
                "time": 1672502400000,
                "levels": [
                    [{"px": "42000.5", "sz": "1.5", "n": 10}, {"px": "41999.0", "sz": "2.0", "n": 5}],
                    [{"px": "42001.0", "sz": "0.5", "n": 3}]
                ]
            }
        }"#;
        let msg: HyperliquidOrderBooks = serde_json::from_str(input).unwrap();
        assert_eq!(msg.data.coin, "BTC");
        assert_eq!(msg.data.levels.len(), 2);
        assert_eq!(msg.data.levels[0].len(), 2); // 2 bid levels
        assert_eq!(msg.data.levels[1].len(), 1); // 1 ask level
        assert_eq!(msg.data.levels[0][0].px, dec!(42000.5));
    }

    #[test]
    fn test_subscription_id_derivation() {
        let input = r#"{
            "channel": "l2Book",
            "data": {
                "coin": "ETH",
                "time": 1672502400000,
                "levels": [[], []]
            }
        }"#;
        let msg: HyperliquidOrderBooks = serde_json::from_str(input).unwrap();
        assert!(msg.subscription_id.0.as_str().contains("l2Book"));
        assert!(msg.subscription_id.0.as_str().contains("ETH"));
    }
}
