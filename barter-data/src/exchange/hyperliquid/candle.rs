use super::channel::HyperliquidChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    de::datetime_utc_from_epoch_duration,
    subscription::SubscriptionId,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;
use std::time::Duration;

/// Hyperliquid real-time kline/candlestick message.
///
/// ### Raw Payload Examples
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
/// ```json
/// {
///     "channel": "candle",
///     "data": {
///         "t": 1672502400000, "T": 1672502459999,
///         "s": "ETH", "i": "1m",
///         "o": "1850.0", "h": "1860.0", "l": "1845.0", "c": "1855.5",
///         "v": "12.345", "n": 150
///     }
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub struct HyperliquidKline {
    pub subscription_id: SubscriptionId,
    pub candle: HyperliquidCandle,
}

/// Inner candle data from the Hyperliquid `"data"` field.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HyperliquidCandle {
    /// Kline open time (epoch ms).
    #[serde(alias = "t")]
    pub open_time: u64,

    /// Kline close time (epoch ms).
    #[serde(alias = "T")]
    pub close_time: u64,

    /// Coin symbol (e.g., "ETH").
    #[serde(alias = "s")]
    pub coin: String,

    /// Candle interval (e.g., "1m", "1h").
    #[serde(alias = "i")]
    pub interval: String,

    /// Open price.
    #[serde(alias = "o")]
    pub open: String,

    /// High price.
    #[serde(alias = "h")]
    pub high: String,

    /// Low price.
    #[serde(alias = "l")]
    pub low: String,

    /// Close price.
    #[serde(alias = "c")]
    pub close: String,

    /// Base asset volume.
    #[serde(alias = "v")]
    pub volume: String,

    /// Number of trades.
    #[serde(alias = "n")]
    pub trade_count: u64,
}

impl<'de> Deserialize<'de> for HyperliquidKline {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        /// Helper struct to match the raw JSON envelope.
        #[derive(Deserialize)]
        struct HyperliquidKlineRaw {
            #[serde(alias = "channel")]
            _channel: String,
            data: HyperliquidCandle,
        }

        let raw = HyperliquidKlineRaw::deserialize(deserializer)?;
        let candle = raw.data;

        let subscription_id = ExchangeSub::from((
            HyperliquidChannel(format_smolstr!("candle.{}", candle.interval)),
            candle.coin.as_str(),
        ))
        .id();

        Ok(HyperliquidKline {
            subscription_id,
            candle,
        })
    }
}

impl Identifier<Option<SubscriptionId>> for HyperliquidKline {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, HyperliquidKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, HyperliquidKline),
    ) -> Self {
        let candle = kline.candle;

        let open: f64 = candle.open.parse().unwrap_or(0.0);
        let high: f64 = candle.high.parse().unwrap_or(0.0);
        let low: f64 = candle.low.parse().unwrap_or(0.0);
        let close: f64 = candle.close.parse().unwrap_or(0.0);
        let volume: f64 = candle.volume.parse().unwrap_or(0.0);

        let open_time = datetime_utc_from_epoch_duration(Duration::from_millis(candle.open_time));
        let close_time =
            datetime_utc_from_epoch_duration(Duration::from_millis(candle.close_time));

        Self(vec![Ok(MarketEvent {
            time_exchange: close_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume,
                quote_volume: None,
                trade_count: candle.trade_count,
                is_closed: true,
            },
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_hyperliquid_kline() {
        let input = r#"
        {
            "channel": "candle",
            "data": {
                "t": 1672502400000, "T": 1672502459999,
                "s": "ETH", "i": "1m",
                "o": "1850.0", "h": "1860.0", "l": "1845.0", "c": "1855.5",
                "v": "12.345", "n": 150
            }
        }
        "#;

        let actual: HyperliquidKline = serde_json::from_str(input).unwrap();

        assert_eq!(actual.candle.coin, "ETH");
        assert_eq!(actual.candle.interval, "1m");
        assert_eq!(actual.candle.open_time, 1672502400000);
        assert_eq!(actual.candle.close_time, 1672502459999);
        assert_eq!(actual.candle.open, "1850.0");
        assert_eq!(actual.candle.high, "1860.0");
        assert_eq!(actual.candle.low, "1845.0");
        assert_eq!(actual.candle.close, "1855.5");
        assert_eq!(actual.candle.volume, "12.345");
        assert_eq!(actual.candle.trade_count, 150);
    }

    #[test]
    fn test_hyperliquid_kline_subscription_id() {
        let input = r#"
        {
            "channel": "candle",
            "data": {
                "t": 1672502400000, "T": 1672502459999,
                "s": "ETH", "i": "1m",
                "o": "1850.0", "h": "1860.0", "l": "1845.0", "c": "1855.5",
                "v": "12.345", "n": 150
            }
        }
        "#;

        let kline: HyperliquidKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("candle.1m|ETH"))
        );
    }

    #[test]
    fn test_hyperliquid_kline_to_candle() {
        let kline = HyperliquidKline {
            subscription_id: SubscriptionId::from("candle.1m|ETH"),
            candle: HyperliquidCandle {
                open_time: 1672502400000,
                close_time: 1672502459999,
                coin: "ETH".to_string(),
                interval: "1m".to_string(),
                open: "1850.0".to_string(),
                high: "1860.0".to_string(),
                low: "1845.0".to_string(),
                close: "1855.5".to_string(),
                volume: "12.345".to_string(),
                trade_count: 150,
            },
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Hyperliquid, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(
            event.kind.open_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000))
        );
        assert_eq!(
            event.kind.close_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502459999))
        );
        assert_eq!(event.time_exchange, event.kind.close_time);
        assert_eq!(event.kind.open, 1850.0);
        assert_eq!(event.kind.high, 1860.0);
        assert_eq!(event.kind.low, 1845.0);
        assert_eq!(event.kind.close, 1855.5);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.quote_volume, None);
        assert_eq!(event.kind.trade_count, 150);
        assert_eq!(event.kind.is_closed, true);
    }

    #[test]
    fn test_deserialize_hyperliquid_kline_missing_field() {
        // Missing the "n" (trade_count) field
        let input = r#"
        {
            "channel": "candle",
            "data": {
                "t": 1672502400000, "T": 1672502459999,
                "s": "ETH", "i": "1m",
                "o": "1850.0", "h": "1860.0", "l": "1845.0", "c": "1855.5",
                "v": "12.345"
            }
        }
        "#;

        assert!(serde_json::from_str::<HyperliquidKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_hyperliquid_kline_missing_data() {
        let input = r#"
        {
            "channel": "candle"
        }
        "#;

        assert!(serde_json::from_str::<HyperliquidKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_hyperliquid_kline_missing_coin() {
        let input = r#"
        {
            "channel": "candle",
            "data": {
                "t": 1672502400000, "T": 1672502459999,
                "i": "1m",
                "o": "1850.0", "h": "1860.0", "l": "1845.0", "c": "1855.5",
                "v": "12.345", "n": 150
            }
        }
        "#;

        assert!(serde_json::from_str::<HyperliquidKline>(input).is_err());
    }

    #[test]
    fn test_hyperliquid_kline_subscription_id_different_interval() {
        let input = r#"
        {
            "channel": "candle",
            "data": {
                "t": 1672502400000, "T": 1672506000000,
                "s": "BTC", "i": "1h",
                "o": "16850.0", "h": "16900.0", "l": "16800.0", "c": "16875.0",
                "v": "100.5", "n": 5000
            }
        }
        "#;

        let kline: HyperliquidKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("candle.1h|BTC"))
        );
    }

    #[test]
    fn test_hyperliquid_kline_to_candle_zero_volume() {
        let kline = HyperliquidKline {
            subscription_id: SubscriptionId::from("candle.1m|ETH"),
            candle: HyperliquidCandle {
                open_time: 1672502400000,
                close_time: 1672502459999,
                coin: "ETH".to_string(),
                interval: "1m".to_string(),
                open: "1850.0".to_string(),
                high: "1860.0".to_string(),
                low: "1845.0".to_string(),
                close: "1855.5".to_string(),
                volume: "0.0".to_string(),
                trade_count: 0,
            },
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Hyperliquid, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.volume, 0.0);
        assert_eq!(event.kind.quote_volume, None);
        assert_eq!(event.kind.trade_count, 0);
        assert_eq!(event.kind.is_closed, true);
    }
}
