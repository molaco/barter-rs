use super::channel::BinanceChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Binance real-time kline/candlestick message.
///
/// ### Raw Payload Examples
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-streams>
/// ```json
/// {
///     "e":"kline","E":1672515782136,"s":"BTCUSDT",
///     "k":{
///         "t":1672515780000,"T":1672515839999,
///         "s":"BTCUSDT","i":"1m",
///         "o":"16850.00","c":"16855.50","h":"16860.00","l":"16845.00",
///         "v":"12.345","q":"208000.00","n":150,
///         "x":false
///     }
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceKline {
    #[serde(alias = "k")]
    pub kline: BinanceKlineData,
}

/// Inner kline data from the Binance `"k"` field.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BinanceKlineData {
    /// Symbol (e.g., "BTCUSDT").
    #[serde(alias = "s")]
    pub symbol: String,

    /// Interval (e.g., "1m", "1h").
    #[serde(alias = "i")]
    pub interval: String,

    /// Kline open time (epoch ms).
    #[serde(
        alias = "t",
        deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub open_time: DateTime<Utc>,

    /// Kline close time (epoch ms).
    #[serde(
        alias = "T",
        deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc"
    )]
    pub close_time: DateTime<Utc>,

    /// Open price.
    #[serde(alias = "o", deserialize_with = "barter_integration::de::de_str")]
    pub open: f64,

    /// High price.
    #[serde(alias = "h", deserialize_with = "barter_integration::de::de_str")]
    pub high: f64,

    /// Low price.
    #[serde(alias = "l", deserialize_with = "barter_integration::de::de_str")]
    pub low: f64,

    /// Close price.
    #[serde(alias = "c", deserialize_with = "barter_integration::de::de_str")]
    pub close: f64,

    /// Base asset volume.
    #[serde(alias = "v", deserialize_with = "barter_integration::de::de_str")]
    pub volume: f64,

    /// Quote asset volume.
    #[serde(alias = "q", deserialize_with = "barter_integration::de::de_str")]
    pub quote_volume: f64,

    /// Number of trades.
    #[serde(alias = "n")]
    pub trade_count: u64,
}

impl Identifier<Option<SubscriptionId>> for BinanceKline {
    fn id(&self) -> Option<SubscriptionId> {
        Some(
            ExchangeSub::from((
                BinanceChannel(format!("@kline_{}", self.kline.interval)),
                self.kline.symbol.as_str(),
            ))
            .id(),
        )
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BinanceKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, BinanceKline),
    ) -> Self {
        let k = kline.kline;
        Self(vec![Ok(MarketEvent {
            time_exchange: k.close_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                open_time: k.open_time,
                close_time: k.close_time,
                open: k.open,
                high: k.high,
                low: k.low,
                close: k.close,
                volume: k.volume,
                quote_volume: Some(k.quote_volume),
                trade_count: k.trade_count,
            },
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_deserialize_binance_kline() {
        let input = r#"
        {
            "e":"kline","E":1672515782136,"s":"BTCUSDT",
            "k":{
                "t":1672515780000,"T":1672515839999,
                "s":"BTCUSDT","i":"1m",
                "o":"16850.00","c":"16855.50","h":"16860.00","l":"16845.00",
                "v":"12.345","q":"208000.00","n":150,
                "x":false
            }
        }
        "#;

        let actual: BinanceKline = serde_json::from_str(input).unwrap();

        assert_eq!(actual.kline.symbol, "BTCUSDT");
        assert_eq!(actual.kline.interval, "1m");
        assert_eq!(
            actual.kline.open_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672515780000))
        );
        assert_eq!(
            actual.kline.close_time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672515839999))
        );
        assert_eq!(actual.kline.open, 16850.00);
        assert_eq!(actual.kline.high, 16860.00);
        assert_eq!(actual.kline.low, 16845.00);
        assert_eq!(actual.kline.close, 16855.50);
        assert_eq!(actual.kline.volume, 12.345);
        assert_eq!(actual.kline.quote_volume, 208000.00);
        assert_eq!(actual.kline.trade_count, 150);
    }

    #[test]
    fn test_binance_kline_subscription_id() {
        let input = r#"
        {
            "e":"kline","E":1672515782136,"s":"BTCUSDT",
            "k":{
                "t":1672515780000,"T":1672515839999,
                "s":"BTCUSDT","i":"1m",
                "o":"16850.00","c":"16855.50","h":"16860.00","l":"16845.00",
                "v":"12.345","q":"208000.00","n":150,
                "x":false
            }
        }
        "#;

        let kline: BinanceKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("@kline_1m|BTCUSDT"))
        );
    }

    #[test]
    fn test_binance_kline_to_candle() {
        let kline = BinanceKline {
            kline: BinanceKlineData {
                symbol: "BTCUSDT".to_string(),
                interval: "1m".to_string(),
                open_time: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1672515780000,
                )),
                close_time: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1672515839999,
                )),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                volume: 12.345,
                quote_volume: 208000.0,
                trade_count: 150,
            },
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::BinanceSpot, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open, 16850.0);
        assert_eq!(event.kind.high, 16860.0);
        assert_eq!(event.kind.low, 16845.0);
        assert_eq!(event.kind.close, 16855.5);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.quote_volume, Some(208000.0));
        assert_eq!(event.kind.trade_count, 150);
    }

    #[test]
    fn test_deserialize_binance_kline_missing_open_price() {
        let input = r#"
        {
            "e":"kline","E":1672515782136,"s":"BTCUSDT",
            "k":{
                "t":1672515780000,"T":1672515839999,
                "s":"BTCUSDT","i":"1m",
                "c":"16855.50","h":"16860.00","l":"16845.00",
                "v":"12.345","q":"208000.00","n":150,
                "x":false
            }
        }
        "#;

        assert!(serde_json::from_str::<BinanceKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_binance_kline_missing_kline_field() {
        let input = r#"
        {
            "e":"kline","E":1672515782136,"s":"BTCUSDT"
        }
        "#;

        assert!(serde_json::from_str::<BinanceKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_binance_kline_missing_symbol() {
        let input = r#"
        {
            "e":"kline","E":1672515782136,"s":"BTCUSDT",
            "k":{
                "t":1672515780000,"T":1672515839999,
                "i":"1m",
                "o":"16850.00","c":"16855.50","h":"16860.00","l":"16845.00",
                "v":"12.345","q":"208000.00","n":150,
                "x":false
            }
        }
        "#;

        assert!(serde_json::from_str::<BinanceKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_binance_kline_invalid_price_string() {
        let input = r#"
        {
            "e":"kline","E":1672515782136,"s":"BTCUSDT",
            "k":{
                "t":1672515780000,"T":1672515839999,
                "s":"BTCUSDT","i":"1m",
                "o":"not_a_number","c":"16855.50","h":"16860.00","l":"16845.00",
                "v":"12.345","q":"208000.00","n":150,
                "x":false
            }
        }
        "#;

        assert!(serde_json::from_str::<BinanceKline>(input).is_err());
    }

    #[test]
    fn test_binance_kline_to_candle_zero_volume() {
        let kline = BinanceKline {
            kline: BinanceKlineData {
                symbol: "BTCUSDT".to_string(),
                interval: "1m".to_string(),
                open_time: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1672515780000,
                )),
                close_time: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1672515839999,
                )),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                volume: 0.0,
                quote_volume: 0.0,
                trade_count: 0,
            },
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::BinanceSpot, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.volume, 0.0);
        assert_eq!(event.kind.quote_volume, Some(0.0));
        assert_eq!(event.kind.trade_count, 0);
    }
}
