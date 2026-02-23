use crate::{
    event::{MarketEvent, MarketIter},
    exchange::bybit::message::BybitPayload,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Terse type alias for a [`BybitKlineData`] real-time kline/candlestick WebSocket message.
///
/// Uses the generic [`BybitPayload`] wrapper which handles topic-based subscription ID extraction.
pub type BybitKline = BybitPayload<Vec<BybitKlineData>>;

/// Inner kline data from the Bybit `"data"` array.
///
/// ### Raw Payload Examples
/// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/kline>
/// ```json
/// {
///     "start": 1672502400000,
///     "end": 1672502459999,
///     "interval": "1",
///     "open": "16850",
///     "close": "16855.5",
///     "high": "16860",
///     "low": "16845",
///     "volume": "12.345",
///     "turnover": "208000",
///     "confirm": true,
///     "timestamp": 1672502458000
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BybitKlineData {
    /// Kline open time (epoch ms).
    #[serde(deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc")]
    pub start: DateTime<Utc>,

    /// Kline close time (epoch ms).
    #[serde(deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc")]
    pub end: DateTime<Utc>,

    /// Interval (e.g., "1", "60", "D").
    pub interval: String,

    /// Open price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub open: f64,

    /// High price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub high: f64,

    /// Low price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub low: f64,

    /// Close price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub close: f64,

    /// Base asset volume.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub volume: f64,

    /// Quote asset volume (turnover).
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub turnover: f64,

    /// Whether the candle is confirmed/closed.
    pub confirm: bool,

    /// Timestamp (epoch ms).
    #[serde(deserialize_with = "barter_integration::de::de_u64_epoch_ms_as_datetime_utc")]
    pub timestamp: DateTime<Utc>,
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BybitKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, BybitKline),
    ) -> Self {
        match kline.data.into_iter().next() {
            Some(k) => Self(vec![Ok(MarketEvent {
                time_exchange: k.end,
                time_received: Utc::now(),
                exchange: exchange_id,
                instrument,
                kind: Candle {
                    open_time: k.start,
                    close_time: k.end,
                    open: k.open,
                    high: k.high,
                    low: k.low,
                    close: k.close,
                    volume: k.volume,
                    quote_volume: Some(k.turnover),
                    trade_count: 0,
                },
            })]),
            None => Self(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::bybit::message::BybitPayloadKind;
    use barter_integration::{
        de::datetime_utc_from_epoch_duration, subscription::SubscriptionId,
    };
    use smol_str::ToSmolStr;
    use std::time::Duration;

    #[test]
    fn test_deserialize_bybit_kline() {
        let input = r#"
        {
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672502458000,
            "data": [{
                "start": 1672502400000,
                "end": 1672502459999,
                "interval": "1",
                "open": "16850",
                "close": "16855.5",
                "high": "16860",
                "low": "16845",
                "volume": "12.345",
                "turnover": "208000",
                "confirm": true,
                "timestamp": 1672502458000
            }]
        }
        "#;

        let actual: BybitKline = serde_json::from_str(input).unwrap();

        assert_eq!(actual.kind, BybitPayloadKind::Snapshot);
        assert_eq!(
            actual.time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502458000))
        );
        assert_eq!(actual.data.len(), 1);

        let k = &actual.data[0];
        assert_eq!(k.interval, "1");
        assert_eq!(
            k.start,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000))
        );
        assert_eq!(
            k.end,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502459999))
        );
        assert_eq!(k.open, 16850.0);
        assert_eq!(k.high, 16860.0);
        assert_eq!(k.low, 16845.0);
        assert_eq!(k.close, 16855.5);
        assert_eq!(k.volume, 12.345);
        assert_eq!(k.turnover, 208000.0);
        assert!(k.confirm);
    }

    #[test]
    fn test_bybit_kline_subscription_id() {
        let input = r#"
        {
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672502458000,
            "data": [{
                "start": 1672502400000,
                "end": 1672502459999,
                "interval": "1",
                "open": "16850",
                "close": "16855.5",
                "high": "16860",
                "low": "16845",
                "volume": "12.345",
                "turnover": "208000",
                "confirm": true,
                "timestamp": 1672502458000
            }]
        }
        "#;

        let kline: BybitKline = serde_json::from_str(input).unwrap();

        assert_eq!(
            kline.subscription_id,
            SubscriptionId("kline.1|BTCUSDT".to_smolstr())
        );
    }

    #[test]
    fn test_bybit_kline_to_candle() {
        let kline = BybitKline {
            subscription_id: SubscriptionId("kline.1|BTCUSDT".to_smolstr()),
            kind: BybitPayloadKind::Snapshot,
            time: datetime_utc_from_epoch_duration(Duration::from_millis(1672502458000)),
            data: vec![BybitKlineData {
                start: datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000)),
                end: datetime_utc_from_epoch_duration(Duration::from_millis(1672502459999)),
                interval: "1".to_string(),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                volume: 12.345,
                turnover: 208000.0,
                confirm: true,
                timestamp: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1672502458000,
                )),
            }],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::BybitSpot, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open, 16850.0);
        assert_eq!(event.kind.high, 16860.0);
        assert_eq!(event.kind.low, 16845.0);
        assert_eq!(event.kind.close, 16855.5);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.quote_volume, Some(208000.0));
        assert_eq!(event.kind.trade_count, 0);
    }

    #[test]
    fn test_deserialize_bybit_kline_missing_open() {
        let input = r#"
        {
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672502458000,
            "data": [{
                "start": 1672502400000,
                "end": 1672502459999,
                "interval": "1",
                "close": "16855.5",
                "high": "16860",
                "low": "16845",
                "volume": "12.345",
                "turnover": "208000",
                "confirm": true,
                "timestamp": 1672502458000
            }]
        }
        "#;

        assert!(serde_json::from_str::<BybitKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_bybit_kline_missing_topic() {
        let input = r#"
        {
            "type": "snapshot",
            "ts": 1672502458000,
            "data": [{
                "start": 1672502400000,
                "end": 1672502459999,
                "interval": "1",
                "open": "16850",
                "close": "16855.5",
                "high": "16860",
                "low": "16845",
                "volume": "12.345",
                "turnover": "208000",
                "confirm": true,
                "timestamp": 1672502458000
            }]
        }
        "#;

        assert!(serde_json::from_str::<BybitKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_bybit_kline_invalid_price_string() {
        let input = r#"
        {
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672502458000,
            "data": [{
                "start": 1672502400000,
                "end": 1672502459999,
                "interval": "1",
                "open": "abc",
                "close": "16855.5",
                "high": "16860",
                "low": "16845",
                "volume": "12.345",
                "turnover": "208000",
                "confirm": true,
                "timestamp": 1672502458000
            }]
        }
        "#;

        assert!(serde_json::from_str::<BybitKline>(input).is_err());
    }

    #[test]
    fn test_bybit_kline_empty_data_array() {
        let input = r#"
        {
            "topic": "kline.1.BTCUSDT",
            "type": "snapshot",
            "ts": 1672502458000,
            "data": []
        }
        "#;

        let kline: BybitKline = serde_json::from_str(input).unwrap();
        assert!(kline.data.is_empty());

        // Converting empty data should produce empty MarketIter
        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::BybitSpot, "instrument_key", kline));
        assert!(market_iter.0.is_empty());
    }

    #[test]
    fn test_bybit_kline_to_candle_zero_volume() {
        let kline = BybitKline {
            subscription_id: SubscriptionId("kline.1|BTCUSDT".to_smolstr()),
            kind: BybitPayloadKind::Snapshot,
            time: datetime_utc_from_epoch_duration(Duration::from_millis(1672502458000)),
            data: vec![BybitKlineData {
                start: datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000)),
                end: datetime_utc_from_epoch_duration(Duration::from_millis(1672502459999)),
                interval: "1".to_string(),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                volume: 0.0,
                turnover: 0.0,
                confirm: true,
                timestamp: datetime_utc_from_epoch_duration(Duration::from_millis(
                    1672502458000,
                )),
            }],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::BybitSpot, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.volume, 0.0);
        assert_eq!(event.kind.quote_volume, Some(0.0));
        assert_eq!(event.kind.trade_count, 0);
    }
}
