use super::message::GateioMessage;
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

/// Type alias for a [`Gateio`](super::Gateio) candlestick/kline WebSocket message.
///
/// Used by all GateIO server variants (spot, futures, perpetuals, options).
pub type GateioKline = GateioMessage<GateioKlineInner>;

/// [`Gateio`](super::Gateio) candlestick/kline inner data.
///
/// ### Raw Payload Examples
/// #### Spot Candlestick
/// See docs: <https://www.gate.io/docs/developers/apiv4/ws/en/#candlesticks-channel>
/// ```json
/// {
///   "t": "1672502400",
///   "v": "12.345",
///   "c": "16855.5",
///   "h": "16860",
///   "l": "16845",
///   "o": "16850",
///   "n": "1m_BTC_USDT",
///   "a": "208000"
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct GateioKlineInner {
    /// Timestamp in unix seconds (as string).
    #[serde(
        rename = "t",
        deserialize_with = "barter_integration::de::de_str"
    )]
    pub timestamp: i64,

    /// Open price.
    #[serde(rename = "o", deserialize_with = "barter_integration::de::de_str")]
    pub open: f64,

    /// High price.
    #[serde(rename = "h", deserialize_with = "barter_integration::de::de_str")]
    pub high: f64,

    /// Low price.
    #[serde(rename = "l", deserialize_with = "barter_integration::de::de_str")]
    pub low: f64,

    /// Close price.
    #[serde(rename = "c", deserialize_with = "barter_integration::de::de_str")]
    pub close: f64,

    /// Base asset volume.
    #[serde(rename = "v", deserialize_with = "barter_integration::de::de_str")]
    pub volume: f64,

    /// Name identifier (e.g., "1m_BTC_USDT").
    #[serde(rename = "n")]
    pub name: String,

    /// Quote asset volume / amount.
    #[serde(rename = "a", deserialize_with = "barter_integration::de::de_str")]
    pub quote_volume: f64,
}

impl Identifier<Option<SubscriptionId>> for GateioKline {
    fn id(&self) -> Option<SubscriptionId> {
        // name is like "1m_BTC_USDT" - split at first '_' to get market
        let market = self.data.name.splitn(2, '_').nth(1)?;
        Some(ExchangeSub::from((&self.channel, market)).id())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, GateioKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, GateioKline),
    ) -> Self {
        let k = kline.data;
        let open_time = DateTime::<Utc>::from_timestamp(k.timestamp, 0).unwrap_or_default();
        Self(vec![Ok(MarketEvent {
            time_exchange: open_time,
            time_received: Utc::now(),
            exchange: exchange_id,
            instrument,
            kind: Candle {
                open_time,
                close_time: open_time,
                open: k.open,
                high: k.high,
                low: k.low,
                close: k.close,
                volume: k.volume,
                quote_volume: Some(k.quote_volume),
                trade_count: 0,
            },
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_gateio_kline_spot() {
        let input = r#"
        {
            "time": 1672502458,
            "time_ms": 1672502458231,
            "channel": "spot.candlesticks_1m",
            "event": "update",
            "result": {
                "t": "1672502400",
                "v": "12.345",
                "c": "16855.5",
                "h": "16860",
                "l": "16845",
                "o": "16850",
                "n": "1m_BTC_USDT",
                "a": "208000"
            }
        }
        "#;

        let kline: GateioKline = serde_json::from_str(input).unwrap();
        assert_eq!(kline.channel, "spot.candlesticks_1m");
        assert_eq!(kline.data.timestamp, 1672502400);
        assert_eq!(kline.data.open, 16850.0);
        assert_eq!(kline.data.high, 16860.0);
        assert_eq!(kline.data.low, 16845.0);
        assert_eq!(kline.data.close, 16855.5);
        assert_eq!(kline.data.volume, 12.345);
        assert_eq!(kline.data.quote_volume, 208000.0);
        assert_eq!(kline.data.name, "1m_BTC_USDT");
    }

    #[test]
    fn test_deserialize_gateio_kline_futures() {
        let input = r#"
        {
            "time": 1672502458,
            "time_ms": 1672502458231,
            "channel": "futures.candlesticks_1m",
            "event": "update",
            "result": {
                "t": "1672502400",
                "v": "12.345",
                "c": "16855.5",
                "h": "16860",
                "l": "16845",
                "o": "16850",
                "n": "1m_BTC_USDT",
                "a": "208000"
            }
        }
        "#;

        let kline: GateioKline = serde_json::from_str(input).unwrap();
        assert_eq!(kline.channel, "futures.candlesticks_1m");
    }

    #[test]
    fn test_gateio_kline_subscription_id() {
        let input = r#"
        {
            "time": 1672502458,
            "time_ms": 1672502458231,
            "channel": "spot.candlesticks_1m",
            "event": "update",
            "result": {
                "t": "1672502400",
                "v": "12.345",
                "c": "16855.5",
                "h": "16860",
                "l": "16845",
                "o": "16850",
                "n": "1m_BTC_USDT",
                "a": "208000"
            }
        }
        "#;

        let kline: GateioKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("spot.candlesticks_1m|BTC_USDT"))
        );
    }

    #[test]
    fn test_gateio_kline_to_candle() {
        let kline = GateioKline {
            channel: "spot.candlesticks_1m".to_string(),
            error: None,
            data: GateioKlineInner {
                timestamp: 1672502400,
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                volume: 12.345,
                name: "1m_BTC_USDT".to_string(),
                quote_volume: 208000.0,
            },
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::GateioSpot, "instrument_key", kline));

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
        assert_eq!(
            event.kind.open_time,
            DateTime::<Utc>::from_timestamp(1672502400, 0).unwrap()
        );
    }
}
