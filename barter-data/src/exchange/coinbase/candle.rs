use super::channel::CoinbaseChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Coinbase real-time candle WebSocket message.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.cloud.coinbase.com/advanced-trade/docs/ws-channels#candles-channel>
/// ```json
/// {
///     "channel":"candles",
///     "timestamp":"2023-06-09T20:19:35.396Z",
///     "sequence_num":0,
///     "events":[{
///         "type":"snapshot",
///         "candles":[{
///             "start":"1688998200",
///             "high":"1867.72",
///             "low":"1865.63",
///             "open":"1867.38",
///             "close":"1866.81",
///             "volume":"0.20269406",
///             "product_id":"ETH-USD"
///         }]
///     }]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct CoinbaseKline {
    pub events: Vec<CoinbaseKlineEvent>,
}

/// Inner event from the Coinbase candles `"events"` array.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct CoinbaseKlineEvent {
    /// Event type: "snapshot" or "update".
    #[serde(alias = "type")]
    pub event_type: String,

    /// Array of candle data.
    pub candles: Vec<CoinbaseKlineData>,
}

/// Individual candle data from the Coinbase candles channel.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct CoinbaseKlineData {
    /// Candle start time (unix seconds as string).
    #[serde(deserialize_with = "de_str_unix_seconds_as_datetime_utc")]
    pub start: DateTime<Utc>,

    /// High price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub high: f64,

    /// Low price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub low: f64,

    /// Open price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub open: f64,

    /// Close price.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub close: f64,

    /// Base asset volume.
    #[serde(deserialize_with = "barter_integration::de::de_str")]
    pub volume: f64,

    /// Product identifier (e.g., "ETH-USD").
    pub product_id: String,
}

impl Identifier<Option<SubscriptionId>> for CoinbaseKline {
    fn id(&self) -> Option<SubscriptionId> {
        self.events
            .first()
            .and_then(|event| event.candles.first())
            .map(|candle| {
                ExchangeSub::from((
                    CoinbaseChannel::candles(),
                    candle.product_id.as_str(),
                ))
                .id()
            })
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, CoinbaseKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, CoinbaseKline),
    ) -> Self {
        // Coinbase candles only support 5-minute intervals (300 seconds).
        let candle_duration = Duration::seconds(300);

        Self(
            kline
                .events
                .into_iter()
                .flat_map(|event| event.candles)
                .map(|candle| {
                    Ok(MarketEvent {
                        time_exchange: candle.start + candle_duration,
                        time_received: Utc::now(),
                        exchange: exchange_id,
                        instrument: instrument.clone(),
                        kind: Candle {
                            open_time: candle.start,
                            close_time: candle.start + candle_duration,
                            open: candle.open,
                            high: candle.high,
                            low: candle.low,
                            close: candle.close,
                            volume: candle.volume,
                            quote_volume: None,
                            trade_count: 0,
                        },
                    })
                })
                .collect(),
        )
    }
}

/// Deserialize a unix seconds string (e.g., "1688998200") into a `DateTime<Utc>`.
fn de_str_unix_seconds_as_datetime_utc<'de, D>(
    deserializer: D,
) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let seconds_str: &str = serde::Deserialize::deserialize(deserializer)?;
    let seconds: i64 = seconds_str
        .parse()
        .map_err(serde::de::Error::custom)?;
    DateTime::from_timestamp(seconds, 0)
        .ok_or_else(|| serde::de::Error::custom("invalid unix timestamp"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_coinbase_kline() {
        let input = r#"
        {
            "channel":"candles",
            "timestamp":"2023-06-09T20:19:35.396Z",
            "sequence_num":0,
            "events":[{
                "type":"snapshot",
                "candles":[{
                    "start":"1688998200",
                    "high":"1867.72",
                    "low":"1865.63",
                    "open":"1867.38",
                    "close":"1866.81",
                    "volume":"0.20269406",
                    "product_id":"ETH-USD"
                }]
            }]
        }
        "#;

        let actual: CoinbaseKline = serde_json::from_str(input).unwrap();

        assert_eq!(actual.events.len(), 1);
        let event = &actual.events[0];
        assert_eq!(event.event_type, "snapshot");
        assert_eq!(event.candles.len(), 1);
        let candle = &event.candles[0];
        assert_eq!(
            candle.start,
            DateTime::from_timestamp(1688998200, 0).unwrap()
        );
        assert_eq!(candle.high, 1867.72);
        assert_eq!(candle.low, 1865.63);
        assert_eq!(candle.open, 1867.38);
        assert_eq!(candle.close, 1866.81);
        assert_eq!(candle.volume, 0.20269406);
        assert_eq!(candle.product_id, "ETH-USD");
    }

    #[test]
    fn test_coinbase_kline_subscription_id() {
        let input = r#"
        {
            "channel":"candles",
            "timestamp":"2023-06-09T20:19:35.396Z",
            "sequence_num":0,
            "events":[{
                "type":"snapshot",
                "candles":[{
                    "start":"1688998200",
                    "high":"1867.72",
                    "low":"1865.63",
                    "open":"1867.38",
                    "close":"1866.81",
                    "volume":"0.20269406",
                    "product_id":"ETH-USD"
                }]
            }]
        }
        "#;

        let kline: CoinbaseKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("candles|ETH-USD"))
        );
    }

    #[test]
    fn test_coinbase_kline_to_candle() {
        let open_time = DateTime::from_timestamp(1688998200, 0).unwrap();
        let close_time = open_time + Duration::seconds(300);

        let kline = CoinbaseKline {
            events: vec![CoinbaseKlineEvent {
                event_type: "update".to_string(),
                candles: vec![CoinbaseKlineData {
                    start: open_time,
                    high: 1867.72,
                    low: 1865.63,
                    open: 1867.38,
                    close: 1866.81,
                    volume: 0.20269406,
                    product_id: "ETH-USD".to_string(),
                }],
            }],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Coinbase, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open_time, open_time);
        assert_eq!(event.kind.close_time, close_time);
        assert_eq!(event.kind.open, 1867.38);
        assert_eq!(event.kind.high, 1867.72);
        assert_eq!(event.kind.low, 1865.63);
        assert_eq!(event.kind.close, 1866.81);
        assert_eq!(event.kind.volume, 0.20269406);
        assert_eq!(event.kind.quote_volume, None);
        assert_eq!(event.kind.trade_count, 0);
    }

    #[test]
    fn test_coinbase_kline_multiple_candles() {
        let input = r#"
        {
            "channel":"candles",
            "timestamp":"2023-06-09T20:19:35.396Z",
            "sequence_num":0,
            "events":[{
                "type":"snapshot",
                "candles":[
                    {
                        "start":"1688998200",
                        "high":"1867.72",
                        "low":"1865.63",
                        "open":"1867.38",
                        "close":"1866.81",
                        "volume":"0.20269406",
                        "product_id":"ETH-USD"
                    },
                    {
                        "start":"1688997900",
                        "high":"1868.00",
                        "low":"1864.00",
                        "open":"1865.00",
                        "close":"1867.38",
                        "volume":"1.5",
                        "product_id":"ETH-USD"
                    }
                ]
            }]
        }
        "#;

        let kline: CoinbaseKline = serde_json::from_str(input).unwrap();

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Coinbase, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 2);
    }
}
