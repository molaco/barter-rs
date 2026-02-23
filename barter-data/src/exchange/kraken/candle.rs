use super::KrakenMessage;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::{ExchangeSub, kraken::channel::KrakenChannel},
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    de::{datetime_utc_from_epoch_duration, extract_next},
    subscription::SubscriptionId,
};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Terse type alias for a [`Kraken`](super::Kraken) real-time OHLC WebSocket message.
pub type KrakenKline = KrakenMessage<KrakenKlineInner>;

/// [`Kraken`](super::Kraken) real-time OHLC candle data with an associated [`SubscriptionId`]
/// (eg/ "ohlc-1|XBT/USD").
///
/// See [`KrakenMessage`] for full raw payload examples.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.kraken.com/websockets/#message-ohlc>
/// ```json
/// [
///     42,
///     [
///         "1672502400.000000",
///         "1672502459.000000",
///         "16850.00000",
///         "16860.00000",
///         "16845.00000",
///         "16855.50000",
///         "16852.50000",
///         "12.34500000",
///         150
///     ],
///     "ohlc-1",
///     "XBT/USD"
/// ]
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub struct KrakenKlineInner {
    pub subscription_id: SubscriptionId,
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub vwap: f64,
    pub volume: f64,
    pub trade_count: u64,
}

impl Identifier<Option<SubscriptionId>> for KrakenKlineInner {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, KrakenKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, KrakenKline),
    ) -> Self {
        match kline {
            KrakenKline::Data(kline) => Self(vec![Ok(MarketEvent {
                time_exchange: kline.close_time,
                time_received: Utc::now(),
                exchange: exchange_id,
                instrument,
                kind: Candle {
                    open_time: kline.open_time,
                    close_time: kline.close_time,
                    open: kline.open,
                    high: kline.high,
                    low: kline.low,
                    close: kline.close,
                    volume: kline.volume,
                    quote_volume: None,
                    trade_count: kline.trade_count,
                },
            })]),
            KrakenKline::Event(_) => Self(vec![]),
        }
    }
}

impl<'de> serde::de::Deserialize<'de> for KrakenKlineInner {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SeqVisitor;

        impl<'de> serde::de::Visitor<'de> for SeqVisitor {
            type Value = KrakenKlineInner;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("KrakenKlineInner struct from the Kraken WebSocket API")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // KrakenKline Sequence Format:
                // [channelID, [time, etime, open, high, low, close, vwap, volume, count], channelName, pair]
                // <https://docs.kraken.com/websockets/#message-ohlc>

                // Extract deprecated channelID & ignore
                let _: serde::de::IgnoredAny = extract_next(&mut seq, "channelID")?;

                // Extract OHLC data array
                let ohlc_data: Vec<serde_json::Value> =
                    extract_next(&mut seq, "ohlc_data")?;

                if ohlc_data.len() < 9 {
                    return Err(serde::de::Error::custom(format!(
                        "expected at least 9 elements in OHLC data array, got {}",
                        ohlc_data.len()
                    )));
                }

                // Parse time (epoch seconds as string)
                let open_time = parse_epoch_str(&ohlc_data[0])?;
                let close_time = parse_epoch_str(&ohlc_data[1])?;

                // Parse price/volume fields (strings)
                let open = parse_f64_str(&ohlc_data[2], "open")?;
                let high = parse_f64_str(&ohlc_data[3], "high")?;
                let low = parse_f64_str(&ohlc_data[4], "low")?;
                let close = parse_f64_str(&ohlc_data[5], "close")?;
                let vwap = parse_f64_str(&ohlc_data[6], "vwap")?;
                let volume = parse_f64_str(&ohlc_data[7], "volume")?;

                // Parse trade count (integer)
                let trade_count = ohlc_data[8]
                    .as_u64()
                    .ok_or_else(|| {
                        serde::de::Error::custom(format!(
                            "expected u64 for trade_count, got {:?}",
                            ohlc_data[8]
                        ))
                    })?;

                // Extract channelName (eg/ "ohlc-1")
                let channel_name =
                    extract_next::<SeqAccessor, String>(&mut seq, "channelName")?;

                // Extract pair (eg/ "XBT/USD") & map to SubscriptionId (ie/ "ohlc-1|XBT/USD")
                let subscription_id =
                    extract_next::<SeqAccessor, String>(&mut seq, "pair").map(|pair| {
                        ExchangeSub::from((KrakenChannel(channel_name), pair)).id()
                    })?;

                // Ignore any additional elements or SerDe will fail
                //  '--> Exchange may add fields without warning
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(KrakenKlineInner {
                    subscription_id,
                    open_time,
                    close_time,
                    open,
                    high,
                    low,
                    close,
                    vwap,
                    volume,
                    trade_count,
                })
            }
        }

        // Use Visitor implementation to deserialise the KrakenKlineInner
        deserializer.deserialize_seq(SeqVisitor)
    }
}

/// Parse a [`serde_json::Value`] string containing an epoch seconds timestamp to
/// [`DateTime<Utc>`].
fn parse_epoch_str<E: serde::de::Error>(value: &serde_json::Value) -> Result<DateTime<Utc>, E> {
    let s = value
        .as_str()
        .ok_or_else(|| E::custom(format!("expected string for epoch timestamp, got {:?}", value)))?;
    let secs: f64 = s.parse().map_err(E::custom)?;
    Ok(datetime_utc_from_epoch_duration(
        std::time::Duration::from_secs_f64(secs),
    ))
}

/// Parse a [`serde_json::Value`] string containing a decimal number to [`f64`].
fn parse_f64_str<E: serde::de::Error>(
    value: &serde_json::Value,
    field_name: &str,
) -> Result<f64, E> {
    let s = value.as_str().ok_or_else(|| {
        E::custom(format!(
            "expected string for {}, got {:?}",
            field_name, value
        ))
    })?;
    s.parse().map_err(E::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::{
        de::datetime_utc_from_epoch_duration, error::SocketError, subscription::SubscriptionId,
    };

    #[test]
    fn test_deserialize_kraken_kline() {
        struct TestCase {
            input: &'static str,
            expected: Result<KrakenKline, SocketError>,
        }

        let tests = vec![TestCase {
            // TC0: valid KrakenKline::Data(KrakenKlineInner)
            input: r#"
                [
                    42,
                    [
                        "1672502400.000000",
                        "1672502459.000000",
                        "16850.00000",
                        "16860.00000",
                        "16845.00000",
                        "16855.50000",
                        "16852.50000",
                        "12.34500000",
                        150
                    ],
                    "ohlc-1",
                    "XBT/USD"
                ]
            "#,
            expected: Ok(KrakenKline::Data(KrakenKlineInner {
                subscription_id: SubscriptionId::from("ohlc-1|XBT/USD"),
                open_time: datetime_utc_from_epoch_duration(
                    std::time::Duration::from_secs_f64(1672502400.000000),
                ),
                close_time: datetime_utc_from_epoch_duration(
                    std::time::Duration::from_secs_f64(1672502459.000000),
                ),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                vwap: 16852.5,
                volume: 12.345,
                trade_count: 150,
            })),
        }];

        for (index, test) in tests.into_iter().enumerate() {
            let actual = serde_json::from_str::<KrakenKline>(test.input);
            match (actual, test.expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(actual, expected, "TC{} failed", index)
                }
                (Err(_), Err(_)) => {
                    // Test passed
                }
                (actual, expected) => {
                    // Test failed
                    panic!(
                        "TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                    );
                }
            }
        }
    }

    #[test]
    fn test_kraken_kline_subscription_id() {
        let input = r#"
            [
                42,
                [
                    "1672502400.000000",
                    "1672502459.000000",
                    "16850.00000",
                    "16860.00000",
                    "16845.00000",
                    "16855.50000",
                    "16852.50000",
                    "12.34500000",
                    150
                ],
                "ohlc-1",
                "XBT/USD"
            ]
        "#;

        let kline: KrakenKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("ohlc-1|XBT/USD"))
        );
    }

    #[test]
    fn test_kraken_kline_to_candle() {
        let kline = KrakenKline::Data(KrakenKlineInner {
            subscription_id: SubscriptionId::from("ohlc-1|XBT/USD"),
            open_time: datetime_utc_from_epoch_duration(
                std::time::Duration::from_secs_f64(1672502400.0),
            ),
            close_time: datetime_utc_from_epoch_duration(
                std::time::Duration::from_secs_f64(1672502459.0),
            ),
            open: 16850.0,
            high: 16860.0,
            low: 16845.0,
            close: 16855.5,
            vwap: 16852.5,
            volume: 12.345,
            trade_count: 150,
        });

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Kraken, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open, 16850.0);
        assert_eq!(event.kind.high, 16860.0);
        assert_eq!(event.kind.low, 16845.0);
        assert_eq!(event.kind.close, 16855.5);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.quote_volume, None);
        assert_eq!(event.kind.trade_count, 150);
    }

    #[test]
    fn test_kraken_kline_event_returns_empty() {
        use super::super::message::KrakenEvent;

        let kline = KrakenKline::Event(KrakenEvent::Heartbeat);

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Kraken, "instrument_key", kline));

        assert!(market_iter.0.is_empty());
    }
}
