use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    de::{datetime_utc_from_epoch_duration, extract_next},
    subscription::SubscriptionId,
};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::time::Duration;

/// [`Bitfinex`](super::Bitfinex) candle data.
///
/// ### Raw Payload Examples
/// Format: \[MTS, OPEN, CLOSE, HIGH, LOW, VOLUME\]
///
/// Note: CLOSE comes before HIGH/LOW (different from standard OHLCV).
///
/// See docs: <https://docs.bitfinex.com/reference/ws-public-candles>
#[derive(Clone, Copy, PartialEq, PartialOrd, Debug, Serialize)]
pub struct BitfinexCandle {
    pub time: DateTime<Utc>,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub volume: f64,
}

impl<'de> serde::Deserialize<'de> for BitfinexCandle {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct SeqVisitor;

        impl<'de> serde::de::Visitor<'de> for SeqVisitor {
            type Value = BitfinexCandle;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter
                    .write_str("BitfinexCandle [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME] from the Bitfinex WebSocket API")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // Candle: [MTS, OPEN, CLOSE, HIGH, LOW, VOLUME]
                let time_millis: u64 = extract_next(&mut seq, "mts")?;
                let open = extract_next(&mut seq, "open")?;
                let close = extract_next(&mut seq, "close")?;
                let high = extract_next(&mut seq, "high")?;
                let low = extract_next(&mut seq, "low")?;
                let volume = extract_next(&mut seq, "volume")?;

                // Ignore any additional elements or SerDe will fail
                //  '--> Bitfinex may add fields without warning
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(BitfinexCandle {
                    time: datetime_utc_from_epoch_duration(Duration::from_millis(time_millis)),
                    open,
                    close,
                    high,
                    low,
                    volume,
                })
            }
        }

        // Use Visitor implementation to deserialise the BitfinexCandle message
        deserializer.deserialize_seq(SeqVisitor)
    }
}

/// [`Bitfinex`](super::Bitfinex) candle message payload variants.
///
/// See docs: <https://docs.bitfinex.com/reference/ws-public-candles>
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub enum BitfinexCandlePayload {
    Heartbeat,
    Candle(BitfinexCandle),
    Snapshot(Vec<BitfinexCandle>),
}

/// [`Bitfinex`](super::Bitfinex) candle WebSocket message.
///
/// Format: \[CHANNEL_ID, \<payload\>\]
/// where payload is `"hb"`, `[candle_data]`, or `[[candle_data], ...]`.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.bitfinex.com/reference/ws-public-candles>
///
/// #### Heartbeat
/// ```json
/// [42, "hb"]
/// ```
///
/// #### Single Candle Update
/// ```json
/// [42, [1672502400000, 16850, 16855.5, 16860, 16845, 12.345]]
/// ```
///
/// #### Snapshot
/// ```json
/// [42, [[1672502400000, 16850, 16855.5, 16860, 16845, 12.345], [...]]]
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub struct BitfinexCandleMessage {
    pub channel_id: u32,
    pub payload: BitfinexCandlePayload,
}

impl Identifier<Option<SubscriptionId>> for BitfinexCandleMessage {
    fn id(&self) -> Option<SubscriptionId> {
        match &self.payload {
            BitfinexCandlePayload::Heartbeat => None,
            _ => Some(SubscriptionId::from(self.channel_id.to_string())),
        }
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, BitfinexCandleMessage)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, message): (ExchangeId, InstrumentKey, BitfinexCandleMessage),
    ) -> Self {
        match message.payload {
            BitfinexCandlePayload::Heartbeat => Self(vec![]),
            BitfinexCandlePayload::Candle(c) => Self(vec![Ok(MarketEvent {
                time_exchange: c.time,
                time_received: Utc::now(),
                exchange: exchange_id,
                instrument,
                kind: Candle {
                    open_time: c.time,
                    close_time: c.time,
                    open: c.open,
                    high: c.high,
                    low: c.low,
                    close: c.close,
                    volume: c.volume,
                    quote_volume: None,
                    trade_count: 0,
                },
            })]),
            BitfinexCandlePayload::Snapshot(_) => {
                // Ignore snapshot, only emit update candles
                // Snapshots are historical data sent on subscribe
                Self(vec![])
            }
        }
    }
}

impl<'de> serde::Deserialize<'de> for BitfinexCandleMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct SeqVisitor;

        impl<'de> serde::de::Visitor<'de> for SeqVisitor {
            type Value = BitfinexCandleMessage;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "BitfinexCandleMessage struct from the Bitfinex WebSocket API",
                )
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // Candle: [CHANNEL_ID, <payload>]
                // Heartbeat: [CHANNEL_ID, "hb"]
                // Snapshot: [CHANNEL_ID, [[MTS, O, C, H, L, V], ...]]
                // Update: [CHANNEL_ID, [MTS, O, C, H, L, V]]

                // Extract CHANNEL_ID used to identify SubscriptionId: 1st element of the sequence
                let channel_id: u32 = extract_next(&mut seq, "channel_id")?;

                // Extract second element to determine payload type
                let second: serde_json::Value = extract_next(&mut seq, "payload")?;

                let payload = match &second {
                    serde_json::Value::String(s) if s == "hb" => {
                        BitfinexCandlePayload::Heartbeat
                    }
                    serde_json::Value::Array(arr)
                        if arr.first().map_or(false, |v| v.is_array()) =>
                    {
                        // Snapshot: [[MTS, O, C, H, L, V], [MTS, O, C, H, L, V], ...]
                        let candles: Vec<BitfinexCandle> =
                            serde_json::from_value(second).map_err(serde::de::Error::custom)?;
                        BitfinexCandlePayload::Snapshot(candles)
                    }
                    serde_json::Value::Array(_) => {
                        // Single candle update: [MTS, O, C, H, L, V]
                        let candle: BitfinexCandle =
                            serde_json::from_value(second).map_err(serde::de::Error::custom)?;
                        BitfinexCandlePayload::Candle(candle)
                    }
                    other => {
                        return Err(serde::de::Error::custom(format!(
                            "unexpected candle payload: {:?}",
                            other
                        )));
                    }
                };

                // Ignore any additional elements or SerDe will fail
                //  '--> Bitfinex may add fields without warning
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(BitfinexCandleMessage {
                    channel_id,
                    payload,
                })
            }
        }

        // Use Visitor implementation to deserialise the WebSocket BitfinexCandleMessage
        deserializer.deserialize_seq(SeqVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::de::datetime_utc_from_epoch_duration;

    #[test]
    fn test_de_bitfinex_candle() {
        let input = r#"[1672502400000, 16850, 16855.5, 16860, 16845, 12.345]"#;
        let candle: BitfinexCandle = serde_json::from_str(input).unwrap();

        assert_eq!(
            candle.time,
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000))
        );
        assert_eq!(candle.open, 16850.0);
        assert_eq!(candle.close, 16855.5);
        assert_eq!(candle.high, 16860.0);
        assert_eq!(candle.low, 16845.0);
        assert_eq!(candle.volume, 12.345);
    }

    #[test]
    fn test_de_bitfinex_candle_message_heartbeat() {
        let input = r#"[42, "hb"]"#;
        let message: BitfinexCandleMessage = serde_json::from_str(input).unwrap();

        assert_eq!(message.channel_id, 42);
        assert_eq!(message.payload, BitfinexCandlePayload::Heartbeat);
    }

    #[test]
    fn test_de_bitfinex_candle_message_update() {
        let input = r#"[42, [1672502400000, 16850, 16855.5, 16860, 16845, 12.345]]"#;
        let message: BitfinexCandleMessage = serde_json::from_str(input).unwrap();

        assert_eq!(message.channel_id, 42);
        match &message.payload {
            BitfinexCandlePayload::Candle(c) => {
                assert_eq!(
                    c.time,
                    datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000))
                );
                assert_eq!(c.open, 16850.0);
                assert_eq!(c.close, 16855.5);
                assert_eq!(c.high, 16860.0);
                assert_eq!(c.low, 16845.0);
                assert_eq!(c.volume, 12.345);
            }
            other => panic!("expected Candle payload, got {:?}", other),
        }
    }

    #[test]
    fn test_de_bitfinex_candle_message_snapshot() {
        let input = r#"[42, [[1672502400000, 16850, 16855.5, 16860, 16845, 12.345], [1672502460000, 16855, 16860.0, 16865, 16850, 8.123]]]"#;
        let message: BitfinexCandleMessage = serde_json::from_str(input).unwrap();

        assert_eq!(message.channel_id, 42);
        match &message.payload {
            BitfinexCandlePayload::Snapshot(candles) => {
                assert_eq!(candles.len(), 2);
                assert_eq!(candles[0].open, 16850.0);
                assert_eq!(candles[0].close, 16855.5);
                assert_eq!(candles[1].open, 16855.0);
                assert_eq!(candles[1].close, 16860.0);
            }
            other => panic!("expected Snapshot payload, got {:?}", other),
        }
    }

    #[test]
    fn test_bitfinex_candle_subscription_id() {
        // Heartbeat should return None
        let heartbeat = BitfinexCandleMessage {
            channel_id: 42,
            payload: BitfinexCandlePayload::Heartbeat,
        };
        assert_eq!(heartbeat.id(), None);

        // Candle update should return Some(SubscriptionId("42"))
        let update = BitfinexCandleMessage {
            channel_id: 42,
            payload: BitfinexCandlePayload::Candle(BitfinexCandle {
                time: datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000)),
                open: 16850.0,
                close: 16855.5,
                high: 16860.0,
                low: 16845.0,
                volume: 12.345,
            }),
        };
        assert_eq!(update.id(), Some(SubscriptionId::from("42")));

        // Snapshot should also return Some(SubscriptionId("42"))
        let snapshot = BitfinexCandleMessage {
            channel_id: 42,
            payload: BitfinexCandlePayload::Snapshot(vec![]),
        };
        assert_eq!(snapshot.id(), Some(SubscriptionId::from("42")));
    }

    #[test]
    fn test_bitfinex_candle_to_market_iter() {
        // Heartbeat produces empty MarketIter
        let heartbeat = BitfinexCandleMessage {
            channel_id: 42,
            payload: BitfinexCandlePayload::Heartbeat,
        };
        let iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Bitfinex, "instrument_key", heartbeat));
        assert!(iter.0.is_empty());

        // Snapshot produces empty MarketIter (snapshots are ignored)
        let snapshot = BitfinexCandleMessage {
            channel_id: 42,
            payload: BitfinexCandlePayload::Snapshot(vec![BitfinexCandle {
                time: datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000)),
                open: 16850.0,
                close: 16855.5,
                high: 16860.0,
                low: 16845.0,
                volume: 12.345,
            }]),
        };
        let iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Bitfinex, "instrument_key", snapshot));
        assert!(iter.0.is_empty());

        // Candle update produces one MarketEvent
        let update = BitfinexCandleMessage {
            channel_id: 42,
            payload: BitfinexCandlePayload::Candle(BitfinexCandle {
                time: datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000)),
                open: 16850.0,
                close: 16855.5,
                high: 16860.0,
                low: 16845.0,
                volume: 12.345,
            }),
        };
        let iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Bitfinex, "instrument_key", update));
        let events: Vec<_> = iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open, 16850.0);
        assert_eq!(event.kind.high, 16860.0);
        assert_eq!(event.kind.low, 16845.0);
        assert_eq!(event.kind.close, 16855.5);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.quote_volume, None);
        assert_eq!(event.kind.trade_count, 0);
    }
}
