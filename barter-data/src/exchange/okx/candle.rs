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

/// OKX real-time kline/candlestick WebSocket message.
///
/// ### Raw Payload Examples
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-candlesticks-channel>
/// ```json
/// {
///     "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
///     "data": [
///         ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","1"]
///     ]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OkxKline {
    pub arg: OkxKlineArg,
    pub data: Vec<Vec<String>>,
}

/// The `"arg"` field from an OKX kline WebSocket message, containing
/// the channel name and instrument ID.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct OkxKlineArg {
    pub channel: String,
    #[serde(rename = "instId")]
    pub inst_id: String,
}

impl Identifier<Option<SubscriptionId>> for OkxKline {
    fn id(&self) -> Option<SubscriptionId> {
        Some(
            ExchangeSub::from((self.arg.channel.as_str(), self.arg.inst_id.as_str())).id(),
        )
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, OkxKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, OkxKline),
    ) -> Self {
        Self(
            kline
                .data
                .into_iter()
                .filter_map(|fields| {
                    if fields.len() < 9 {
                        return None;
                    }

                    let ts_ms: u64 = fields[0].parse().ok()?;
                    let open_time = DateTime::<Utc>::from_timestamp_millis(ts_ms as i64)?;

                    let open: f64 = fields[1].parse().ok()?;
                    let high: f64 = fields[2].parse().ok()?;
                    let low: f64 = fields[3].parse().ok()?;
                    let close: f64 = fields[4].parse().ok()?;
                    let volume: f64 = fields[5].parse().ok()?;
                    let quote_volume: f64 = fields[7].parse().ok()?;

                    Some(Ok(MarketEvent {
                        time_exchange: open_time,
                        time_received: Utc::now(),
                        exchange: exchange_id,
                        instrument: instrument.clone(),
                        kind: Candle {
                            open_time,
                            close_time: open_time,
                            open,
                            high,
                            low,
                            close,
                            volume,
                            quote_volume: Some(quote_volume),
                            trade_count: 0,
                        },
                    }))
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::de::datetime_utc_from_epoch_duration;
    use std::time::Duration;

    #[test]
    fn test_deserialize_okx_kline() {
        let input = r#"
        {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "data": [
                ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","1"]
            ]
        }
        "#;

        let actual: OkxKline = serde_json::from_str(input).unwrap();

        assert_eq!(actual.arg.channel, "candle1m");
        assert_eq!(actual.arg.inst_id, "BTC-USDT");
        assert_eq!(actual.data.len(), 1);
        assert_eq!(actual.data[0].len(), 9);
        assert_eq!(actual.data[0][0], "1672502400000");
        assert_eq!(actual.data[0][1], "16850");
        assert_eq!(actual.data[0][8], "1");
    }

    #[test]
    fn test_okx_kline_subscription_id() {
        let input = r#"
        {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "data": [
                ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","1"]
            ]
        }
        "#;

        let kline: OkxKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("candle1m|BTC-USDT"))
        );
    }

    #[test]
    fn test_okx_kline_to_candle() {
        let kline = OkxKline {
            arg: OkxKlineArg {
                channel: "candle1m".to_string(),
                inst_id: "BTC-USDT".to_string(),
            },
            data: vec![vec![
                "1672502400000".to_string(),
                "16850".to_string(),
                "16860".to_string(),
                "16845".to_string(),
                "16855.5".to_string(),
                "12.345".to_string(),
                "208000".to_string(),
                "208000".to_string(),
                "1".to_string(),
            ]],
        };

        let expected_open_time =
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000));

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Okx, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open_time, expected_open_time);
        assert_eq!(event.kind.close_time, expected_open_time);
        assert_eq!(event.kind.open, 16850.0);
        assert_eq!(event.kind.high, 16860.0);
        assert_eq!(event.kind.low, 16845.0);
        assert_eq!(event.kind.close, 16855.5);
        assert_eq!(event.kind.volume, 12.345);
        assert_eq!(event.kind.quote_volume, Some(208000.0));
        assert_eq!(event.kind.trade_count, 0);
    }

    #[test]
    fn test_okx_kline_incomplete_candle() {
        let input = r#"
        {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "data": [
                ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","0"]
            ]
        }
        "#;

        let kline: OkxKline = serde_json::from_str(input).unwrap();

        // Incomplete candles (confirm="0") are still produced
        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Okx, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn test_deserialize_okx_kline_missing_arg() {
        let input = r#"
        {
            "data": [
                ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","1"]
            ]
        }
        "#;

        assert!(serde_json::from_str::<OkxKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_okx_kline_missing_inst_id() {
        let input = r#"
        {
            "arg": {"channel": "candle1m"},
            "data": [
                ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","1"]
            ]
        }
        "#;

        assert!(serde_json::from_str::<OkxKline>(input).is_err());
    }

    #[test]
    fn test_deserialize_okx_kline_missing_data() {
        let input = r#"
        {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"}
        }
        "#;

        assert!(serde_json::from_str::<OkxKline>(input).is_err());
    }

    #[test]
    fn test_okx_kline_empty_data_array() {
        let input = r#"
        {
            "arg": {"channel": "candle1m", "instId": "BTC-USDT"},
            "data": []
        }
        "#;

        let kline: OkxKline = serde_json::from_str(input).unwrap();
        assert!(kline.data.is_empty());

        // Converting empty data should produce empty MarketIter
        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Okx, "instrument_key", kline));
        assert!(market_iter.0.is_empty());
    }

    #[test]
    fn test_okx_kline_too_few_fields_filtered_out() {
        // OKX conversion requires at least 9 fields; fewer should be filtered out
        let kline = OkxKline {
            arg: OkxKlineArg {
                channel: "candle1m".to_string(),
                inst_id: "BTC-USDT".to_string(),
            },
            data: vec![vec![
                "1672502400000".to_string(),
                "16850".to_string(),
                "16860".to_string(),
            ]],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Okx, "instrument_key", kline));
        assert!(market_iter.0.is_empty());
    }

    #[test]
    fn test_okx_kline_to_candle_zero_volume() {
        let kline = OkxKline {
            arg: OkxKlineArg {
                channel: "candle1m".to_string(),
                inst_id: "BTC-USDT".to_string(),
            },
            data: vec![vec![
                "1672502400000".to_string(),
                "16850".to_string(),
                "16860".to_string(),
                "16845".to_string(),
                "16855.5".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
            ]],
        };

        let expected_open_time =
            datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000));

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Okx, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open_time, expected_open_time);
        assert_eq!(event.kind.volume, 0.0);
        assert_eq!(event.kind.quote_volume, Some(0.0));
        assert_eq!(event.kind.trade_count, 0);
    }

    #[test]
    fn test_okx_kline_unparseable_price_filtered_out() {
        let kline = OkxKline {
            arg: OkxKlineArg {
                channel: "candle1m".to_string(),
                inst_id: "BTC-USDT".to_string(),
            },
            data: vec![vec![
                "1672502400000".to_string(),
                "not_a_number".to_string(),
                "16860".to_string(),
                "16845".to_string(),
                "16855.5".to_string(),
                "12.345".to_string(),
                "208000".to_string(),
                "208000".to_string(),
                "1".to_string(),
            ]],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Okx, "instrument_key", kline));
        assert!(market_iter.0.is_empty());
    }
}
