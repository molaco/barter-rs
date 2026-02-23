use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::bitmex::message::BitmexMessage,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use smol_str::format_smolstr;

/// Terse type alias for a [`BitmexKline`](BitmexKlineInner) real-time kline WebSocket message.
pub type BitmexKline = BitmexMessage<BitmexKlineInner>;

/// ### Raw Payload Examples
/// See docs: <https://www.bitmex.com/app/wsAPI>
/// #### Kline payload
/// ```json
/// {
///     "table": "tradeBin1m",
///     "action": "insert",
///     "data": [
///         {
///             "timestamp": "2023-01-01T00:01:00.000Z",
///             "symbol": "XBTUSD",
///             "open": 16850,
///             "close": 16855.5,
///             "high": 16860,
///             "low": 16845,
///             "trades": 150,
///             "volume": 123450,
///             "vwap": 16852.5,
///             "lastSize": 5,
///             "turnover": 733000000,
///             "homeNotional": 12.345,
///             "foreignNotional": 208000
///         }
///     ]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct BitmexKlineInner {
    pub timestamp: DateTime<Utc>,
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub trades: u64,
    pub volume: f64,
    #[serde(rename = "foreignNotional")]
    pub foreign_notional: f64,
}

impl Identifier<Option<SubscriptionId>> for BitmexKline {
    fn id(&self) -> Option<SubscriptionId> {
        self.data
            .first()
            .map(|kline| SubscriptionId(format_smolstr!("{}|{}", self.table, kline.symbol)))
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, BitmexKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, klines): (ExchangeId, InstrumentKey, BitmexKline),
    ) -> Self {
        Self(
            klines
                .data
                .into_iter()
                .map(|k| {
                    Ok(MarketEvent {
                        time_exchange: k.timestamp,
                        time_received: Utc::now(),
                        exchange: exchange_id,
                        instrument: instrument.clone(),
                        kind: Candle {
                            open_time: k.timestamp,
                            close_time: k.timestamp,
                            open: k.open,
                            high: k.high,
                            low: k.low,
                            close: k.close,
                            volume: k.volume,
                            quote_volume: Some(k.foreign_notional),
                            trade_count: k.trades,
                        },
                    })
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_deserialize_bitmex_kline_inner() {
        let input = r#"
        {
            "timestamp": "2023-01-01T00:01:00.000Z",
            "symbol": "XBTUSD",
            "open": 16850,
            "close": 16855.5,
            "high": 16860,
            "low": 16845,
            "trades": 150,
            "volume": 123450,
            "vwap": 16852.5,
            "lastSize": 5,
            "turnover": 733000000,
            "homeNotional": 12.345,
            "foreignNotional": 208000
        }
        "#;

        let actual: BitmexKlineInner = serde_json::from_str(input).unwrap();

        assert_eq!(
            actual.timestamp,
            Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap()
        );
        assert_eq!(actual.symbol, "XBTUSD");
        assert_eq!(actual.open, 16850.0);
        assert_eq!(actual.high, 16860.0);
        assert_eq!(actual.low, 16845.0);
        assert_eq!(actual.close, 16855.5);
        assert_eq!(actual.trades, 150);
        assert_eq!(actual.volume, 123450.0);
        assert_eq!(actual.foreign_notional, 208000.0);
    }

    #[test]
    fn test_deserialize_bitmex_kline() {
        let input = r#"
        {
            "table": "tradeBin1m",
            "action": "insert",
            "data": [
                {
                    "timestamp": "2023-01-01T00:01:00.000Z",
                    "symbol": "XBTUSD",
                    "open": 16850,
                    "close": 16855.5,
                    "high": 16860,
                    "low": 16845,
                    "trades": 150,
                    "volume": 123450,
                    "vwap": 16852.5,
                    "lastSize": 5,
                    "turnover": 733000000,
                    "homeNotional": 12.345,
                    "foreignNotional": 208000
                }
            ]
        }
        "#;

        let actual: BitmexKline = serde_json::from_str(input).unwrap();

        assert_eq!(actual.table, "tradeBin1m");
        assert_eq!(actual.data.len(), 1);
        assert_eq!(actual.data[0].symbol, "XBTUSD");
        assert_eq!(actual.data[0].open, 16850.0);
        assert_eq!(actual.data[0].close, 16855.5);
    }

    #[test]
    fn test_bitmex_kline_subscription_id() {
        let input = r#"
        {
            "table": "tradeBin1m",
            "action": "insert",
            "data": [
                {
                    "timestamp": "2023-01-01T00:01:00.000Z",
                    "symbol": "XBTUSD",
                    "open": 16850,
                    "close": 16855.5,
                    "high": 16860,
                    "low": 16845,
                    "trades": 150,
                    "volume": 123450,
                    "vwap": 16852.5,
                    "lastSize": 5,
                    "turnover": 733000000,
                    "homeNotional": 12.345,
                    "foreignNotional": 208000
                }
            ]
        }
        "#;

        let kline: BitmexKline = serde_json::from_str(input).unwrap();
        let sub_id = kline.id();

        assert_eq!(
            sub_id,
            Some(SubscriptionId::from("tradeBin1m|XBTUSD"))
        );
    }

    #[test]
    fn test_bitmex_kline_to_candle() {
        let kline = BitmexKline {
            table: "tradeBin1m".to_string(),
            data: vec![BitmexKlineInner {
                timestamp: Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
                symbol: "XBTUSD".to_string(),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                trades: 150,
                volume: 123450.0,
                foreign_notional: 208000.0,
            }],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Bitmex, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.open, 16850.0);
        assert_eq!(event.kind.high, 16860.0);
        assert_eq!(event.kind.low, 16845.0);
        assert_eq!(event.kind.close, 16855.5);
        assert_eq!(event.kind.volume, 123450.0);
        assert_eq!(event.kind.quote_volume, Some(208000.0));
        assert_eq!(event.kind.trade_count, 150);
    }

    #[test]
    fn test_deserialize_bitmex_kline_inner_missing_symbol() {
        let input = r#"
        {
            "timestamp": "2023-01-01T00:01:00.000Z",
            "open": 16850,
            "close": 16855.5,
            "high": 16860,
            "low": 16845,
            "trades": 150,
            "volume": 123450,
            "vwap": 16852.5,
            "lastSize": 5,
            "turnover": 733000000,
            "homeNotional": 12.345,
            "foreignNotional": 208000
        }
        "#;

        assert!(serde_json::from_str::<BitmexKlineInner>(input).is_err());
    }

    #[test]
    fn test_deserialize_bitmex_kline_inner_missing_timestamp() {
        let input = r#"
        {
            "symbol": "XBTUSD",
            "open": 16850,
            "close": 16855.5,
            "high": 16860,
            "low": 16845,
            "trades": 150,
            "volume": 123450,
            "vwap": 16852.5,
            "lastSize": 5,
            "turnover": 733000000,
            "homeNotional": 12.345,
            "foreignNotional": 208000
        }
        "#;

        assert!(serde_json::from_str::<BitmexKlineInner>(input).is_err());
    }

    #[test]
    fn test_deserialize_bitmex_kline_inner_invalid_timestamp() {
        let input = r#"
        {
            "timestamp": "not-a-date",
            "symbol": "XBTUSD",
            "open": 16850,
            "close": 16855.5,
            "high": 16860,
            "low": 16845,
            "trades": 150,
            "volume": 123450,
            "vwap": 16852.5,
            "lastSize": 5,
            "turnover": 733000000,
            "homeNotional": 12.345,
            "foreignNotional": 208000
        }
        "#;

        assert!(serde_json::from_str::<BitmexKlineInner>(input).is_err());
    }

    #[test]
    fn test_bitmex_kline_empty_data_array() {
        let input = r#"
        {
            "table": "tradeBin1m",
            "action": "insert",
            "data": []
        }
        "#;

        let kline: BitmexKline = serde_json::from_str(input).unwrap();
        assert!(kline.data.is_empty());

        // Converting empty data should produce empty MarketIter
        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Bitmex, "instrument_key", kline));
        assert!(market_iter.0.is_empty());
    }

    #[test]
    fn test_bitmex_kline_empty_data_subscription_id_is_none() {
        let kline = BitmexKline {
            table: "tradeBin1m".to_string(),
            data: vec![],
        };

        assert_eq!(kline.id(), None);
    }

    #[test]
    fn test_bitmex_kline_to_candle_zero_volume() {
        let kline = BitmexKline {
            table: "tradeBin1m".to_string(),
            data: vec![BitmexKlineInner {
                timestamp: Utc.with_ymd_and_hms(2023, 1, 1, 0, 1, 0).unwrap(),
                symbol: "XBTUSD".to_string(),
                open: 16850.0,
                high: 16860.0,
                low: 16845.0,
                close: 16855.5,
                trades: 0,
                volume: 0.0,
                foreign_notional: 0.0,
            }],
        };

        let market_iter: MarketIter<&str, Candle> =
            MarketIter::from((ExchangeId::Bitmex, "instrument_key", kline));

        let events: Vec<_> = market_iter.0.into_iter().collect();
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.kind.volume, 0.0);
        assert_eq!(event.kind.quote_volume, Some(0.0));
        assert_eq!(event.kind.trade_count, 0);
    }
}
