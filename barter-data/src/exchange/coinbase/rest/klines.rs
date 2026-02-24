use crate::subscription::candle::{Candle, Interval};
use barter_integration::protocol::http::rest::RestRequest;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub use crate::exchange::coinbase::coinbase_interval;

/// Return the [`TimeDelta`] duration for a given [`Interval`].
///
/// Used to compute `close_time` from `open_time` since the Coinbase API
/// only returns the candle start time.
pub fn interval_duration(interval: Interval) -> TimeDelta {
    match interval {
        Interval::M1 => TimeDelta::minutes(1),
        Interval::M3 => TimeDelta::minutes(3),
        Interval::M5 => TimeDelta::minutes(5),
        Interval::M15 => TimeDelta::minutes(15),
        Interval::M30 => TimeDelta::minutes(30),
        Interval::H1 => TimeDelta::hours(1),
        Interval::H2 => TimeDelta::hours(2),
        Interval::H4 => TimeDelta::hours(4),
        Interval::H6 => TimeDelta::hours(6),
        Interval::H12 => TimeDelta::hours(12),
        Interval::D1 => TimeDelta::days(1),
        Interval::D3 => TimeDelta::days(3),
        Interval::W1 => TimeDelta::weeks(1),
        Interval::Month1 => TimeDelta::days(30),
    }
}

/// Wrapper around the Coinbase candles response JSON.
///
/// ```json
/// { "candles": [ { "start": "1672502400", ... }, ... ] }
/// ```
#[derive(Debug, Deserialize)]
pub struct CoinbaseKlinesResponse {
    pub candles: Vec<CoinbaseKlineRaw>,
}

/// Raw kline/candlestick data returned by the Coinbase REST API.
///
/// All fields are strings in the JSON response, including the unix
/// timestamp in seconds.
#[derive(Debug, Clone, Deserialize)]
pub struct CoinbaseKlineRaw {
    pub start: String,
    pub low: String,
    pub high: String,
    pub open: String,
    pub close: String,
    pub volume: String,
}

impl CoinbaseKlineRaw {
    /// Convert this raw kline into a [`Candle`], computing `close_time`
    /// from `open_time + interval_duration`.
    pub fn into_candle(self, interval: Interval) -> Result<Candle, String> {
        let start_secs: i64 = self
            .start
            .parse()
            .map_err(|e| format!("failed to parse start '{}': {}", self.start, e))?;

        let open_time = DateTime::<Utc>::from_timestamp(start_secs, 0)
            .ok_or_else(|| format!("invalid start timestamp: {}", start_secs))?;

        let close_time = open_time + interval_duration(interval);

        let open = self
            .open
            .parse::<f64>()
            .map_err(|e| format!("failed to parse open '{}': {}", self.open, e))?;

        let high = self
            .high
            .parse::<f64>()
            .map_err(|e| format!("failed to parse high '{}': {}", self.high, e))?;

        let low = self
            .low
            .parse::<f64>()
            .map_err(|e| format!("failed to parse low '{}': {}", self.low, e))?;

        let close = self
            .close
            .parse::<f64>()
            .map_err(|e| format!("failed to parse close '{}': {}", self.close, e))?;

        let volume = self
            .volume
            .parse::<f64>()
            .map_err(|e| format!("failed to parse volume '{}': {}", self.volume, e))?;

        Ok(Candle {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume: None,
            trade_count: 0,
            is_closed: true,
        })
    }
}

/// Query parameters for a Coinbase klines REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetCoinbaseKlinesParams {
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub granularity: String,
}

/// REST request to fetch kline/candlestick data from the Coinbase API.
///
/// The `path` field is an owned [`String`] because it includes the dynamic
/// `product_id` in the URL path.
#[derive(Debug, Clone)]
pub struct GetCoinbaseKlines {
    /// Full endpoint path including product_id, e.g.
    /// `/api/v3/brokerage/market/products/BTC-USD/candles`.
    pub path: String,
    /// Query parameters for the klines request.
    pub params: GetCoinbaseKlinesParams,
}

impl RestRequest for GetCoinbaseKlines {
    type Response = CoinbaseKlinesResponse;
    type QueryParams = GetCoinbaseKlinesParams;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Owned(self.path.clone())
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coinbase_interval_supported() {
        assert_eq!(coinbase_interval(Interval::M1).unwrap(), "ONE_MINUTE");
        assert_eq!(coinbase_interval(Interval::M5).unwrap(), "FIVE_MINUTES");
        assert_eq!(coinbase_interval(Interval::M15).unwrap(), "FIFTEEN_MINUTES");
        assert_eq!(coinbase_interval(Interval::M30).unwrap(), "THIRTY_MINUTES");
        assert_eq!(coinbase_interval(Interval::H1).unwrap(), "ONE_HOUR");
        assert_eq!(coinbase_interval(Interval::H2).unwrap(), "TWO_HOURS");
        assert_eq!(coinbase_interval(Interval::H6).unwrap(), "SIX_HOURS");
        assert_eq!(coinbase_interval(Interval::D1).unwrap(), "ONE_DAY");
    }

    #[test]
    fn test_coinbase_interval_unsupported() {
        assert!(coinbase_interval(Interval::M3).is_err());
        assert!(coinbase_interval(Interval::H4).is_err());
        assert!(coinbase_interval(Interval::H12).is_err());
        assert!(coinbase_interval(Interval::D3).is_err());
        assert!(coinbase_interval(Interval::W1).is_err());
        assert!(coinbase_interval(Interval::Month1).is_err());
    }

    #[test]
    fn test_interval_duration() {
        assert_eq!(interval_duration(Interval::M1), TimeDelta::minutes(1));
        assert_eq!(interval_duration(Interval::M5), TimeDelta::minutes(5));
        assert_eq!(interval_duration(Interval::H1), TimeDelta::hours(1));
        assert_eq!(interval_duration(Interval::D1), TimeDelta::days(1));
    }

    #[test]
    fn test_deserialize_coinbase_kline_raw() {
        let json = r#"{
            "start": "1672502400",
            "low": "16750.00",
            "high": "16900.50",
            "open": "16800.00",
            "close": "16850.00",
            "volume": "1234.56"
        }"#;

        let raw: CoinbaseKlineRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.start, "1672502400");
        assert_eq!(raw.low, "16750.00");
        assert_eq!(raw.high, "16900.50");
        assert_eq!(raw.open, "16800.00");
        assert_eq!(raw.close, "16850.00");
        assert_eq!(raw.volume, "1234.56");
    }

    #[test]
    fn test_deserialize_coinbase_klines_response() {
        let json = r#"{
            "candles": [
                {
                    "start": "1672502400",
                    "low": "16750.00",
                    "high": "16900.50",
                    "open": "16800.00",
                    "close": "16850.00",
                    "volume": "1234.56"
                },
                {
                    "start": "1672416000",
                    "low": "16600.00",
                    "high": "16800.00",
                    "open": "16700.00",
                    "close": "16780.00",
                    "volume": "5678.90"
                }
            ]
        }"#;

        let response: CoinbaseKlinesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.candles.len(), 2);
        assert_eq!(response.candles[0].start, "1672502400");
        assert_eq!(response.candles[1].start, "1672416000");
    }

    #[test]
    fn test_coinbase_kline_raw_into_candle() {
        let raw = CoinbaseKlineRaw {
            start: "1672502400".to_string(),
            low: "16750.00".to_string(),
            high: "16900.50".to_string(),
            open: "16800.00".to_string(),
            close: "16850.00".to_string(),
            volume: "1234.56".to_string(),
        };

        let candle = raw.into_candle(Interval::H1).unwrap();

        assert_eq!(
            candle.open_time,
            DateTime::<Utc>::from_timestamp(1672502400, 0).unwrap()
        );
        assert_eq!(
            candle.close_time,
            DateTime::<Utc>::from_timestamp(1672502400, 0).unwrap() + TimeDelta::hours(1)
        );
        assert!((candle.open - 16800.0).abs() < 1e-10);
        assert!((candle.high - 16900.5).abs() < 1e-10);
        assert!((candle.low - 16750.0).abs() < 1e-10);
        assert!((candle.close - 16850.0).abs() < 1e-10);
        assert!((candle.volume - 1234.56).abs() < 1e-10);
        assert_eq!(candle.quote_volume, None);
        assert_eq!(candle.trade_count, 0);
    }

    #[test]
    fn test_coinbase_kline_raw_into_candle_invalid_start() {
        let raw = CoinbaseKlineRaw {
            start: "not_a_number".to_string(),
            low: "16750.00".to_string(),
            high: "16900.50".to_string(),
            open: "16800.00".to_string(),
            close: "16850.00".to_string(),
            volume: "1234.56".to_string(),
        };

        assert!(raw.into_candle(Interval::H1).is_err());
    }

    #[test]
    fn test_get_coinbase_klines_path() {
        let request = GetCoinbaseKlines {
            path: "/api/v3/brokerage/market/products/BTC-USD/candles".to_string(),
            params: GetCoinbaseKlinesParams {
                start: Some(1672502400),
                end: Some(1672588800),
                granularity: "ONE_HOUR".to_string(),
            },
        };

        assert_eq!(
            request.path(),
            Cow::<str>::Owned("/api/v3/brokerage/market/products/BTC-USD/candles".to_string())
        );
        assert_eq!(GetCoinbaseKlines::method(), reqwest::Method::GET);
        assert!(request.query_params().is_some());
    }
}
