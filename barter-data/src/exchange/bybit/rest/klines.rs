use crate::subscription::candle::Candle;
use barter_integration::{de::extract_next, protocol::http::rest::RestRequest};
use chrono::DateTime;
use serde::Serialize;
use std::borrow::Cow;

pub use crate::exchange::bybit::bybit_interval;

/// REST request to fetch kline/candlestick data from the Bybit API.
///
/// The `path` field stores the endpoint path (always `/v5/market/kline`).
#[derive(Debug, Clone)]
pub struct GetBybitKlines {
    /// Endpoint path (e.g., "/v5/market/kline").
    pub path: &'static str,
    /// Query parameters for the klines request.
    pub params: GetBybitKlinesParams,
}

/// Query parameters for a Bybit klines REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetBybitKlinesParams {
    /// Market category: "spot" or "linear".
    pub category: String,
    /// Trading pair symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Kline interval (e.g., "1", "60", "D").
    pub interval: String,
    /// Optional start time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,
    /// Optional end time in milliseconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<i64>,
    /// Optional limit on the number of klines to return (max 1000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetBybitKlines {
    type Response = BybitKlinesResponse;
    type QueryParams = GetBybitKlinesParams;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(self.path)
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.params)
    }
}

/// Top-level Bybit klines API response wrapper.
///
/// Bybit returns klines nested inside `result.list`:
/// ```json
/// {
///   "retCode": 0,
///   "retMsg": "OK",
///   "result": {
///     "symbol": "BTCUSDT",
///     "category": "spot",
///     "list": [...]
///   }
/// }
/// ```
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BybitKlinesResponse {
    pub result: BybitKlinesResult,
}

/// Inner result containing the list of raw kline arrays.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct BybitKlinesResult {
    pub list: Vec<BybitKlineRaw>,
}

/// Raw kline/candlestick data returned by the Bybit REST API.
///
/// Bybit returns klines as arrays of strings:
/// `[startTime, open, high, low, close, volume, turnover]`
///
/// This struct uses a custom [`Deserialize`] implementation with a sequence
/// visitor to parse each positional element.
#[derive(Debug, Clone)]
pub struct BybitKlineRaw {
    pub start_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub turnover: String,
}

impl<'de> serde::Deserialize<'de> for BybitKlineRaw {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct BybitKlineVisitor;

        impl<'de> serde::de::Visitor<'de> for BybitKlineVisitor {
            type Value = BybitKlineRaw;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Bybit kline array with 7 string elements")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // Bybit kline array layout (7 string elements):
                // [0]  startTime  (String, ms timestamp)
                // [1]  open       (String)
                // [2]  high       (String)
                // [3]  low        (String)
                // [4]  close      (String)
                // [5]  volume     (String)
                // [6]  turnover   (String, quote volume)
                let start_time_str: String = extract_next(&mut seq, "start_time")?;
                let start_time: i64 = start_time_str.parse().map_err(|e| {
                    serde::de::Error::custom(format!(
                        "failed to parse start_time '{}': {}",
                        start_time_str, e
                    ))
                })?;
                let open = extract_next(&mut seq, "open")?;
                let high = extract_next(&mut seq, "high")?;
                let low = extract_next(&mut seq, "low")?;
                let close = extract_next(&mut seq, "close")?;
                let volume = extract_next(&mut seq, "volume")?;
                let turnover = extract_next(&mut seq, "turnover")?;

                // Skip any remaining elements
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(BybitKlineRaw {
                    start_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    turnover,
                })
            }
        }

        deserializer.deserialize_seq(BybitKlineVisitor)
    }
}

impl TryFrom<BybitKlineRaw> for Candle {
    type Error = String;

    fn try_from(raw: BybitKlineRaw) -> Result<Self, Self::Error> {
        let open_time = DateTime::from_timestamp_millis(raw.start_time)
            .ok_or_else(|| format!("invalid start_time millis: {}", raw.start_time))?;

        // Bybit does not return a close_time; derive it from start_time.
        // We use the same start_time as both open_time and close_time since the
        // actual close time depends on the interval and will be set by the
        // caller if needed. Following the same approach as the raw data.
        let close_time = open_time;

        let open = raw
            .open
            .parse::<f64>()
            .map_err(|e| format!("failed to parse open '{}': {}", raw.open, e))?;

        let high = raw
            .high
            .parse::<f64>()
            .map_err(|e| format!("failed to parse high '{}': {}", raw.high, e))?;

        let low = raw
            .low
            .parse::<f64>()
            .map_err(|e| format!("failed to parse low '{}': {}", raw.low, e))?;

        let close = raw
            .close
            .parse::<f64>()
            .map_err(|e| format!("failed to parse close '{}': {}", raw.close, e))?;

        let volume = raw
            .volume
            .parse::<f64>()
            .map_err(|e| format!("failed to parse volume '{}': {}", raw.volume, e))?;

        let turnover = raw
            .turnover
            .parse::<f64>()
            .map_err(|e| format!("failed to parse turnover '{}': {}", raw.turnover, e))?;

        Ok(Candle {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume: Some(turnover),
            trade_count: 0, // Bybit klines do not include trade count
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;

    #[test]
    fn test_bybit_interval_mapping() {
        assert_eq!(bybit_interval(Interval::M1), "1");
        assert_eq!(bybit_interval(Interval::M3), "3");
        assert_eq!(bybit_interval(Interval::M5), "5");
        assert_eq!(bybit_interval(Interval::M15), "15");
        assert_eq!(bybit_interval(Interval::M30), "30");
        assert_eq!(bybit_interval(Interval::H1), "60");
        assert_eq!(bybit_interval(Interval::H2), "120");
        assert_eq!(bybit_interval(Interval::H4), "240");
        assert_eq!(bybit_interval(Interval::H6), "360");
        assert_eq!(bybit_interval(Interval::H12), "720");
        assert_eq!(bybit_interval(Interval::D1), "D");
        assert_eq!(bybit_interval(Interval::D3), "D"); // fallback
        assert_eq!(bybit_interval(Interval::W1), "W");
        assert_eq!(bybit_interval(Interval::Month1), "M");
    }

    #[test]
    fn test_deserialize_bybit_kline_raw() {
        let json = r#"[
            "1672502400000",
            "16800.00",
            "16900.50",
            "16750.00",
            "16850.00",
            "1234.56",
            "20800000.00"
        ]"#;

        let raw: BybitKlineRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.start_time, 1672502400000);
        assert_eq!(raw.open, "16800.00");
        assert_eq!(raw.high, "16900.50");
        assert_eq!(raw.low, "16750.00");
        assert_eq!(raw.close, "16850.00");
        assert_eq!(raw.volume, "1234.56");
        assert_eq!(raw.turnover, "20800000.00");
    }

    #[test]
    fn test_deserialize_bybit_klines_response() {
        let json = r#"{
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "symbol": "BTCUSDT",
                "category": "spot",
                "list": [
                    ["1672502400000", "16800.00", "16900.50", "16750.00", "16850.00", "1234.56", "20800000.00"],
                    ["1672416000000", "16700.00", "16850.00", "16650.00", "16800.00", "2345.67", "39300000.00"]
                ]
            }
        }"#;

        let response: BybitKlinesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.result.list.len(), 2);
        assert_eq!(response.result.list[0].start_time, 1672502400000);
        assert_eq!(response.result.list[1].start_time, 1672416000000);
    }

    #[test]
    fn test_try_from_bybit_kline_raw_for_candle() {
        let raw = BybitKlineRaw {
            start_time: 1672502400000,
            open: "16800.00".to_string(),
            high: "16900.50".to_string(),
            low: "16750.00".to_string(),
            close: "16850.00".to_string(),
            volume: "1234.56".to_string(),
            turnover: "20800000.00".to_string(),
        };

        let candle = Candle::try_from(raw).unwrap();

        assert_eq!(
            candle.open_time,
            DateTime::from_timestamp_millis(1672502400000).unwrap()
        );
        assert_eq!(candle.close_time, candle.open_time);
        assert!((candle.open - 16800.0).abs() < 1e-10);
        assert!((candle.high - 16900.5).abs() < 1e-10);
        assert!((candle.low - 16750.0).abs() < 1e-10);
        assert!((candle.close - 16850.0).abs() < 1e-10);
        assert!((candle.volume - 1234.56).abs() < 1e-6);
        assert!((candle.quote_volume.unwrap() - 20800000.0).abs() < 1e-6);
        assert_eq!(candle.trade_count, 0);
    }

    #[test]
    fn test_try_from_bybit_kline_raw_invalid_open() {
        let raw = BybitKlineRaw {
            start_time: 1672502400000,
            open: "not_a_number".to_string(),
            high: "16900.50".to_string(),
            low: "16750.00".to_string(),
            close: "16850.00".to_string(),
            volume: "1234.56".to_string(),
            turnover: "20800000.00".to_string(),
        };

        assert!(Candle::try_from(raw).is_err());
    }

    #[test]
    fn test_deserialize_bybit_kline_vec() {
        let json = r#"[
            ["1672502400000","16800.00","16900.50","16750.00","16850.00","1234.56","20800000.00"],
            ["1672416000000","16700.00","16850.00","16650.00","16800.00","2345.67","39300000.00"]
        ]"#;

        let klines: Vec<BybitKlineRaw> = serde_json::from_str(json).unwrap();
        assert_eq!(klines.len(), 2);
        assert_eq!(klines[0].start_time, 1672502400000);
        assert_eq!(klines[1].start_time, 1672416000000);
    }
}
