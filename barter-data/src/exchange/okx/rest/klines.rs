use crate::subscription::candle::{Candle, Interval};
use barter_integration::{de::extract_next, protocol::http::rest::RestRequest};
use chrono::{DateTime, TimeDelta};
use serde::Serialize;
use std::borrow::Cow;

pub use crate::exchange::okx::okx_interval;

/// Return the duration of one candle for the given [`Interval`].
///
/// Used to compute `close_time` from `open_time` since OKX does not return
/// a close timestamp.
fn interval_duration(interval: Interval) -> TimeDelta {
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
        // Approximate month as 30 days.
        Interval::Month1 => TimeDelta::days(30),
    }
}

/// Wrapper around the OKX REST klines response.
///
/// OKX returns:
/// ```json
/// {
///   "code": "0",
///   "msg": "",
///   "data": [["ts","o","h","l","c","vol","volCcy","volCcyQuote","confirm"], ...]
/// }
/// ```
///
/// A `code` of `"0"` indicates success. Any other value is an error.
#[derive(Debug, serde::Deserialize)]
pub struct OkxKlinesResponse {
    pub code: String,
    pub msg: String,
    pub data: Vec<OkxKlineRaw>,
}

/// Raw kline data returned by the OKX REST API.
///
/// OKX returns klines as 9-element string arrays:
/// `[ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]`
///
/// All elements are strings. This struct uses a custom [`Deserialize`]
/// implementation with a sequence visitor to parse each positional element.
#[derive(Debug, Clone)]
pub struct OkxKlineRaw {
    /// Candle open time in milliseconds since epoch.
    pub ts: String,
    /// Open price.
    pub open: String,
    /// High price.
    pub high: String,
    /// Low price.
    pub low: String,
    /// Close price.
    pub close: String,
    /// Trading volume in base currency.
    pub volume: String,
    /// Trading volume in quote currency.
    pub vol_ccy_quote: String,
    /// Whether this candle is confirmed: "0" (incomplete) or "1" (complete).
    pub confirm: String,
}

impl<'de> serde::Deserialize<'de> for OkxKlineRaw {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct OkxKlineVisitor;

        impl<'de> serde::de::Visitor<'de> for OkxKlineVisitor {
            type Value = OkxKlineRaw;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an OKX kline array with 9 string elements")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // OKX kline array layout (9 elements, all strings):
                // [0] ts             (open time ms)
                // [1] open
                // [2] high
                // [3] low
                // [4] close
                // [5] vol            (base volume)
                // [6] volCcy         (quote volume or contract volume)
                // [7] volCcyQuote    (quote volume)
                // [8] confirm        ("0" or "1")
                let ts: String = extract_next(&mut seq, "ts")?;
                let open: String = extract_next(&mut seq, "open")?;
                let high: String = extract_next(&mut seq, "high")?;
                let low: String = extract_next(&mut seq, "low")?;
                let close: String = extract_next(&mut seq, "close")?;
                let volume: String = extract_next(&mut seq, "vol")?;
                // volCcy — skip (we use volCcyQuote instead)
                let _vol_ccy: String = extract_next(&mut seq, "volCcy")?;
                let vol_ccy_quote: String = extract_next(&mut seq, "volCcyQuote")?;
                let confirm: String = extract_next(&mut seq, "confirm")?;

                // Drain any trailing elements
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(OkxKlineRaw {
                    ts,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    vol_ccy_quote,
                    confirm,
                })
            }
        }

        deserializer.deserialize_seq(OkxKlineVisitor)
    }
}

/// Convert an [`OkxKlineRaw`] into a normalised [`Candle`].
///
/// The `close_time` is derived from `open_time + interval_duration - 1ms` since
/// OKX does not return a close timestamp. The `interval` field is required for
/// this computation and must be set before calling this conversion.
///
/// Since `TryFrom` cannot accept extra parameters, callers should use
/// [`try_into_candle`] instead.
impl OkxKlineRaw {
    /// Convert into a [`Candle`], computing `close_time` from the given [`Interval`].
    pub fn try_into_candle(self, interval: Interval) -> Result<Candle, String> {
        let ts_ms: i64 = self
            .ts
            .parse()
            .map_err(|e| format!("failed to parse ts '{}': {}", self.ts, e))?;

        let open_time = DateTime::from_timestamp_millis(ts_ms)
            .ok_or_else(|| format!("invalid ts millis: {}", ts_ms))?;

        let close_time = open_time + interval_duration(interval) - TimeDelta::milliseconds(1);

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

        let quote_volume = self
            .vol_ccy_quote
            .parse::<f64>()
            .map_err(|e| {
                format!(
                    "failed to parse vol_ccy_quote '{}': {}",
                    self.vol_ccy_quote, e
                )
            })?;

        Ok(Candle {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume: Some(quote_volume),
            // OKX does not return trade count in the klines response.
            trade_count: 0,
            is_closed: true,
        })
    }

    /// Return the open timestamp in milliseconds.
    pub fn ts_millis(&self) -> Result<i64, String> {
        self.ts
            .parse()
            .map_err(|e| format!("failed to parse ts '{}': {}", self.ts, e))
    }
}

/// REST request to fetch kline/candlestick data from the OKX API.
///
/// The `path` field allows switching between `/api/v5/market/candles`
/// (recent data) and `/api/v5/market/history-candles` (historical data).
#[derive(Debug, Clone)]
pub struct GetOkxKlines {
    /// Endpoint path (e.g. `/api/v5/market/candles` or `/api/v5/market/history-candles`).
    pub path: &'static str,
    /// Query parameters for the klines request.
    pub params: GetOkxKlinesParams,
}

/// Query parameters for an OKX klines REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetOkxKlinesParams {
    /// Instrument ID (e.g. "BTC-USDT").
    #[serde(rename = "instId")]
    pub inst_id: String,
    /// Candlestick interval (e.g. "1m", "1H", "1D").
    pub bar: String,
    /// Pagination cursor: returns records newer than this timestamp (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<i64>,
    /// Pagination cursor: returns records older than this timestamp (ms).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<i64>,
    /// Maximum number of records to return (max 100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetOkxKlines {
    type Response = OkxKlinesResponse;
    type QueryParams = GetOkxKlinesParams;
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

/// OKX klines endpoint for recent data (last 1440 data points).
pub const OKX_KLINES_PATH: &str = "/api/v5/market/candles";

/// OKX klines endpoint for historical data (older than 1440 bars).
pub const OKX_HISTORY_KLINES_PATH: &str = "/api/v5/market/history-candles";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;

    #[test]
    fn test_okx_interval_mapping() {
        assert_eq!(okx_interval(Interval::M1), "1m");
        assert_eq!(okx_interval(Interval::M3), "3m");
        assert_eq!(okx_interval(Interval::M5), "5m");
        assert_eq!(okx_interval(Interval::M15), "15m");
        assert_eq!(okx_interval(Interval::M30), "30m");
        assert_eq!(okx_interval(Interval::H1), "1H");
        assert_eq!(okx_interval(Interval::H2), "2H");
        assert_eq!(okx_interval(Interval::H4), "4H");
        assert_eq!(okx_interval(Interval::H6), "6H");
        assert_eq!(okx_interval(Interval::H12), "12H");
        assert_eq!(okx_interval(Interval::D1), "1D");
        assert_eq!(okx_interval(Interval::D3), "3D");
        assert_eq!(okx_interval(Interval::W1), "1W");
        assert_eq!(okx_interval(Interval::Month1), "1M");
    }

    #[test]
    fn test_deserialize_okx_kline_raw() {
        let json = r#"[
            "1672502400000",
            "16800.00",
            "16900.50",
            "16750.00",
            "16850.00",
            "1234.56",
            "20800000.00",
            "20800000.00",
            "1"
        ]"#;

        let raw: OkxKlineRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.ts, "1672502400000");
        assert_eq!(raw.open, "16800.00");
        assert_eq!(raw.high, "16900.50");
        assert_eq!(raw.low, "16750.00");
        assert_eq!(raw.close, "16850.00");
        assert_eq!(raw.volume, "1234.56");
        assert_eq!(raw.vol_ccy_quote, "20800000.00");
        assert_eq!(raw.confirm, "1");
    }

    #[test]
    fn test_deserialize_okx_klines_response() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [
                ["1672502400000","16800.00","16900.50","16750.00","16850.00","1234.56","20800000.00","20800000.00","1"],
                ["1672416000000","16700.00","16850.00","16650.00","16800.00","2345.67","39200000.00","39200000.00","1"]
            ]
        }"#;

        let response: OkxKlinesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.code, "0");
        assert_eq!(response.msg, "");
        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].ts, "1672502400000");
        assert_eq!(response.data[1].ts, "1672416000000");
    }

    #[test]
    fn test_try_into_candle() {
        let raw = OkxKlineRaw {
            ts: "1672502400000".to_string(),
            open: "16800.00".to_string(),
            high: "16900.50".to_string(),
            low: "16750.00".to_string(),
            close: "16850.00".to_string(),
            volume: "1234.56".to_string(),
            vol_ccy_quote: "20800000.00".to_string(),
            confirm: "1".to_string(),
        };

        let candle = raw.try_into_candle(Interval::D1).unwrap();

        assert_eq!(
            candle.open_time,
            DateTime::from_timestamp_millis(1672502400000).unwrap()
        );
        // close_time = open_time + 1 day - 1ms
        assert_eq!(
            candle.close_time,
            DateTime::from_timestamp_millis(1672502400000 + 86_400_000 - 1).unwrap()
        );
        assert!((candle.open - 16800.0).abs() < 1e-10);
        assert!((candle.high - 16900.50).abs() < 1e-10);
        assert!((candle.low - 16750.0).abs() < 1e-10);
        assert!((candle.close - 16850.0).abs() < 1e-10);
        assert!((candle.volume - 1234.56).abs() < 1e-6);
        assert!((candle.quote_volume.unwrap() - 20800000.0).abs() < 1e-6);
        assert_eq!(candle.trade_count, 0);
    }

    #[test]
    fn test_try_into_candle_1m_interval() {
        let raw = OkxKlineRaw {
            ts: "1672502400000".to_string(),
            open: "100.0".to_string(),
            high: "110.0".to_string(),
            low: "90.0".to_string(),
            close: "105.0".to_string(),
            volume: "500.0".to_string(),
            vol_ccy_quote: "50000.0".to_string(),
            confirm: "1".to_string(),
        };

        let candle = raw.try_into_candle(Interval::M1).unwrap();

        // close_time = open_time + 1 minute - 1ms
        assert_eq!(
            candle.close_time,
            DateTime::from_timestamp_millis(1672502400000 + 60_000 - 1).unwrap()
        );
    }

    #[test]
    fn test_try_into_candle_invalid_ts() {
        let raw = OkxKlineRaw {
            ts: "not_a_number".to_string(),
            open: "100.0".to_string(),
            high: "110.0".to_string(),
            low: "90.0".to_string(),
            close: "105.0".to_string(),
            volume: "500.0".to_string(),
            vol_ccy_quote: "50000.0".to_string(),
            confirm: "1".to_string(),
        };

        let result = raw.try_into_candle(Interval::M1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse ts"));
    }

    #[test]
    fn test_try_into_candle_invalid_price() {
        let raw = OkxKlineRaw {
            ts: "1672502400000".to_string(),
            open: "bad_price".to_string(),
            high: "110.0".to_string(),
            low: "90.0".to_string(),
            close: "105.0".to_string(),
            volume: "500.0".to_string(),
            vol_ccy_quote: "50000.0".to_string(),
            confirm: "1".to_string(),
        };

        let result = raw.try_into_candle(Interval::M1);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse open"));
    }

    #[test]
    fn test_okx_klines_error_response() {
        let json = r#"{
            "code": "51001",
            "msg": "Instrument ID does not exist",
            "data": []
        }"#;

        let response: OkxKlinesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.code, "51001");
        assert_eq!(response.msg, "Instrument ID does not exist");
        assert!(response.data.is_empty());
    }

    #[test]
    fn test_ts_millis() {
        let raw = OkxKlineRaw {
            ts: "1672502400000".to_string(),
            open: "100.0".to_string(),
            high: "110.0".to_string(),
            low: "90.0".to_string(),
            close: "105.0".to_string(),
            volume: "500.0".to_string(),
            vol_ccy_quote: "50000.0".to_string(),
            confirm: "1".to_string(),
        };

        assert_eq!(raw.ts_millis().unwrap(), 1672502400000);
    }
}
