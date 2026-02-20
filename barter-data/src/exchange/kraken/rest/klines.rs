use crate::{error::DataError, subscription::candle::Interval};
use barter_integration::{de::extract_next, protocol::http::rest::RestRequest};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::subscription::candle::Candle;

/// Convert a normalised [`Interval`] to the Kraken API integer-minutes interval.
///
/// Kraken supports: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600.
/// Returns a [`DataError`] for unsupported intervals (3m, 2h, 6h, 12h, 3d, 1M).
pub fn kraken_interval(interval: Interval) -> Result<u32, DataError> {
    match interval {
        Interval::M1 => Ok(1),
        Interval::M5 => Ok(5),
        Interval::M15 => Ok(15),
        Interval::M30 => Ok(30),
        Interval::H1 => Ok(60),
        Interval::H4 => Ok(240),
        Interval::D1 => Ok(1440),
        Interval::W1 => Ok(10080),
        // Kraken's 21600-minute interval (half-month) has no direct Interval variant;
        // these intervals are unsupported by Kraken:
        Interval::M3 => Err(DataError::Socket(
            "Kraken does not support 3m interval".to_string(),
        )),
        Interval::H2 => Err(DataError::Socket(
            "Kraken does not support 2h interval".to_string(),
        )),
        Interval::H6 => Err(DataError::Socket(
            "Kraken does not support 6h interval".to_string(),
        )),
        Interval::H12 => Err(DataError::Socket(
            "Kraken does not support 12h interval".to_string(),
        )),
        Interval::D3 => Err(DataError::Socket(
            "Kraken does not support 3d interval".to_string(),
        )),
        Interval::Month1 => Err(DataError::Socket(
            "Kraken does not support 1M interval".to_string(),
        )),
    }
}

/// Top-level Kraken OHLC response.
///
/// Kraken returns:
/// ```json
/// {
///   "error": [],
///   "result": {
///     "XXBTZUSD": [[...], ...],
///     "last": 1672588800
///   }
/// }
/// ```
///
/// The `result` object contains a dynamic key for the pair data and a `last`
/// cursor for pagination. We deserialize `result` as a raw
/// [`serde_json::Value`] and parse it manually.
#[derive(Debug, Deserialize)]
pub struct KrakenOhlcResponse {
    pub error: Vec<String>,
    pub result: serde_json::Value,
}

impl KrakenOhlcResponse {
    /// Parse the raw `result` value into a vector of [`KrakenKlineRaw`] entries
    /// and an optional `last` pagination cursor.
    ///
    /// Iterates the keys in `result`, treating `"last"` as the cursor and
    /// the remaining key as the OHLC data array. The pair key in the response
    /// may differ from the requested pair (e.g., `"XXBTZUSD"` vs `"XBTUSD"`).
    pub fn parse_klines(&self) -> Result<(Vec<KrakenKlineRaw>, Option<i64>), DataError> {
        let obj = self
            .result
            .as_object()
            .ok_or_else(|| DataError::Socket("Kraken OHLC result is not an object".to_string()))?;

        let mut last: Option<i64> = None;
        let mut klines: Option<Vec<KrakenKlineRaw>> = None;

        for (key, value) in obj {
            if key == "last" {
                last = value.as_i64();
            } else {
                // This is the pair data array
                let raw: Vec<KrakenKlineRaw> = serde_json::from_value(value.clone())
                    .map_err(|e| {
                        DataError::Socket(format!(
                            "failed to deserialize Kraken OHLC data for key '{}': {}",
                            key, e
                        ))
                    })?;
                klines = Some(raw);
            }
        }

        Ok((klines.unwrap_or_default(), last))
    }
}

/// Raw kline/OHLC data returned by the Kraken REST API.
///
/// Kraken returns each kline as an 8-element array:
/// `[time, open, high, low, close, vwap, volume, count]`
///
/// - `time` is a Unix timestamp in seconds
/// - `open`, `high`, `low`, `close`, `vwap`, `volume` are string values
/// - `count` is an integer trade count
///
/// Uses a custom [`Deserialize`] implementation with a sequence visitor.
#[derive(Debug, Clone)]
pub struct KrakenKlineRaw {
    pub time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub vwap: String,
    pub volume: String,
    pub count: u64,
}

impl<'de> serde::Deserialize<'de> for KrakenKlineRaw {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct KrakenKlineVisitor;

        impl<'de> serde::de::Visitor<'de> for KrakenKlineVisitor {
            type Value = KrakenKlineRaw;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Kraken OHLC array with 8 elements")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // Kraken OHLC array layout (8 elements):
                // [0] time    (i64, seconds)
                // [1] open    (String)
                // [2] high    (String)
                // [3] low     (String)
                // [4] close   (String)
                // [5] vwap    (String)
                // [6] volume  (String)
                // [7] count   (u64)
                let time = extract_next(&mut seq, "time")?;
                let open = extract_next(&mut seq, "open")?;
                let high = extract_next(&mut seq, "high")?;
                let low = extract_next(&mut seq, "low")?;
                let close = extract_next(&mut seq, "close")?;
                let vwap = extract_next(&mut seq, "vwap")?;
                let volume = extract_next(&mut seq, "volume")?;
                let count = extract_next(&mut seq, "count")?;

                // Drain any unexpected trailing elements
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(KrakenKlineRaw {
                    time,
                    open,
                    high,
                    low,
                    close,
                    vwap,
                    volume,
                    count,
                })
            }
        }

        deserializer.deserialize_seq(KrakenKlineVisitor)
    }
}

impl TryFrom<KrakenKlineRaw> for Candle {
    type Error = String;

    fn try_from(raw: KrakenKlineRaw) -> Result<Self, Self::Error> {
        // Kraken time is in seconds; convert to millis for DateTime
        let open_time_ms = raw.time * 1000;
        let open_time = chrono::DateTime::from_timestamp_millis(open_time_ms)
            .ok_or_else(|| format!("invalid time seconds: {}", raw.time))?;

        // Kraken does not provide a close_time directly. Derive it from open_time.
        // For now, set close_time equal to open_time since we lack interval context.
        // The caller can adjust if needed.
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

        Ok(Candle {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume: None,
            trade_count: raw.count,
        })
    }
}

/// REST request to fetch OHLC data from the Kraken API.
///
/// Endpoint: `GET /0/public/OHLC`
#[derive(Debug, Clone)]
pub struct GetKrakenOhlc {
    /// Query parameters for the OHLC request.
    pub params: GetKrakenOhlcParams,
}

/// Query parameters for a Kraken OHLC REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetKrakenOhlcParams {
    /// Trading pair (e.g., "XBTUSD").
    pub pair: String,
    /// Interval in minutes (e.g., 1, 5, 15, 60).
    pub interval: u32,
    /// Optional `since` cursor — unix timestamp in seconds. Kraken returns
    /// data since this time.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<i64>,
}

impl RestRequest for GetKrakenOhlc {
    type Response = KrakenOhlcResponse;
    type QueryParams = GetKrakenOhlcParams;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/0/public/OHLC")
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
    fn test_kraken_interval_supported() {
        assert_eq!(kraken_interval(Interval::M1).unwrap(), 1);
        assert_eq!(kraken_interval(Interval::M5).unwrap(), 5);
        assert_eq!(kraken_interval(Interval::M15).unwrap(), 15);
        assert_eq!(kraken_interval(Interval::M30).unwrap(), 30);
        assert_eq!(kraken_interval(Interval::H1).unwrap(), 60);
        assert_eq!(kraken_interval(Interval::H4).unwrap(), 240);
        assert_eq!(kraken_interval(Interval::D1).unwrap(), 1440);
        assert_eq!(kraken_interval(Interval::W1).unwrap(), 10080);
    }

    #[test]
    fn test_kraken_interval_unsupported() {
        assert!(kraken_interval(Interval::M3).is_err());
        assert!(kraken_interval(Interval::H2).is_err());
        assert!(kraken_interval(Interval::H6).is_err());
        assert!(kraken_interval(Interval::H12).is_err());
        assert!(kraken_interval(Interval::D3).is_err());
        assert!(kraken_interval(Interval::Month1).is_err());
    }

    #[test]
    fn test_deserialize_kraken_kline_raw() {
        let json = r#"[1672502400, "16800.00", "16900.50", "16750.00", "16850.00", "16825.30", "1234.56", 5000]"#;

        let raw: KrakenKlineRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.time, 1672502400);
        assert_eq!(raw.open, "16800.00");
        assert_eq!(raw.high, "16900.50");
        assert_eq!(raw.low, "16750.00");
        assert_eq!(raw.close, "16850.00");
        assert_eq!(raw.vwap, "16825.30");
        assert_eq!(raw.volume, "1234.56");
        assert_eq!(raw.count, 5000);
    }

    #[test]
    fn test_deserialize_kraken_ohlc_response() {
        let json = r#"{
            "error": [],
            "result": {
                "XXBTZUSD": [
                    [1672502400, "16800.00", "16900.50", "16750.00", "16850.00", "16825.30", "1234.56", 5000],
                    [1672506000, "16850.00", "16950.00", "16800.00", "16900.00", "16875.00", "987.65", 3000]
                ],
                "last": 1672588800
            }
        }"#;

        let response: KrakenOhlcResponse = serde_json::from_str(json).unwrap();
        assert!(response.error.is_empty());

        let (klines, last) = response.parse_klines().unwrap();
        assert_eq!(klines.len(), 2);
        assert_eq!(klines[0].time, 1672502400);
        assert_eq!(klines[1].time, 1672506000);
        assert_eq!(last, Some(1672588800));
    }

    #[test]
    fn test_deserialize_kraken_ohlc_response_with_errors() {
        let json = r#"{
            "error": ["EGeneral:Invalid arguments"],
            "result": {}
        }"#;

        let response: KrakenOhlcResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error.len(), 1);
        assert_eq!(response.error[0], "EGeneral:Invalid arguments");

        let (klines, last) = response.parse_klines().unwrap();
        assert!(klines.is_empty());
        assert!(last.is_none());
    }

    #[test]
    fn test_try_from_kraken_kline_raw_for_candle() {
        let raw = KrakenKlineRaw {
            time: 1672502400,
            open: "16800.00".to_string(),
            high: "16900.50".to_string(),
            low: "16750.00".to_string(),
            close: "16850.00".to_string(),
            vwap: "16825.30".to_string(),
            volume: "1234.56".to_string(),
            count: 5000,
        };

        let candle = Candle::try_from(raw).unwrap();

        assert_eq!(
            candle.open_time,
            chrono::DateTime::from_timestamp_millis(1672502400 * 1000).unwrap()
        );
        assert_eq!(candle.close_time, candle.open_time);
        assert!((candle.open - 16800.0).abs() < 1e-10);
        assert!((candle.high - 16900.50).abs() < 1e-10);
        assert!((candle.low - 16750.0).abs() < 1e-10);
        assert!((candle.close - 16850.0).abs() < 1e-10);
        assert!((candle.volume - 1234.56).abs() < 1e-10);
        assert!(candle.quote_volume.is_none());
        assert_eq!(candle.trade_count, 5000);
    }

    #[test]
    fn test_deserialize_kraken_kline_vec() {
        let json = r#"[
            [1672502400, "16800.00", "16900.50", "16750.00", "16850.00", "16825.30", "1234.56", 5000],
            [1672506000, "16850.00", "16950.00", "16800.00", "16900.00", "16875.00", "987.65", 3000]
        ]"#;

        let klines: Vec<KrakenKlineRaw> = serde_json::from_str(json).unwrap();
        assert_eq!(klines.len(), 2);
        assert_eq!(klines[0].time, 1672502400);
        assert_eq!(klines[1].time, 1672506000);
    }

    #[test]
    fn test_parse_klines_dynamic_pair_key() {
        // The response pair key may differ from the request pair
        let json = r#"{
            "error": [],
            "result": {
                "XETHZUSD": [
                    [1672502400, "1200.00", "1250.00", "1180.00", "1220.00", "1215.00", "500.00", 1000]
                ],
                "last": 1672506000
            }
        }"#;

        let response: KrakenOhlcResponse = serde_json::from_str(json).unwrap();
        let (klines, last) = response.parse_klines().unwrap();
        assert_eq!(klines.len(), 1);
        assert_eq!(klines[0].open, "1200.00");
        assert_eq!(last, Some(1672506000));
    }

    #[test]
    fn test_get_kraken_ohlc_request_path() {
        let request = GetKrakenOhlc {
            params: GetKrakenOhlcParams {
                pair: "XBTUSD".to_string(),
                interval: 60,
                since: None,
            },
        };

        assert_eq!(request.path().as_ref(), "/0/public/OHLC");
        assert_eq!(GetKrakenOhlc::method(), reqwest::Method::GET);
        assert!(request.query_params().is_some());
    }
}
