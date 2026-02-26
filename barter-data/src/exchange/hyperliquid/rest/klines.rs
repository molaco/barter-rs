use crate::subscription::candle::Candle;
use barter_integration::protocol::http::rest::RestRequest;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

pub use crate::exchange::hyperliquid::hyperliquid_interval;

/// Hyperliquid REST API path for the info endpoint.
///
/// All public data queries use POST to `/info` with a JSON body.
pub const HYPERLIQUID_INFO_PATH: &str = "/info";

/// Raw kline/candlestick data returned by the Hyperliquid REST API.
///
/// Hyperliquid returns klines as JSON objects with short field names:
/// ```json
/// {"t":1672531200000,"T":1672531259999,"s":"BTC","i":"1m","o":"16500.0","c":"16550.0","h":"16600.0","l":"16400.0","v":"1234.56","n":42}
/// ```
///
/// We only deserialize the fields we need; unknown fields are ignored via
/// serde's default behaviour.
#[derive(Debug, Clone, Deserialize)]
pub struct HyperliquidKlineRaw {
    /// Open time in milliseconds since epoch.
    pub t: u64,
    /// Close time in milliseconds since epoch.
    #[serde(rename = "T")]
    pub close_time: u64,
    /// Open price (string).
    pub o: String,
    /// High price (string).
    pub h: String,
    /// Low price (string).
    pub l: String,
    /// Close price (string).
    pub c: String,
    /// Volume (string).
    pub v: String,
}

impl HyperliquidKlineRaw {
    /// Convert this raw kline into a normalised [`Candle`].
    ///
    /// Hyperliquid provides both open and close timestamps, so no interval
    /// duration computation is needed.
    pub fn try_into_candle(self) -> Result<Candle, String> {
        let open_time = DateTime::from_timestamp_millis(self.t as i64)
            .ok_or_else(|| format!("invalid open time millis: {}", self.t))?;

        let close_time = DateTime::from_timestamp_millis(self.close_time as i64)
            .ok_or_else(|| format!("invalid close time millis: {}", self.close_time))?;

        let open = self
            .o
            .parse::<f64>()
            .map_err(|e| format!("failed to parse open '{}': {}", self.o, e))?;

        let high = self
            .h
            .parse::<f64>()
            .map_err(|e| format!("failed to parse high '{}': {}", self.h, e))?;

        let low = self
            .l
            .parse::<f64>()
            .map_err(|e| format!("failed to parse low '{}': {}", self.l, e))?;

        let close = self
            .c
            .parse::<f64>()
            .map_err(|e| format!("failed to parse close '{}': {}", self.c, e))?;

        let volume = self
            .v
            .parse::<f64>()
            .map_err(|e| format!("failed to parse volume '{}': {}", self.v, e))?;

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

/// JSON body for the Hyperliquid candleSnapshot request.
///
/// Hyperliquid uses POST requests to `/info` with a JSON body like:
/// ```json
/// {
///     "type": "candleSnapshot",
///     "req": {
///         "coin": "BTC",
///         "interval": "1h",
///         "startTime": 1672531200000,
///         "endTime": 1672617600000
///     }
/// }
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct PostHyperliquidKlines {
    /// Request type, always `"candleSnapshot"`.
    #[serde(rename = "type")]
    pub request_type: &'static str,
    /// The inner request parameters.
    pub req: HyperliquidKlineReq,
}

/// Inner request parameters for the Hyperliquid candleSnapshot query.
#[derive(Debug, Clone, Serialize)]
pub struct HyperliquidKlineReq {
    /// Coin symbol (e.g., "BTC", "ETH").
    pub coin: String,
    /// Candle interval (e.g., "1h", "4h", "1d").
    pub interval: String,
    /// Start time in milliseconds since epoch.
    #[serde(rename = "startTime")]
    pub start_time: i64,
    /// End time in milliseconds since epoch.
    #[serde(rename = "endTime")]
    pub end_time: i64,
}

/// REST request wrapper for the Hyperliquid candleSnapshot endpoint.
///
/// This implements [`RestRequest`] with `Method::POST` and a JSON body,
/// unlike most other exchange clients which use GET with query parameters.
#[derive(Debug, Clone)]
pub struct GetHyperliquidKlines {
    /// The JSON body payload.
    pub body: PostHyperliquidKlines,
}

impl RestRequest for GetHyperliquidKlines {
    type Response = Vec<HyperliquidKlineRaw>;
    type QueryParams = ();
    type Body = PostHyperliquidKlines;

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(HYPERLIQUID_INFO_PATH)
    }

    fn method() -> reqwest::Method {
        reqwest::Method::POST
    }

    fn body(&self) -> Option<&Self::Body> {
        Some(&self.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;

    #[test]
    fn test_hyperliquid_interval_mapping() {
        assert_eq!(hyperliquid_interval(Interval::M1).unwrap(), "1m");
        assert_eq!(hyperliquid_interval(Interval::M3).unwrap(), "3m");
        assert_eq!(hyperliquid_interval(Interval::M5).unwrap(), "5m");
        assert_eq!(hyperliquid_interval(Interval::M15).unwrap(), "15m");
        assert_eq!(hyperliquid_interval(Interval::M30).unwrap(), "30m");
        assert_eq!(hyperliquid_interval(Interval::H1).unwrap(), "1h");
        assert_eq!(hyperliquid_interval(Interval::H2).unwrap(), "2h");
        assert_eq!(hyperliquid_interval(Interval::H4).unwrap(), "4h");
        assert_eq!(hyperliquid_interval(Interval::H6).unwrap(), "6h");
        assert_eq!(hyperliquid_interval(Interval::H12).unwrap(), "12h");
        assert_eq!(hyperliquid_interval(Interval::D1).unwrap(), "1d");
        assert_eq!(hyperliquid_interval(Interval::D3).unwrap(), "3d");
        assert_eq!(hyperliquid_interval(Interval::W1).unwrap(), "1w");
        assert_eq!(hyperliquid_interval(Interval::Month1).unwrap(), "1M");
    }

    #[test]
    fn test_deserialize_hyperliquid_kline_raw() {
        let json = r#"{
            "t": 1672531200000,
            "T": 1672531259999,
            "s": "BTC",
            "i": "1m",
            "o": "16500.0",
            "c": "16550.0",
            "h": "16600.0",
            "l": "16400.0",
            "v": "1234.56",
            "n": 42
        }"#;

        let raw: HyperliquidKlineRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.t, 1672531200000);
        assert_eq!(raw.close_time, 1672531259999);
        assert_eq!(raw.o, "16500.0");
        assert_eq!(raw.h, "16600.0");
        assert_eq!(raw.l, "16400.0");
        assert_eq!(raw.c, "16550.0");
        assert_eq!(raw.v, "1234.56");
    }

    #[test]
    fn test_deserialize_hyperliquid_kline_vec() {
        let json = r#"[
            {"t":1672531200000,"T":1672531259999,"s":"BTC","i":"1m","o":"16500.0","c":"16550.0","h":"16600.0","l":"16400.0","v":"1234.56","n":42},
            {"t":1672531260000,"T":1672531319999,"s":"BTC","i":"1m","o":"16550.0","c":"16580.0","h":"16620.0","l":"16530.0","v":"987.65","n":31}
        ]"#;

        let klines: Vec<HyperliquidKlineRaw> = serde_json::from_str(json).unwrap();
        assert_eq!(klines.len(), 2);
        assert_eq!(klines[0].t, 1672531200000);
        assert_eq!(klines[1].t, 1672531260000);
    }

    #[test]
    fn test_try_into_candle() {
        let raw = HyperliquidKlineRaw {
            t: 1672531200000,
            close_time: 1672531259999,
            o: "16500.0".to_string(),
            h: "16600.0".to_string(),
            l: "16400.0".to_string(),
            c: "16550.0".to_string(),
            v: "1234.56".to_string(),
        };

        let candle = raw.try_into_candle().unwrap();

        assert_eq!(
            candle.open_time,
            DateTime::from_timestamp_millis(1672531200000).unwrap()
        );
        assert_eq!(
            candle.close_time,
            DateTime::from_timestamp_millis(1672531259999).unwrap()
        );
        assert!((candle.open - 16500.0).abs() < 1e-10);
        assert!((candle.high - 16600.0).abs() < 1e-10);
        assert!((candle.low - 16400.0).abs() < 1e-10);
        assert!((candle.close - 16550.0).abs() < 1e-10);
        assert!((candle.volume - 1234.56).abs() < 1e-6);
        assert_eq!(candle.quote_volume, None);
        assert_eq!(candle.trade_count, 0);
        assert!(candle.is_closed);
    }

    #[test]
    fn test_try_into_candle_invalid_price() {
        let raw = HyperliquidKlineRaw {
            t: 1672531200000,
            close_time: 1672531259999,
            o: "bad_price".to_string(),
            h: "16600.0".to_string(),
            l: "16400.0".to_string(),
            c: "16550.0".to_string(),
            v: "1234.56".to_string(),
        };

        let result = raw.try_into_candle();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse open"));
    }

    #[test]
    fn test_serialize_post_body() {
        let body = PostHyperliquidKlines {
            request_type: "candleSnapshot",
            req: HyperliquidKlineReq {
                coin: "BTC".to_string(),
                interval: "1h".to_string(),
                start_time: 1672531200000,
                end_time: 1672617600000,
            },
        };

        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(json["type"], "candleSnapshot");
        assert_eq!(json["req"]["coin"], "BTC");
        assert_eq!(json["req"]["interval"], "1h");
        assert_eq!(json["req"]["startTime"], 1672531200000_i64);
        assert_eq!(json["req"]["endTime"], 1672617600000_i64);
    }

    #[test]
    fn test_get_hyperliquid_klines_request() {
        let request = GetHyperliquidKlines {
            body: PostHyperliquidKlines {
                request_type: "candleSnapshot",
                req: HyperliquidKlineReq {
                    coin: "BTC".to_string(),
                    interval: "1h".to_string(),
                    start_time: 1672531200000,
                    end_time: 1672617600000,
                },
            },
        };

        assert_eq!(request.path(), Cow::<str>::Borrowed("/info"));
        assert_eq!(GetHyperliquidKlines::method(), reqwest::Method::POST);
        assert!(request.body().is_some());
        assert!(request.query_params().is_none());
    }
}
