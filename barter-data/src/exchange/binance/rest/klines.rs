use crate::subscription::candle::Candle;
use barter_integration::{de::extract_next, protocol::http::rest::RestRequest};
use chrono::DateTime;
use serde::Serialize;
use std::borrow::Cow;

pub use crate::exchange::binance::binance_interval;

/// REST request to fetch kline/candlestick data from a Binance API variant.
///
/// The `path` field stores the endpoint path, which differs between spot
/// (`/api/v3/klines`) and futures (`/fapi/v1/klines`).
#[derive(Debug, Clone)]
pub struct GetKlines {
    /// Endpoint path (varies by server variant, e.g. spot vs futures).
    pub path: &'static str,
    /// Query parameters for the klines request.
    pub params: GetKlinesParams,
}

/// Query parameters for a Binance klines REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetKlinesParams {
    pub symbol: String,
    pub interval: String,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetKlines {
    type Response = Vec<BinanceKlineRaw>;
    type QueryParams = GetKlinesParams;
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

/// Raw kline/candlestick data returned by the Binance REST API.
///
/// Binance returns klines as arrays of mixed types:
/// `[open_time, open, high, low, close, volume, close_time, quote_volume, trade_count, ...]`
///
/// This struct uses a custom [`Deserialize`] implementation with a sequence
/// visitor to parse each positional element.
#[derive(Debug, Clone)]
pub struct BinanceKlineRaw {
    pub open_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: i64,
    pub quote_volume: String,
    pub trade_count: u64,
}

impl<'de> serde::Deserialize<'de> for BinanceKlineRaw {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct BinanceKlineVisitor;

        impl<'de> serde::de::Visitor<'de> for BinanceKlineVisitor {
            type Value = BinanceKlineRaw;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Binance kline array with at least 9 elements")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // Binance kline array layout (12 elements):
                // [0]  open_time      (i64 ms)
                // [1]  open           (String)
                // [2]  high           (String)
                // [3]  low            (String)
                // [4]  close          (String)
                // [5]  volume         (String)
                // [6]  close_time     (i64 ms)
                // [7]  quote_volume   (String)
                // [8]  trade_count    (u64)
                // [9]  taker_buy_base_asset_volume  (ignored)
                // [10] taker_buy_quote_asset_volume (ignored)
                // [11] unused                       (ignored)
                let open_time = extract_next(&mut seq, "open_time")?;
                let open = extract_next(&mut seq, "open")?;
                let high = extract_next(&mut seq, "high")?;
                let low = extract_next(&mut seq, "low")?;
                let close = extract_next(&mut seq, "close")?;
                let volume = extract_next(&mut seq, "volume")?;
                let close_time = extract_next(&mut seq, "close_time")?;
                let quote_volume = extract_next(&mut seq, "quote_volume")?;
                let trade_count = extract_next(&mut seq, "trade_count")?;

                // Skip remaining elements (indices 9..11)
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(BinanceKlineRaw {
                    open_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    close_time,
                    quote_volume,
                    trade_count,
                })
            }
        }

        deserializer.deserialize_seq(BinanceKlineVisitor)
    }
}

impl TryFrom<BinanceKlineRaw> for Candle {
    type Error = String;

    fn try_from(raw: BinanceKlineRaw) -> Result<Self, Self::Error> {
        let open_time = DateTime::from_timestamp_millis(raw.open_time)
            .ok_or_else(|| format!("invalid open_time millis: {}", raw.open_time))?;

        let close_time = DateTime::from_timestamp_millis(raw.close_time)
            .ok_or_else(|| format!("invalid close_time millis: {}", raw.close_time))?;

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

        let quote_volume = raw
            .quote_volume
            .parse::<f64>()
            .map_err(|e| format!("failed to parse quote_volume '{}': {}", raw.quote_volume, e))?;

        Ok(Candle {
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume,
            quote_volume: Some(quote_volume),
            trade_count: raw.trade_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;

    #[test]
    fn test_binance_interval_mapping() {
        assert_eq!(binance_interval(Interval::M1), "1m");
        assert_eq!(binance_interval(Interval::M3), "3m");
        assert_eq!(binance_interval(Interval::M5), "5m");
        assert_eq!(binance_interval(Interval::M15), "15m");
        assert_eq!(binance_interval(Interval::M30), "30m");
        assert_eq!(binance_interval(Interval::H1), "1h");
        assert_eq!(binance_interval(Interval::H2), "2h");
        assert_eq!(binance_interval(Interval::H4), "4h");
        assert_eq!(binance_interval(Interval::H6), "6h");
        assert_eq!(binance_interval(Interval::H12), "12h");
        assert_eq!(binance_interval(Interval::D1), "1d");
        assert_eq!(binance_interval(Interval::D3), "3d");
        assert_eq!(binance_interval(Interval::W1), "1w");
        assert_eq!(binance_interval(Interval::Month1), "1M");
    }

    #[test]
    fn test_deserialize_binance_kline_raw() {
        let json = r#"[
            1499040000000,
            "0.01634000",
            "0.80000000",
            "0.01575800",
            "0.01577100",
            "148976.11427815",
            1499644799999,
            "2434.19055334",
            308,
            "1.20000000",
            "3.40000000",
            "0"
        ]"#;

        let raw: BinanceKlineRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.open_time, 1499040000000);
        assert_eq!(raw.open, "0.01634000");
        assert_eq!(raw.high, "0.80000000");
        assert_eq!(raw.low, "0.01575800");
        assert_eq!(raw.close, "0.01577100");
        assert_eq!(raw.volume, "148976.11427815");
        assert_eq!(raw.close_time, 1499644799999);
        assert_eq!(raw.quote_volume, "2434.19055334");
        assert_eq!(raw.trade_count, 308);
    }

    #[test]
    fn test_try_from_binance_kline_raw_for_candle() {
        let raw = BinanceKlineRaw {
            open_time: 1499040000000,
            open: "0.01634000".to_string(),
            high: "0.80000000".to_string(),
            low: "0.01575800".to_string(),
            close: "0.01577100".to_string(),
            volume: "148976.11427815".to_string(),
            close_time: 1499644799999,
            quote_volume: "2434.19055334".to_string(),
            trade_count: 308,
        };

        let candle = Candle::try_from(raw).unwrap();

        assert_eq!(
            candle.open_time,
            DateTime::from_timestamp_millis(1499040000000).unwrap()
        );
        assert_eq!(
            candle.close_time,
            DateTime::from_timestamp_millis(1499644799999).unwrap()
        );
        assert!((candle.open - 0.01634).abs() < 1e-10);
        assert!((candle.high - 0.8).abs() < 1e-10);
        assert!((candle.low - 0.015758).abs() < 1e-10);
        assert!((candle.close - 0.015771).abs() < 1e-10);
        assert!((candle.volume - 148976.11427815).abs() < 1e-6);
        assert!((candle.quote_volume.unwrap() - 2434.19055334).abs() < 1e-6);
        assert_eq!(candle.trade_count, 308);
    }

    #[test]
    fn test_deserialize_binance_kline_vec() {
        let json = r#"[
            [1499040000000,"0.01634000","0.80000000","0.01575800","0.01577100","148976.11427815",1499644799999,"2434.19055334",308,"1.2","3.4","0"],
            [1499644800000,"0.01577100","0.01590000","0.01573000","0.01580000","100000.00000000",1500249599999,"1500.00000000",200,"0.5","1.0","0"]
        ]"#;

        let klines: Vec<BinanceKlineRaw> = serde_json::from_str(json).unwrap();
        assert_eq!(klines.len(), 2);
        assert_eq!(klines[0].open_time, 1499040000000);
        assert_eq!(klines[1].open_time, 1499644800000);
    }
}
