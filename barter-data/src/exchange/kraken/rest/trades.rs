use crate::{error::DataError, rest::RestTrade};
use barter_instrument::Side;
use barter_integration::{de::extract_next, protocol::http::rest::RestRequest};
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Top-level Kraken Trades response.
///
/// Same dynamic-key pattern as klines: the `result` object contains
/// a key for the pair data and a `last` cursor for pagination.
///
/// ```json
/// {
///   "error": [],
///   "result": {
///     "XXBTZUSD": [[...], ...],
///     "last": "1679907066543200000"
///   }
/// }
/// ```
///
/// The `result` object contains a dynamic key for the pair data and a `last`
/// cursor (nanosecond nonce string) for forward pagination. We deserialize
/// `result` as a raw [`serde_json::Value`] and parse it manually.
#[derive(Debug, Deserialize)]
pub struct KrakenTradesResponse {
    pub error: Vec<String>,
    pub result: serde_json::Value,
}

impl KrakenTradesResponse {
    /// Parse the raw `result` into a vector of [`KrakenTradeRaw`] and
    /// an optional `last` pagination cursor (nonce string).
    ///
    /// Iterates the keys in `result`, treating `"last"` as the cursor and
    /// the remaining key as the trade data array. The pair key in the response
    /// may differ from the requested pair (e.g., `"XXBTZUSD"` vs `"XBTUSD"`).
    pub fn parse_trades(&self) -> Result<(Vec<KrakenTradeRaw>, Option<String>), DataError> {
        let obj = self.result.as_object().ok_or_else(|| {
            DataError::DataParse("Kraken Trades result is not an object".to_string())
        })?;

        let mut last: Option<String> = None;
        let mut trades: Option<Vec<KrakenTradeRaw>> = None;

        for (key, value) in obj {
            if key == "last" {
                last = value.as_str().map(|s| s.to_string());
            } else {
                // This is the pair data array
                let raw: Vec<KrakenTradeRaw> =
                    serde_json::from_value(value.clone()).map_err(|e| {
                        DataError::DataParse(format!(
                            "failed to deserialize Kraken trades for key '{}': {}",
                            key, e
                        ))
                    })?;
                trades = Some(raw);
            }
        }

        Ok((trades.unwrap_or_default(), last))
    }
}

/// Raw trade from Kraken REST API.
///
/// Kraken returns each trade as a 7-element positional array:
/// `[price, volume, time, buy_sell, market_limit, misc, trade_id]`
///
/// - `price`: String
/// - `volume`: String
/// - `time`: float (Unix timestamp with fractional seconds)
/// - `buy_sell`: "b" for buy, "s" for sell
/// - `market_limit`: "m" or "l" (ignored)
/// - `misc`: String (ignored)
/// - `trade_id`: integer (unquoted)
///
/// Uses a custom [`Deserialize`] implementation with a sequence visitor.
#[derive(Debug, Clone)]
pub struct KrakenTradeRaw {
    pub price: String,
    pub volume: String,
    /// Unix timestamp with fractional seconds (e.g., 1679907065.2091).
    pub time: f64,
    /// "b" for buy, "s" for sell.
    pub buy_sell: String,
    /// Trade ID as integer.
    pub trade_id: u64,
}

impl<'de> serde::Deserialize<'de> for KrakenTradeRaw {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        struct KrakenTradeVisitor;

        impl<'de> serde::de::Visitor<'de> for KrakenTradeVisitor {
            type Value = KrakenTradeRaw;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a Kraken trade array with 7 elements")
            }

            fn visit_seq<SeqAccessor>(
                self,
                mut seq: SeqAccessor,
            ) -> Result<Self::Value, SeqAccessor::Error>
            where
                SeqAccessor: serde::de::SeqAccess<'de>,
            {
                // Kraken trade array layout (7 elements):
                // [0] price        (String)
                // [1] volume       (String)
                // [2] time         (f64, seconds with fractional)
                // [3] buy_sell     (String, "b" or "s")
                // [4] market_limit (String, "m" or "l" — ignored)
                // [5] misc         (String — ignored)
                // [6] trade_id     (u64)
                let price = extract_next(&mut seq, "price")?;
                let volume = extract_next(&mut seq, "volume")?;
                let time = extract_next(&mut seq, "time")?;
                let buy_sell = extract_next(&mut seq, "buy_sell")?;
                let _market_limit: String = extract_next(&mut seq, "market_limit")?;
                let _misc: String = extract_next(&mut seq, "misc")?;
                let trade_id = extract_next(&mut seq, "trade_id")?;

                // Drain any unexpected trailing elements
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(KrakenTradeRaw {
                    price,
                    volume,
                    time,
                    buy_sell,
                    trade_id,
                })
            }
        }

        deserializer.deserialize_seq(KrakenTradeVisitor)
    }
}

impl TryFrom<KrakenTradeRaw> for RestTrade {
    type Error = String;

    fn try_from(raw: KrakenTradeRaw) -> Result<Self, Self::Error> {
        // Kraken time is a float in seconds; convert to millis for DateTime
        let time_ms = (raw.time * 1000.0).round() as i64;
        let time = DateTime::from_timestamp_millis(time_ms)
            .ok_or_else(|| format!("invalid time seconds: {}", raw.time))?;

        let price = raw
            .price
            .parse::<f64>()
            .map_err(|e| format!("failed to parse price '{}': {}", raw.price, e))?;

        let amount = raw
            .volume
            .parse::<f64>()
            .map_err(|e| format!("failed to parse volume '{}': {}", raw.volume, e))?;

        let side = match raw.buy_sell.as_str() {
            "b" => Side::Buy,
            "s" => Side::Sell,
            other => return Err(format!("unknown buy_sell value: '{}'", other)),
        };

        Ok(RestTrade {
            id: raw.trade_id.to_string(),
            time,
            price,
            amount,
            side,
        })
    }
}

/// REST request to fetch trades from the Kraken API.
///
/// Endpoint: `GET /0/public/Trades`
#[derive(Debug, Clone)]
pub struct GetKrakenTrades {
    /// Query parameters for the trades request.
    pub params: GetKrakenTradesParams,
}

/// Query parameters for a Kraken Trades REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetKrakenTradesParams {
    /// Trading pair (e.g., "XBTUSD").
    pub pair: String,
    /// Optional `since` cursor — nanosecond nonce string from previous
    /// response's `last`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
    /// Optional count (max 1000).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<u32>,
}

impl RestRequest for GetKrakenTrades {
    type Response = KrakenTradesResponse;
    type QueryParams = GetKrakenTradesParams;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed("/0/public/Trades")
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
    fn test_deserialize_kraken_trade_raw() {
        let json = r#"["26260.98000", "0.00342500", 1679907065.2091, "b", "m", "", 391120170]"#;

        let raw: KrakenTradeRaw = serde_json::from_str(json).unwrap();
        assert_eq!(raw.price, "26260.98000");
        assert_eq!(raw.volume, "0.00342500");
        assert!((raw.time - 1679907065.2091).abs() < 1e-4);
        assert_eq!(raw.buy_sell, "b");
        assert_eq!(raw.trade_id, 391120170);
    }

    #[test]
    fn test_try_from_kraken_trade_raw_for_rest_trade() {
        let raw = KrakenTradeRaw {
            price: "26260.98000".to_string(),
            volume: "0.00342500".to_string(),
            time: 1679907065.2091,
            buy_sell: "b".to_string(),
            trade_id: 391120170,
        };

        let trade = RestTrade::try_from(raw).unwrap();

        assert_eq!(trade.id, "391120170");
        // 1679907065.2091 * 1000 = 1679907065209.1 → rounded to 1679907065209
        assert_eq!(
            trade.time,
            DateTime::from_timestamp_millis(1679907065209).unwrap()
        );
        assert!((trade.price - 26260.98).abs() < 1e-2);
        assert!((trade.amount - 0.003425).abs() < 1e-8);
        assert_eq!(trade.side, Side::Buy);

        // Test sell trade
        let raw_sell = KrakenTradeRaw {
            price: "26261.50000".to_string(),
            volume: "0.01000000".to_string(),
            time: 1679907066.5432,
            buy_sell: "s".to_string(),
            trade_id: 391120171,
        };

        let trade_sell = RestTrade::try_from(raw_sell).unwrap();
        assert_eq!(trade_sell.side, Side::Sell);

        // Test invalid side
        let raw_invalid = KrakenTradeRaw {
            price: "100.0".to_string(),
            volume: "1.0".to_string(),
            time: 1679907066.0,
            buy_sell: "x".to_string(),
            trade_id: 1,
        };

        assert!(RestTrade::try_from(raw_invalid).is_err());
    }

    #[test]
    fn test_deserialize_kraken_trades_response() {
        let json = r#"{
            "error": [],
            "result": {
                "XXBTZUSD": [
                    ["26260.98000", "0.00342500", 1679907065.2091, "b", "m", "", 391120170],
                    ["26261.50000", "0.01000000", 1679907066.5432, "s", "l", "", 391120171]
                ],
                "last": "1679907066543200000"
            }
        }"#;

        let response: KrakenTradesResponse = serde_json::from_str(json).unwrap();
        assert!(response.error.is_empty());

        let (trades, last) = response.parse_trades().unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].price, "26260.98000");
        assert_eq!(trades[0].buy_sell, "b");
        assert_eq!(trades[0].trade_id, 391120170);
        assert_eq!(trades[1].price, "26261.50000");
        assert_eq!(trades[1].buy_sell, "s");
        assert_eq!(trades[1].trade_id, 391120171);
        assert_eq!(last, Some("1679907066543200000".to_string()));
    }

    #[test]
    fn test_parse_trades_dynamic_pair_key() {
        // The response pair key may differ from the request pair
        let json = r#"{
            "error": [],
            "result": {
                "XETHZUSD": [
                    ["1200.50000", "2.50000000", 1679900000.1234, "s", "l", "", 500000001]
                ],
                "last": "1679900000123400000"
            }
        }"#;

        let response: KrakenTradesResponse = serde_json::from_str(json).unwrap();
        let (trades, last) = response.parse_trades().unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].price, "1200.50000");
        assert_eq!(trades[0].buy_sell, "s");
        assert_eq!(trades[0].trade_id, 500000001);
        assert_eq!(last, Some("1679900000123400000".to_string()));
    }

    #[test]
    fn test_deserialize_kraken_trades_response_with_errors() {
        let json = r#"{
            "error": ["EGeneral:Invalid arguments"],
            "result": {}
        }"#;

        let response: KrakenTradesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.error.len(), 1);
        assert_eq!(response.error[0], "EGeneral:Invalid arguments");

        let (trades, last) = response.parse_trades().unwrap();
        assert!(trades.is_empty());
        assert!(last.is_none());
    }

    #[test]
    fn test_get_kraken_trades_request_path() {
        let request = GetKrakenTrades {
            params: GetKrakenTradesParams {
                pair: "XBTUSD".to_string(),
                since: None,
                count: None,
            },
        };

        assert_eq!(request.path().as_ref(), "/0/public/Trades");
        assert_eq!(GetKrakenTrades::method(), reqwest::Method::GET);
        assert!(request.query_params().is_some());
    }

    #[test]
    fn test_deserialize_kraken_trade_raw_vec() {
        let json = r#"[
            ["26260.98000", "0.00342500", 1679907065.2091, "b", "m", "", 391120170],
            ["26261.50000", "0.01000000", 1679907066.5432, "s", "l", "", 391120171]
        ]"#;

        let trades: Vec<KrakenTradeRaw> = serde_json::from_str(json).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].trade_id, 391120170);
        assert_eq!(trades[1].trade_id, 391120171);
    }

    #[test]
    fn test_try_from_kraken_trade_raw_invalid_price() {
        let raw = KrakenTradeRaw {
            price: "not_a_number".to_string(),
            volume: "1.0".to_string(),
            time: 1672502400.0,
            buy_sell: "b".to_string(),
            trade_id: 12345,
        };
        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse price"));
    }

    #[test]
    fn test_try_from_kraken_trade_raw_invalid_volume() {
        let raw = KrakenTradeRaw {
            price: "16800.00".to_string(),
            volume: "bad".to_string(),
            time: 1672502400.0,
            buy_sell: "b".to_string(),
            trade_id: 12345,
        };
        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse volume"));
    }
}
