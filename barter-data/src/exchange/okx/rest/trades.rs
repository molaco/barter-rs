use crate::rest::RestTrade;
use barter_instrument::Side;
use barter_integration::protocol::http::rest::RestRequest;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// OKX REST API endpoint for historical trades.
pub const OKX_HISTORY_TRADES_PATH: &str = "/api/v5/market/history-trades";

/// Top-level response from OKX history-trades endpoint.
///
/// OKX returns:
/// ```json
/// {
///   "code": "0",
///   "msg": "",
///   "data": [...]
/// }
/// ```
///
/// A `code` of `"0"` indicates success. Any other value is an error.
#[derive(Debug, Clone, Deserialize)]
pub struct OkxTradesResponse {
    pub code: String,
    pub msg: String,
    pub data: Vec<OkxRestTrade>,
}

/// A single trade from the OKX REST API.
#[derive(Debug, Clone, Deserialize)]
pub struct OkxRestTrade {
    /// Trade ID (used for pagination cursor).
    #[serde(rename = "tradeId")]
    pub trade_id: String,
    /// Trade price as a string.
    #[serde(rename = "px")]
    pub price: String,
    /// Trade size as a string.
    #[serde(rename = "sz")]
    pub amount: String,
    /// Trade side: "buy" or "sell".
    pub side: String,
    /// Timestamp in milliseconds as a string.
    pub ts: String,
}

/// REST request to fetch historical trade data from the OKX API.
#[derive(Debug, Clone)]
pub struct GetOkxTrades {
    /// Query parameters for the history-trades request.
    pub params: GetOkxTradesParams,
}

/// Query parameters for an OKX history-trades REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetOkxTradesParams {
    /// Instrument ID (e.g., "BTC-USDT").
    #[serde(rename = "instId")]
    pub inst_id: String,
    /// Pagination cursor: return records with tradeId < this value (older trades).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,
    /// Pagination cursor: return records with tradeId > this value (newer trades).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<String>,
    /// Max records per request (max 100).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetOkxTrades {
    type Response = OkxTradesResponse;
    type QueryParams = GetOkxTradesParams;
    type Body = ();

    fn path(&self) -> Cow<'static, str> {
        Cow::Borrowed(OKX_HISTORY_TRADES_PATH)
    }

    fn method() -> reqwest::Method {
        reqwest::Method::GET
    }

    fn query_params(&self) -> Option<&Self::QueryParams> {
        Some(&self.params)
    }
}

impl TryFrom<OkxRestTrade> for RestTrade {
    type Error = String;

    fn try_from(raw: OkxRestTrade) -> Result<Self, Self::Error> {
        let ts_ms: i64 = raw
            .ts
            .parse()
            .map_err(|e| format!("failed to parse ts '{}': {}", raw.ts, e))?;

        let time = DateTime::from_timestamp_millis(ts_ms)
            .ok_or_else(|| format!("invalid timestamp millis: {}", ts_ms))?;

        let price = raw
            .price
            .parse::<f64>()
            .map_err(|e| format!("failed to parse price '{}': {}", raw.price, e))?;

        let amount = raw
            .amount
            .parse::<f64>()
            .map_err(|e| format!("failed to parse amount '{}': {}", raw.amount, e))?;

        let side = match raw.side.as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => return Err(format!("unknown trade side: '{}'", other)),
        };

        Ok(RestTrade {
            id: raw.trade_id,
            time,
            price,
            amount,
            side,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_okx_trades_response() {
        let json = r#"{
            "code": "0",
            "msg": "",
            "data": [
                {
                    "instId": "BTC-USDT",
                    "tradeId": "391120170",
                    "px": "26260.98",
                    "sz": "0.003425",
                    "side": "buy",
                    "ts": "1679907065209"
                },
                {
                    "instId": "BTC-USDT",
                    "tradeId": "391120171",
                    "px": "26261.50",
                    "sz": "0.001000",
                    "side": "sell",
                    "ts": "1679907065300"
                }
            ]
        }"#;

        let response: OkxTradesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.code, "0");
        assert_eq!(response.msg, "");
        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].trade_id, "391120170");
        assert_eq!(response.data[0].price, "26260.98");
        assert_eq!(response.data[0].amount, "0.003425");
        assert_eq!(response.data[0].side, "buy");
        assert_eq!(response.data[0].ts, "1679907065209");
        assert_eq!(response.data[1].trade_id, "391120171");
        assert_eq!(response.data[1].side, "sell");
    }

    #[test]
    fn test_try_from_okx_rest_trade_for_rest_trade() {
        // Test buy trade
        let raw_buy = OkxRestTrade {
            trade_id: "391120170".to_string(),
            price: "26260.98".to_string(),
            amount: "0.003425".to_string(),
            side: "buy".to_string(),
            ts: "1679907065209".to_string(),
        };

        let trade = RestTrade::try_from(raw_buy).unwrap();

        assert_eq!(trade.id, "391120170");
        assert_eq!(
            trade.time,
            DateTime::from_timestamp_millis(1679907065209).unwrap()
        );
        assert!((trade.price - 26260.98).abs() < 1e-10);
        assert!((trade.amount - 0.003425).abs() < 1e-10);
        assert_eq!(trade.side, Side::Buy);

        // Test sell trade
        let raw_sell = OkxRestTrade {
            trade_id: "391120171".to_string(),
            price: "26261.50".to_string(),
            amount: "0.001000".to_string(),
            side: "sell".to_string(),
            ts: "1679907065300".to_string(),
        };

        let trade_sell = RestTrade::try_from(raw_sell).unwrap();

        assert_eq!(trade_sell.id, "391120171");
        assert_eq!(trade_sell.side, Side::Sell);
    }

    #[test]
    fn test_try_from_okx_rest_trade_invalid_side() {
        let raw = OkxRestTrade {
            trade_id: "391120170".to_string(),
            price: "26260.98".to_string(),
            amount: "0.003425".to_string(),
            side: "unknown".to_string(),
            ts: "1679907065209".to_string(),
        };

        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown trade side"));
    }

    #[test]
    fn test_try_from_okx_rest_trade_invalid_ts() {
        let raw = OkxRestTrade {
            trade_id: "391120170".to_string(),
            price: "26260.98".to_string(),
            amount: "0.003425".to_string(),
            side: "buy".to_string(),
            ts: "not_a_number".to_string(),
        };

        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse ts"));
    }

    #[test]
    fn test_try_from_okx_rest_trade_invalid_price() {
        let raw = OkxRestTrade {
            trade_id: "391120170".to_string(),
            price: "bad_price".to_string(),
            amount: "0.003425".to_string(),
            side: "buy".to_string(),
            ts: "1679907065209".to_string(),
        };

        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse price"));
    }

    #[test]
    fn test_try_from_okx_rest_trade_invalid_amount() {
        let raw = OkxRestTrade {
            trade_id: "1".to_string(),
            price: "26260.98".to_string(),
            amount: "bad".to_string(),
            side: "buy".to_string(),
            ts: "1679907065209".to_string(),
        };
        assert!(RestTrade::try_from(raw).is_err());
    }

    #[test]
    fn test_okx_trades_error_response() {
        let json = r#"{
            "code": "51001",
            "msg": "Instrument ID does not exist",
            "data": []
        }"#;

        let response: OkxTradesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.code, "51001");
        assert_eq!(response.msg, "Instrument ID does not exist");
        assert!(response.data.is_empty());
    }
}
