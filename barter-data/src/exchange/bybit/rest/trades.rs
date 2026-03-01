use crate::rest::RestTrade;
use barter_instrument::Side;
use barter_integration::protocol::http::rest::RestRequest;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Top-level response from `GET /v5/market/recent-trade`.
///
/// Bybit returns trades nested inside `result.list`:
/// ```json
/// {
///   "retCode": 0,
///   "retMsg": "OK",
///   "result": {
///     "category": "spot",
///     "list": [...]
///   }
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct BybitTradesResponse {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
    pub result: BybitTradesResult,
}

/// Inner result containing the list of trades.
#[derive(Debug, Clone, Deserialize)]
pub struct BybitTradesResult {
    pub list: Vec<BybitRestTrade>,
}

/// A single trade from the Bybit REST API.
///
/// See: <https://bybit-exchange.github.io/docs/v5/market/recent-trade>
#[derive(Debug, Clone, Deserialize)]
pub struct BybitRestTrade {
    /// Execution ID.
    #[serde(rename = "execId")]
    pub exec_id: String,
    /// Trade price as a string.
    pub price: String,
    /// Trade size as a string.
    pub size: String,
    /// Trade side: "Buy" or "Sell".
    pub side: String,
    /// Timestamp in milliseconds as a string.
    pub time: String,
}

/// REST request to fetch recent trades from the Bybit API.
///
/// The `path` field stores the endpoint path (always `/v5/market/recent-trade`).
#[derive(Debug, Clone)]
pub struct GetBybitTrades {
    /// Endpoint path (e.g., "/v5/market/recent-trade").
    pub path: &'static str,
    /// Query parameters for the trades request.
    pub params: GetBybitTradesParams,
}

/// Query parameters for a Bybit recent trades REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetBybitTradesParams {
    /// Market category: "spot" or "linear".
    pub category: String,
    /// Trading pair symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Max trades per request.
    /// Note: Bybit spot max is 60, linear/inverse max is 1000.
    /// If a larger limit is requested, the API silently clamps it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetBybitTrades {
    type Response = BybitTradesResponse;
    type QueryParams = GetBybitTradesParams;
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

impl TryFrom<BybitRestTrade> for RestTrade {
    type Error = String;

    fn try_from(raw: BybitRestTrade) -> Result<Self, Self::Error> {
        let time_ms: i64 = raw
            .time
            .parse()
            .map_err(|e| format!("failed to parse time as i64 '{}': {}", raw.time, e))?;

        let time = DateTime::from_timestamp_millis(time_ms)
            .ok_or_else(|| format!("invalid timestamp millis: {}", time_ms))?;

        let price = raw
            .price
            .parse::<f64>()
            .map_err(|e| format!("failed to parse price '{}': {}", raw.price, e))?;

        let amount = raw
            .size
            .parse::<f64>()
            .map_err(|e| format!("failed to parse size '{}': {}", raw.size, e))?;

        let side = match raw.side.as_str() {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            other => return Err(format!("unknown trade side: '{}'", other)),
        };

        Ok(RestTrade {
            id: raw.exec_id,
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
    fn test_deserialize_bybit_trades_response() {
        let json = r#"{
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "category": "spot",
                "list": [
                    {
                        "execId": "2100000000007764455",
                        "symbol": "BTCUSDT",
                        "price": "26260.98",
                        "size": "0.003425",
                        "side": "Sell",
                        "time": "1679907065209",
                        "isBlockTrade": false
                    },
                    {
                        "execId": "2100000000007764456",
                        "symbol": "BTCUSDT",
                        "price": "26261.50",
                        "size": "0.001000",
                        "side": "Buy",
                        "time": "1679907065300",
                        "isBlockTrade": false
                    }
                ]
            }
        }"#;

        let response: BybitTradesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.ret_code, 0);
        assert_eq!(response.ret_msg, "OK");
        assert_eq!(response.result.list.len(), 2);
        assert_eq!(response.result.list[0].exec_id, "2100000000007764455");
        assert_eq!(response.result.list[0].price, "26260.98");
        assert_eq!(response.result.list[0].size, "0.003425");
        assert_eq!(response.result.list[0].side, "Sell");
        assert_eq!(response.result.list[0].time, "1679907065209");
        assert_eq!(response.result.list[1].exec_id, "2100000000007764456");
        assert_eq!(response.result.list[1].side, "Buy");
    }

    #[test]
    fn test_try_from_bybit_rest_trade_for_rest_trade() {
        // Test Sell trade
        let raw_sell = BybitRestTrade {
            exec_id: "2100000000007764455".to_string(),
            price: "26260.98".to_string(),
            size: "0.003425".to_string(),
            side: "Sell".to_string(),
            time: "1679907065209".to_string(),
        };

        let trade = RestTrade::try_from(raw_sell).unwrap();

        assert_eq!(trade.id, "2100000000007764455");
        assert_eq!(
            trade.time,
            DateTime::from_timestamp_millis(1679907065209).unwrap()
        );
        assert!((trade.price - 26260.98).abs() < 1e-10);
        assert!((trade.amount - 0.003425).abs() < 1e-10);
        assert_eq!(trade.side, Side::Sell);

        // Test Buy trade
        let raw_buy = BybitRestTrade {
            exec_id: "2100000000007764456".to_string(),
            price: "26261.50".to_string(),
            size: "0.001000".to_string(),
            side: "Buy".to_string(),
            time: "1679907065300".to_string(),
        };

        let trade_buy = RestTrade::try_from(raw_buy).unwrap();

        assert_eq!(trade_buy.id, "2100000000007764456");
        assert_eq!(trade_buy.side, Side::Buy);
    }

    #[test]
    fn test_try_from_bybit_rest_trade_invalid_time() {
        let raw = BybitRestTrade {
            exec_id: "1".to_string(),
            price: "26260.98".to_string(),
            size: "0.003425".to_string(),
            side: "Buy".to_string(),
            time: "not_a_number".to_string(),
        };
        assert!(RestTrade::try_from(raw).is_err());
    }

    #[test]
    fn test_try_from_bybit_rest_trade_invalid_price() {
        let raw = BybitRestTrade {
            exec_id: "1".to_string(),
            price: "bad".to_string(),
            size: "0.003425".to_string(),
            side: "Buy".to_string(),
            time: "1679907065209".to_string(),
        };
        assert!(RestTrade::try_from(raw).is_err());
    }

    #[test]
    fn test_try_from_bybit_rest_trade_invalid_side() {
        let raw = BybitRestTrade {
            exec_id: "1".to_string(),
            price: "26260.98".to_string(),
            size: "0.003425".to_string(),
            side: "UNKNOWN".to_string(),
            time: "1679907065209".to_string(),
        };
        assert!(RestTrade::try_from(raw).is_err());
    }
}
