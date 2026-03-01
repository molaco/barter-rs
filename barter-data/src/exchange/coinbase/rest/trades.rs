use crate::rest::RestTrade;
use barter_instrument::Side;
use barter_integration::protocol::http::rest::RestRequest;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Coinbase REST API endpoint prefix for market ticker (trades).
/// The full path includes the product ID: `/api/v3/brokerage/market/products/{product_id}/ticker`
pub const COINBASE_TICKER_PATH_PREFIX: &str = "/api/v3/brokerage/market/products";

/// Top-level response from the Coinbase ticker endpoint.
///
/// ```json
/// {
///     "trades": [...],
///     "best_bid": "26260.97",
///     "best_ask": "26261.00"
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct CoinbaseTradesResponse {
    pub trades: Vec<CoinbaseRestTrade>,
}

/// A single trade from the Coinbase REST API.
#[derive(Debug, Clone, Deserialize)]
pub struct CoinbaseRestTrade {
    /// Trade ID.
    pub trade_id: String,
    /// Trade price as a string.
    pub price: String,
    /// Trade size as a string.
    pub size: String,
    /// Trade side: "BUY" or "SELL".
    pub side: String,
    /// Timestamp in ISO 8601 format.
    pub time: String,
}

/// REST request to fetch trade data from the Coinbase API.
///
/// The `path` field is an owned [`String`] because it includes the dynamic
/// `product_id` in the URL path.
#[derive(Debug, Clone)]
pub struct GetCoinbaseTrades {
    /// Full endpoint path including product_id, e.g.
    /// `/api/v3/brokerage/market/products/BTC-USD/ticker`.
    pub path: String,
    /// Query parameters for the trades request.
    pub params: GetCoinbaseTradesParams,
}

/// Query parameters for a Coinbase trades REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetCoinbaseTradesParams {
    /// Start time as Unix timestamp in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<i64>,
    /// End time as Unix timestamp in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<i64>,
    /// Max trades per request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetCoinbaseTrades {
    type Response = CoinbaseTradesResponse;
    type QueryParams = GetCoinbaseTradesParams;
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

impl TryFrom<CoinbaseRestTrade> for RestTrade {
    type Error = String;

    fn try_from(raw: CoinbaseRestTrade) -> Result<Self, Self::Error> {
        let time = DateTime::parse_from_rfc3339(&raw.time)
            .map_err(|e| format!("failed to parse time '{}': {}", raw.time, e))?
            .with_timezone(&Utc);

        let price = raw
            .price
            .parse::<f64>()
            .map_err(|e| format!("failed to parse price '{}': {}", raw.price, e))?;

        let amount = raw
            .size
            .parse::<f64>()
            .map_err(|e| format!("failed to parse size '{}': {}", raw.size, e))?;

        let side = match raw.side.as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
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
    fn test_deserialize_coinbase_trades_response() {
        let json = r#"{
            "trades": [
                {
                    "trade_id": "34964583",
                    "product_id": "BTC-USD",
                    "price": "26260.98",
                    "size": "0.003425",
                    "time": "2023-03-27T10:51:05.209Z",
                    "side": "BUY",
                    "bid": "",
                    "ask": ""
                },
                {
                    "trade_id": "34964584",
                    "product_id": "BTC-USD",
                    "price": "26261.50",
                    "size": "0.001000",
                    "time": "2023-03-27T10:51:06.100Z",
                    "side": "SELL",
                    "bid": "",
                    "ask": ""
                }
            ],
            "best_bid": "26260.97",
            "best_ask": "26261.00"
        }"#;

        let response: CoinbaseTradesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(response.trades.len(), 2);
        assert_eq!(response.trades[0].trade_id, "34964583");
        assert_eq!(response.trades[0].price, "26260.98");
        assert_eq!(response.trades[0].size, "0.003425");
        assert_eq!(response.trades[0].side, "BUY");
        assert_eq!(response.trades[0].time, "2023-03-27T10:51:05.209Z");
        assert_eq!(response.trades[1].trade_id, "34964584");
        assert_eq!(response.trades[1].side, "SELL");
    }

    #[test]
    fn test_try_from_coinbase_rest_trade_for_rest_trade() {
        // Test BUY trade
        let raw_buy = CoinbaseRestTrade {
            trade_id: "34964583".to_string(),
            price: "26260.98".to_string(),
            size: "0.003425".to_string(),
            side: "BUY".to_string(),
            time: "2023-03-27T10:51:05.209Z".to_string(),
        };

        let trade = RestTrade::try_from(raw_buy).unwrap();

        assert_eq!(trade.id, "34964583");
        assert_eq!(
            trade.time,
            DateTime::parse_from_rfc3339("2023-03-27T10:51:05.209Z")
                .unwrap()
                .with_timezone(&Utc)
        );
        assert!((trade.price - 26260.98).abs() < 1e-10);
        assert!((trade.amount - 0.003425).abs() < 1e-10);
        assert_eq!(trade.side, Side::Buy);

        // Test SELL trade
        let raw_sell = CoinbaseRestTrade {
            trade_id: "34964584".to_string(),
            price: "26261.50".to_string(),
            size: "0.001000".to_string(),
            side: "SELL".to_string(),
            time: "2023-03-27T10:51:06.100Z".to_string(),
        };

        let trade_sell = RestTrade::try_from(raw_sell).unwrap();

        assert_eq!(trade_sell.id, "34964584");
        assert_eq!(trade_sell.side, Side::Sell);
    }

    #[test]
    fn test_try_from_coinbase_rest_trade_invalid_side() {
        let raw = CoinbaseRestTrade {
            trade_id: "34964583".to_string(),
            price: "26260.98".to_string(),
            size: "0.003425".to_string(),
            side: "UNKNOWN".to_string(),
            time: "2023-03-27T10:51:05.209Z".to_string(),
        };

        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("unknown trade side"));
    }

    #[test]
    fn test_try_from_coinbase_rest_trade_invalid_time() {
        let raw = CoinbaseRestTrade {
            trade_id: "34964583".to_string(),
            price: "26260.98".to_string(),
            size: "0.003425".to_string(),
            side: "BUY".to_string(),
            time: "not_a_timestamp".to_string(),
        };

        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse time"));
    }

    #[test]
    fn test_try_from_coinbase_rest_trade_invalid_price() {
        let raw = CoinbaseRestTrade {
            trade_id: "34964583".to_string(),
            price: "bad_price".to_string(),
            size: "0.003425".to_string(),
            side: "BUY".to_string(),
            time: "2023-03-27T10:51:05.209Z".to_string(),
        };

        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse price"));
    }

    #[test]
    fn test_get_coinbase_trades_path() {
        let request = GetCoinbaseTrades {
            path: "/api/v3/brokerage/market/products/BTC-USD/ticker".to_string(),
            params: GetCoinbaseTradesParams {
                start: Some(1672502400),
                end: Some(1672588800),
                limit: Some(100),
            },
        };

        assert_eq!(
            request.path(),
            Cow::<str>::Owned(
                "/api/v3/brokerage/market/products/BTC-USD/ticker".to_string()
            )
        );
        assert_eq!(GetCoinbaseTrades::method(), reqwest::Method::GET);
        assert!(request.query_params().is_some());
    }
}
