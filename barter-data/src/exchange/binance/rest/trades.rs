use crate::rest::RestTrade;
use barter_instrument::Side;
use barter_integration::protocol::http::rest::RestRequest;
use chrono::DateTime;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

/// Raw aggregate trade from Binance REST API (`/api/v3/aggTrades` or `/fapi/v1/aggTrades`).
///
/// Binance aggTrades use terse single-letter JSON keys.
/// See: <https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list>
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceAggTrade {
    /// Aggregate trade ID.
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    /// Price.
    #[serde(rename = "p")]
    pub price: String,
    /// Quantity.
    #[serde(rename = "q")]
    pub amount: String,
    /// Timestamp in milliseconds.
    #[serde(rename = "T")]
    pub timestamp: u64,
    /// Was the buyer the maker? `true` = seller initiated (Side::Sell).
    #[serde(rename = "m")]
    pub buyer_is_maker: bool,
}

/// REST request to fetch aggregate trade data from a Binance API variant.
///
/// The `path` field stores the endpoint path, which differs between spot
/// (`/api/v3/aggTrades`) and futures (`/fapi/v1/aggTrades`).
#[derive(Debug, Clone)]
pub struct GetAggTrades {
    /// Endpoint path (varies by server variant, e.g. spot vs futures).
    pub path: &'static str,
    /// Query parameters for the aggTrades request.
    pub params: GetAggTradesParams,
}

/// Query parameters for a Binance aggTrades REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetAggTradesParams {
    pub symbol: String,
    #[serde(rename = "startTime", skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    #[serde(rename = "endTime", skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    /// Aggregate trade ID to fetch from (inclusive).
    ///
    /// When set, the API returns trades starting from this ID, ignoring
    /// `startTime`. Used for ID-based pagination which avoids skipping
    /// trades that share the same millisecond timestamp at batch boundaries.
    #[serde(rename = "fromId", skip_serializing_if = "Option::is_none")]
    pub from_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetAggTrades {
    type Response = Vec<BinanceAggTrade>;
    type QueryParams = GetAggTradesParams;
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

impl TryFrom<BinanceAggTrade> for RestTrade {
    type Error = String;

    fn try_from(raw: BinanceAggTrade) -> Result<Self, Self::Error> {
        let timestamp_i64 = i64::try_from(raw.timestamp)
            .map_err(|_| format!("timestamp overflow: {} exceeds i64::MAX", raw.timestamp))?;
        let time = DateTime::from_timestamp_millis(timestamp_i64)
            .ok_or_else(|| format!("invalid timestamp millis: {}", raw.timestamp))?;

        let price = raw
            .price
            .parse::<f64>()
            .map_err(|e| format!("failed to parse price '{}': {}", raw.price, e))?;

        let amount = raw
            .amount
            .parse::<f64>()
            .map_err(|e| format!("failed to parse amount '{}': {}", raw.amount, e))?;

        // buyer_is_maker: true means the trade was seller-initiated (taker sold)
        let side = if raw.buyer_is_maker {
            Side::Sell
        } else {
            Side::Buy
        };

        Ok(RestTrade {
            id: raw.agg_trade_id.to_string(),
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
    fn test_deserialize_binance_agg_trade() {
        let json = r#"{
            "a": 26129,
            "p": "0.01633000",
            "q": "4.70443515",
            "f": 27781,
            "l": 27781,
            "T": 1498793709153,
            "m": true,
            "M": true
        }"#;

        let raw: BinanceAggTrade = serde_json::from_str(json).unwrap();
        assert_eq!(raw.agg_trade_id, 26129);
        assert_eq!(raw.price, "0.01633000");
        assert_eq!(raw.amount, "4.70443515");
        assert_eq!(raw.timestamp, 1498793709153);
        assert!(raw.buyer_is_maker);
    }

    #[test]
    fn test_try_from_binance_agg_trade_for_rest_trade() {
        let raw = BinanceAggTrade {
            agg_trade_id: 26129,
            price: "0.01633000".to_string(),
            amount: "4.70443515".to_string(),
            timestamp: 1498793709153,
            buyer_is_maker: true,
        };

        let trade = RestTrade::try_from(raw).unwrap();

        assert_eq!(trade.id, "26129");
        assert_eq!(
            trade.time,
            DateTime::from_timestamp_millis(1498793709153).unwrap()
        );
        assert!((trade.price - 0.01633).abs() < 1e-10);
        assert!((trade.amount - 4.70443515).abs() < 1e-10);
        assert_eq!(trade.side, Side::Sell);

        // Test buyer-initiated trade (buyer_is_maker = false => Side::Buy)
        let raw_buy = BinanceAggTrade {
            agg_trade_id: 26130,
            price: "0.01634000".to_string(),
            amount: "1.00000000".to_string(),
            timestamp: 1498793710000,
            buyer_is_maker: false,
        };

        let trade_buy = RestTrade::try_from(raw_buy).unwrap();
        assert_eq!(trade_buy.side, Side::Buy);
    }

    #[test]
    fn test_deserialize_binance_agg_trade_vec() {
        let json = r#"[
            {
                "a": 26129,
                "p": "0.01633000",
                "q": "4.70443515",
                "f": 27781,
                "l": 27781,
                "T": 1498793709153,
                "m": true,
                "M": true
            },
            {
                "a": 26130,
                "p": "0.01634000",
                "q": "1.00000000",
                "f": 27782,
                "l": 27782,
                "T": 1498793710000,
                "m": false,
                "M": true
            }
        ]"#;

        let trades: Vec<BinanceAggTrade> = serde_json::from_str(json).unwrap();
        assert_eq!(trades.len(), 2);
        assert_eq!(trades[0].agg_trade_id, 26129);
        assert_eq!(trades[1].agg_trade_id, 26130);
        assert!(trades[0].buyer_is_maker);
        assert!(!trades[1].buyer_is_maker);
    }

    #[test]
    fn test_try_from_binance_agg_trade_invalid_price() {
        let raw = BinanceAggTrade {
            agg_trade_id: 1,
            price: "not_a_number".to_string(),
            amount: "0.5".to_string(),
            timestamp: 1679907065209,
            buyer_is_maker: false,
        };
        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse price"));
    }

    #[test]
    fn test_try_from_binance_agg_trade_invalid_amount() {
        let raw = BinanceAggTrade {
            agg_trade_id: 1,
            price: "26260.98".to_string(),
            amount: "bad".to_string(),
            timestamp: 1679907065209,
            buyer_is_maker: false,
        };
        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse amount"));
    }
}
