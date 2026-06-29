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
    /// First (raw) trade ID covered by this aggregate trade.
    ///
    /// Use this to seed `fromId` pagination of the individual-trades endpoint
    /// (`/historicalTrades`), which returns raw trades matching the live
    /// `@trade` stream rather than aggregated trades.
    #[serde(rename = "f")]
    pub first_trade_id: u64,
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

/// Raw individual trade from Binance REST API (`/historicalTrades` or `/trades`).
///
/// Unlike [`BinanceAggTrade`], these are *individual* trades that line up 1:1
/// with the live `@trade` WebSocket stream, so they are the correct source for
/// gap-filling recent windows. Fetched via `fromId` pagination on the
/// `/historicalTrades` endpoint. See:
/// <https://binance-docs.github.io/apidocs/spot/en/#old-trade-lookup>
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceRawTrade {
    /// Trade ID.
    #[serde(rename = "id")]
    pub id: u64,
    /// Price.
    #[serde(rename = "price")]
    pub price: String,
    /// Quantity.
    #[serde(rename = "qty")]
    pub qty: String,
    /// Timestamp in milliseconds.
    #[serde(rename = "time")]
    pub time: u64,
    /// Was the buyer the maker? `true` = seller initiated (Side::Sell).
    #[serde(rename = "isBuyerMaker")]
    pub is_buyer_maker: bool,
}

/// REST request to fetch individual (raw) trade data from a Binance API variant.
///
/// Uses the `/historicalTrades` endpoint (paged by `fromId`) to retrieve
/// individual trades that match the live `@trade` stream, rather than
/// aggregated trades. The `path` field stores the endpoint path, which differs
/// between spot (`/api/v3/historicalTrades`) and futures
/// (`/fapi/v1/historicalTrades`).
#[derive(Debug, Clone)]
pub struct GetHistoricalTrades {
    /// Endpoint path (varies by server variant, e.g. spot vs futures).
    pub path: &'static str,
    /// Query parameters for the historicalTrades request.
    pub params: GetHistoricalTradesParams,
}

/// Query parameters for a Binance historicalTrades REST request.
#[derive(Debug, Clone, Serialize)]
pub struct GetHistoricalTradesParams {
    pub symbol: String,
    /// Raw trade ID to fetch from (inclusive). When omitted, the most recent
    /// trades are returned.
    #[serde(rename = "fromId", skip_serializing_if = "Option::is_none")]
    pub from_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

impl RestRequest for GetHistoricalTrades {
    type Response = Vec<BinanceRawTrade>;
    type QueryParams = GetHistoricalTradesParams;
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

impl TryFrom<BinanceRawTrade> for RestTrade {
    type Error = String;

    fn try_from(raw: BinanceRawTrade) -> Result<Self, Self::Error> {
        let timestamp_i64 = i64::try_from(raw.time)
            .map_err(|_| format!("timestamp overflow: {} exceeds i64::MAX", raw.time))?;
        let time = DateTime::from_timestamp_millis(timestamp_i64)
            .ok_or_else(|| format!("invalid timestamp millis: {}", raw.time))?;

        let price = raw
            .price
            .parse::<f64>()
            .map_err(|e| format!("failed to parse price '{}': {}", raw.price, e))?;

        let amount = raw
            .qty
            .parse::<f64>()
            .map_err(|e| format!("failed to parse amount '{}': {}", raw.qty, e))?;

        // is_buyer_maker: true means the trade was seller-initiated (taker sold)
        let side = if raw.is_buyer_maker {
            Side::Sell
        } else {
            Side::Buy
        };

        Ok(RestTrade {
            id: raw.id.to_string(),
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
            first_trade_id: 27781,
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
            first_trade_id: 27782,
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
            first_trade_id: 1,
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
            first_trade_id: 1,
            price: "26260.98".to_string(),
            amount: "bad".to_string(),
            timestamp: 1679907065209,
            buyer_is_maker: false,
        };
        let result = RestTrade::try_from(raw);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed to parse amount"));
    }

    #[test]
    fn test_deserialize_binance_raw_trade_futures() {
        // Futures `/fapi/v1/historicalTrades` object shape (with `quoteQty`).
        let json = r#"{
            "id": 28457,
            "price": "29000.00",
            "qty": "0.50000000",
            "quoteQty": "14500.00",
            "time": 1609459200000,
            "isBuyerMaker": true
        }"#;

        let raw: BinanceRawTrade = serde_json::from_str(json).unwrap();
        assert_eq!(raw.id, 28457);
        assert_eq!(raw.price, "29000.00");
        assert_eq!(raw.qty, "0.50000000");
        assert_eq!(raw.time, 1609459200000);
        assert!(raw.is_buyer_maker);
    }

    #[test]
    fn test_deserialize_binance_raw_trade_spot_ignores_unknown() {
        // Spot `/api/v3/historicalTrades` object carries an extra `isBestMatch`
        // field which serde must ignore (no `deny_unknown_fields`).
        let json = r#"{
            "id": 28458,
            "price": "29100.50",
            "qty": "1.20000000",
            "quoteQty": "34920.60",
            "time": 1609459260000,
            "isBuyerMaker": false,
            "isBestMatch": true
        }"#;

        let raw: BinanceRawTrade = serde_json::from_str(json).unwrap();
        assert_eq!(raw.id, 28458);
        assert_eq!(raw.time, 1609459260000);
        assert!(!raw.is_buyer_maker);
    }

    #[test]
    fn test_try_from_binance_raw_trade_side_mapping() {
        // isBuyerMaker = true => seller-initiated => Side::Sell
        let raw_sell = BinanceRawTrade {
            id: 1,
            price: "29000.00".to_string(),
            qty: "0.5".to_string(),
            time: 1609459200000,
            is_buyer_maker: true,
        };
        let trade_sell = RestTrade::try_from(raw_sell).unwrap();
        assert_eq!(trade_sell.id, "1");
        assert_eq!(
            trade_sell.time,
            DateTime::from_timestamp_millis(1609459200000).unwrap()
        );
        assert!((trade_sell.price - 29000.0).abs() < 1e-9);
        assert!((trade_sell.amount - 0.5).abs() < 1e-9);
        assert_eq!(trade_sell.side, Side::Sell);

        // isBuyerMaker = false => buyer-initiated => Side::Buy
        let raw_buy = BinanceRawTrade {
            id: 2,
            price: "29100.50".to_string(),
            qty: "1.2".to_string(),
            time: 1609459260000,
            is_buyer_maker: false,
        };
        let trade_buy = RestTrade::try_from(raw_buy).unwrap();
        assert_eq!(trade_buy.side, Side::Buy);
    }

    #[test]
    fn test_get_historical_trades_params_serialize() {
        // Both optional fields present: `fromId` rename applied, `limit` kept.
        let params = GetHistoricalTradesParams {
            symbol: "BTCUSDT".to_string(),
            from_id: Some(28457),
            limit: Some(500),
        };
        let value = serde_json::to_value(&params).unwrap();
        assert_eq!(value["symbol"], "BTCUSDT");
        assert_eq!(value["fromId"], 28457);
        assert_eq!(value["limit"], 500);

        // None fields are omitted entirely (skip_serializing_if).
        let params_min = GetHistoricalTradesParams {
            symbol: "ETHUSDT".to_string(),
            from_id: None,
            limit: None,
        };
        let value_min = serde_json::to_value(&params_min).unwrap();
        assert_eq!(value_min["symbol"], "ETHUSDT");
        assert!(value_min.get("fromId").is_none());
        assert!(value_min.get("limit").is_none());
    }
}
