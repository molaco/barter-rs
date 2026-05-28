use crate::{error::DataError, trade::RestTrade};
use barter_instrument::Side;
use chrono::DateTime;
use serde::Deserialize;

/// A single trade row from OKX's bulk CSV archive.
///
/// CSV columns: `instrument_name, trade_id, side, price, size, created_time`
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct OkxBulkTrade {
    pub instrument_name: String,
    pub trade_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub created_time: String,
}

impl TryFrom<OkxBulkTrade> for RestTrade {
    type Error = DataError;

    fn try_from(trade: OkxBulkTrade) -> Result<Self, Self::Error> {
        let millis = trade.created_time.parse::<i64>().map_err(|e| {
            DataError::DataParse(format!(
                "invalid OKX bulk timestamp '{}': {e}",
                trade.created_time
            ))
        })?;

        let time = DateTime::from_timestamp_millis(millis).ok_or_else(|| {
            DataError::DataParse(format!("invalid OKX bulk timestamp millis: {millis}"))
        })?;

        let side = match trade.side.to_lowercase().as_str() {
            "buy" => Side::Buy,
            "sell" => Side::Sell,
            other => {
                return Err(DataError::DataParse(format!(
                    "unknown OKX bulk side: {other}"
                )));
            }
        };

        let price = trade.price.parse::<f64>().map_err(|e| {
            DataError::DataParse(format!("invalid OKX bulk price '{}': {e}", trade.price))
        })?;

        let amount = trade.size.parse::<f64>().map_err(|e| {
            DataError::DataParse(format!("invalid OKX bulk size '{}': {e}", trade.size))
        })?;

        Ok(RestTrade {
            id: trade.trade_id,
            time,
            price,
            amount,
            side,
        })
    }
}

/// Parse OKX bulk CSV data (with headers) into a vector of [`RestTrade`].
pub fn parse_trades(csv_data: &[u8]) -> Result<Vec<RestTrade>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_data);

    let mut trades = Vec::new();
    for result in reader.deserialize::<OkxBulkTrade>() {
        let raw =
            result.map_err(|e| DataError::DataParse(format!("OKX bulk CSV parse error: {e}")))?;
        trades.push(RestTrade::try_from(raw)?);
    }

    Ok(trades)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CSV: &str = "\
instrument_name,trade_id,side,price,size,created_time
BTC-USDT,100001,buy,42000.50,0.001,1704067200123
BTC-USDT,100002,sell,41999.00,0.002,1704067201456
";

    #[test]
    fn test_parse_trades() {
        let trades = parse_trades(SAMPLE_CSV.as_bytes()).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "100001");
        assert_eq!(trades[0].price, 42000.50);
        assert_eq!(trades[0].amount, 0.001);
        assert_eq!(trades[0].side, Side::Buy);

        assert_eq!(trades[1].id, "100002");
        assert_eq!(trades[1].price, 41999.00);
        assert_eq!(trades[1].amount, 0.002);
        assert_eq!(trades[1].side, Side::Sell);
    }

    #[test]
    fn test_parse_trades_timestamp_conversion() {
        let trades = parse_trades(SAMPLE_CSV.as_bytes()).unwrap();
        assert_eq!(trades[0].time.timestamp_millis(), 1704067200123);
        assert_eq!(trades[1].time.timestamp_millis(), 1704067201456);
    }

    #[test]
    fn test_parse_trades_case_insensitive_side() {
        let csv = "\
instrument_name,trade_id,side,price,size,created_time
BTC-USDT,1,BUY,42000.0,0.001,1704067200000
BTC-USDT,2,Sell,41000.0,0.002,1704067200000
";
        let trades = parse_trades(csv.as_bytes()).unwrap();
        assert_eq!(trades[0].side, Side::Buy);
        assert_eq!(trades[1].side, Side::Sell);
    }

    #[test]
    fn test_parse_trades_invalid_side() {
        let csv = "\
instrument_name,trade_id,side,price,size,created_time
BTC-USDT,1,unknown,42000.0,0.001,1704067200000
";
        let result = parse_trades(csv.as_bytes());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unknown OKX bulk side")
        );
    }

    #[test]
    fn test_parse_trades_invalid_timestamp() {
        let csv = "\
instrument_name,trade_id,side,price,size,created_time
BTC-USDT,1,buy,42000.0,0.001,not_a_number
";
        let result = parse_trades(csv.as_bytes());
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("invalid OKX bulk timestamp")
        );
    }

    #[test]
    fn test_parse_trades_empty_csv() {
        let csv = "\
instrument_name,trade_id,side,price,size,created_time
";
        let trades = parse_trades(csv.as_bytes()).unwrap();
        assert!(trades.is_empty());
    }
}
