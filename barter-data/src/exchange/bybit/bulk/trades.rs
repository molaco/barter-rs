use crate::{error::DataError, trade::RestTrade};
use barter_instrument::Side;
use chrono::DateTime;
use serde::Deserialize;

/// A single trade row from Bybit's bulk CSV archive.
///
/// CSV columns: `timestamp, symbol, side, size, price, tickDirection,
/// trdMatchID, grossValue, homeNotional, foreignNotional`
#[derive(Debug, Deserialize)]
pub struct BybitBulkTrade {
    pub timestamp: f64,
    pub symbol: String,
    pub side: String,
    pub size: String,
    pub price: String,
    #[serde(rename = "tickDirection")]
    pub tick_direction: String,
    #[serde(rename = "trdMatchID")]
    pub trd_match_id: String,
    #[serde(rename = "grossValue")]
    pub gross_value: String,
    #[serde(rename = "homeNotional")]
    pub home_notional: String,
    #[serde(rename = "foreignNotional")]
    pub foreign_notional: String,
}

impl TryFrom<BybitBulkTrade> for RestTrade {
    type Error = DataError;

    fn try_from(trade: BybitBulkTrade) -> Result<Self, Self::Error> {
        let millis = (trade.timestamp * 1000.0) as i64;
        let time = DateTime::from_timestamp_millis(millis).ok_or_else(|| {
            DataError::Socket(format!(
                "invalid Bybit bulk timestamp: {}",
                trade.timestamp
            ))
        })?;

        let side = match trade.side.as_str() {
            "Buy" => Side::Buy,
            "Sell" => Side::Sell,
            other => {
                return Err(DataError::Socket(format!(
                    "unknown Bybit bulk side: {other}"
                )))
            }
        };

        let price = trade.price.parse::<f64>().map_err(|e| {
            DataError::Socket(format!("invalid Bybit bulk price '{}': {e}", trade.price))
        })?;

        let amount = trade.size.parse::<f64>().map_err(|e| {
            DataError::Socket(format!("invalid Bybit bulk size '{}': {e}", trade.size))
        })?;

        Ok(RestTrade {
            id: trade.trd_match_id,
            time,
            price,
            amount,
            side,
        })
    }
}

/// Parse Bybit bulk CSV data (with headers) into a vector of [`RestTrade`].
pub fn parse_trades(csv_data: &[u8]) -> Result<Vec<RestTrade>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_data);

    let mut trades = Vec::new();
    for result in reader.deserialize::<BybitBulkTrade>() {
        let raw = result
            .map_err(|e| DataError::Socket(format!("Bybit bulk CSV parse error: {e}")))?;
        trades.push(RestTrade::try_from(raw)?);
    }

    Ok(trades)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CSV: &str = "\
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
1704067200.123,BTCUSDT,Buy,0.001,42000.50,PlusTick,abc-123,42000500,0.001,42.0005
1704067201.456,BTCUSDT,Sell,0.002,41999.00,MinusTick,def-456,83998000,0.002,83.998
";

    #[test]
    fn test_parse_trades() {
        let trades = parse_trades(SAMPLE_CSV.as_bytes()).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "abc-123");
        assert_eq!(trades[0].price, 42000.50);
        assert_eq!(trades[0].amount, 0.001);
        assert_eq!(trades[0].side, Side::Buy);

        assert_eq!(trades[1].id, "def-456");
        assert_eq!(trades[1].price, 41999.00);
        assert_eq!(trades[1].amount, 0.002);
        assert_eq!(trades[1].side, Side::Sell);
    }

    #[test]
    fn test_parse_trades_timestamp_conversion() {
        let trades = parse_trades(SAMPLE_CSV.as_bytes()).unwrap();
        // 1704067200.123 * 1000 = 1704067200123 ms
        assert_eq!(
            trades[0].time.timestamp_millis(),
            1704067200123
        );
    }

    #[test]
    fn test_parse_trades_invalid_side() {
        let csv = "\
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
1704067200.0,BTCUSDT,Unknown,0.001,42000.0,PlusTick,abc,0,0,0
";
        let result = parse_trades(csv.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("unknown Bybit bulk side"));
    }

    #[test]
    fn test_parse_trades_invalid_price() {
        let csv = "\
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
1704067200.0,BTCUSDT,Buy,0.001,not_a_number,PlusTick,abc,0,0,0
";
        let result = parse_trades(csv.as_bytes());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("invalid Bybit bulk price"));
    }

    #[test]
    fn test_parse_trades_empty_csv() {
        let csv = "\
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
";
        let trades = parse_trades(csv.as_bytes()).unwrap();
        assert!(trades.is_empty());
    }
}
