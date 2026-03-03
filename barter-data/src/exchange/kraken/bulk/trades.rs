use crate::{error::DataError, trade::RestTrade};
use barter_instrument::Side;
use chrono::DateTime;
use serde::Deserialize;

/// CSV record for Kraken bulk trade archives.
///
/// **CSV format:** No header, 7 columns:
/// `price, volume, time, buy_sell, market_limit, misc, trade_id`
#[derive(Debug, Deserialize)]
pub struct KrakenBulkTrade {
    pub price: String,
    pub volume: String,
    /// Unix timestamp with fractional seconds.
    pub time: f64,
    /// `"b"` for buy, `"s"` for sell.
    pub buy_sell: String,
    /// `"m"` for market, `"l"` for limit.
    pub market_limit: String,
    pub misc: String,
    pub trade_id: u64,
}

impl TryFrom<KrakenBulkTrade> for RestTrade {
    type Error = DataError;

    fn try_from(record: KrakenBulkTrade) -> Result<Self, Self::Error> {
        let secs = record.time as i64;
        let nanos = ((record.time.fract().abs()) * 1_000_000_000.0) as u32;

        let time = DateTime::from_timestamp(secs, nanos).ok_or_else(|| {
            DataError::Socket(format!("invalid trade timestamp: {}", record.time))
        })?;

        let price: f64 = record.price.parse().map_err(|e| {
            DataError::Socket(format!(
                "invalid trade price '{}': {e}",
                record.price
            ))
        })?;

        let amount: f64 = record.volume.parse().map_err(|e| {
            DataError::Socket(format!(
                "invalid trade volume '{}': {e}",
                record.volume
            ))
        })?;

        let side = match record.buy_sell.as_str() {
            "b" => Side::Buy,
            "s" => Side::Sell,
            other => {
                return Err(DataError::Socket(format!(
                    "unknown trade side '{other}'"
                )));
            }
        };

        Ok(RestTrade {
            id: record.trade_id.to_string(),
            time,
            price,
            amount,
            side,
        })
    }
}

/// Parse Kraken bulk trade CSV data into a vector of [`RestTrade`]s.
///
/// Expects headerless CSV with 7 columns:
/// `price, volume, time, buy_sell, market_limit, misc, trade_id`
pub fn parse_trades(csv_data: &[u8]) -> Result<Vec<RestTrade>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(csv_data);

    let mut trades = Vec::new();
    for result in reader.deserialize::<KrakenBulkTrade>() {
        let record = result.map_err(|e| {
            DataError::Socket(format!("failed to parse Kraken trade CSV row: {e}"))
        })?;
        trades.push(RestTrade::try_from(record)?);
    }

    Ok(trades)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CSV: &str = "\
42000.00000,0.10000000,1704067200.1234,b,m,,100
42001.50000,0.20000000,1704067201.5678,s,l,,101
";

    #[test]
    fn test_parse_trades() {
        let trades = parse_trades(SAMPLE_CSV.as_bytes()).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "100");
        assert_eq!(trades[0].price, 42000.0);
        assert_eq!(trades[0].amount, 0.1);
        assert_eq!(trades[0].side, Side::Buy);

        assert_eq!(trades[1].id, "101");
        assert_eq!(trades[1].price, 42001.5);
        assert_eq!(trades[1].amount, 0.2);
        assert_eq!(trades[1].side, Side::Sell);
    }

    #[test]
    fn test_parse_trades_timestamp_fractional() {
        let csv = "50000.00,1.0,1704067200.1234,b,m,,200\n";
        let trades = parse_trades(csv.as_bytes()).unwrap();
        assert_eq!(trades.len(), 1);

        let trade = &trades[0];
        assert_eq!(trade.time.timestamp(), 1704067200);
        // Fractional nanos: 0.1234 * 1e9 ≈ 123400000
        assert!(trade.time.timestamp_subsec_nanos() > 0);
    }

    #[test]
    fn test_parse_trades_integer_timestamp() {
        let csv = "50000.00,1.0,1704067200.0,b,m,,300\n";
        let trades = parse_trades(csv.as_bytes()).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].time.timestamp(), 1704067200);
        assert_eq!(trades[0].time.timestamp_subsec_nanos(), 0);
    }

    #[test]
    fn test_parse_trades_empty() {
        let trades = parse_trades(b"").unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn test_parse_trades_invalid_price() {
        let csv = "not_a_number,1.0,1704067200.0,b,m,,400\n";
        let result = parse_trades(csv.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_trades_invalid_side() {
        let csv = "50000.00,1.0,1704067200.0,x,m,,500\n";
        let result = parse_trades(csv.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn test_kraken_bulk_trade_try_from_buy() {
        let record = KrakenBulkTrade {
            price: "42000.00".to_string(),
            volume: "0.5".to_string(),
            time: 1704067200.123,
            buy_sell: "b".to_string(),
            market_limit: "m".to_string(),
            misc: String::new(),
            trade_id: 42,
        };
        let trade = RestTrade::try_from(record).unwrap();
        assert_eq!(trade.id, "42");
        assert_eq!(trade.price, 42000.0);
        assert_eq!(trade.amount, 0.5);
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.time.timestamp(), 1704067200);
    }

    #[test]
    fn test_kraken_bulk_trade_try_from_sell() {
        let record = KrakenBulkTrade {
            price: "2200.00".to_string(),
            volume: "1.0".to_string(),
            time: 1704067200.0,
            buy_sell: "s".to_string(),
            market_limit: "l".to_string(),
            misc: String::new(),
            trade_id: 43,
        };
        let trade = RestTrade::try_from(record).unwrap();
        assert_eq!(trade.side, Side::Sell);
    }

    #[test]
    fn test_multiple_rows() {
        let csv = "\
10000.00,0.01,1600000000.0,b,m,,1
10001.00,0.02,1600000001.0,s,l,,2
10002.00,0.03,1600000002.0,b,m,,3
";
        let trades = parse_trades(csv.as_bytes()).unwrap();
        assert_eq!(trades.len(), 3);
        assert_eq!(trades[0].id, "1");
        assert_eq!(trades[1].id, "2");
        assert_eq!(trades[2].id, "3");
    }
}
