use crate::{error::DataError, subscription::candle::Candle};
use chrono::DateTime;
use serde::Deserialize;

/// CSV record for Binance klines bulk archives.
///
/// **Klines CSV (12 columns):**
/// `open_time, open, high, low, close, volume, close_time, quote_volume, count,
///  taker_buy_base_vol, taker_buy_quote_vol, ignore`
///
/// Spot CSVs have no header row; futures CSVs have a header row.
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct BinanceBulkKline {
    pub open_time: i64,
    pub open: String,
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: String,
    pub close_time: i64,
    pub quote_volume: String,
    pub count: u64,
    #[serde(alias = "taker_buy_base_asset_volume")]
    pub taker_buy_base_vol: String,
    #[serde(alias = "taker_buy_quote_asset_volume")]
    pub taker_buy_quote_vol: String,
    pub ignore: String,
}

/// Convert a bulk kline record to a [`Candle`].
pub fn convert_kline(record: BinanceBulkKline) -> Result<Candle, DataError> {
    let open_time = DateTime::from_timestamp_millis(record.open_time).ok_or_else(|| {
        DataError::DataParse(format!("invalid kline open_time: {}", record.open_time))
    })?;

    let close_time = DateTime::from_timestamp_millis(record.close_time).ok_or_else(|| {
        DataError::DataParse(format!("invalid kline close_time: {}", record.close_time))
    })?;

    let open: f64 = record
        .open
        .parse()
        .map_err(|e| DataError::DataParse(format!("invalid kline open '{}': {e}", record.open)))?;

    let high: f64 = record
        .high
        .parse()
        .map_err(|e| DataError::DataParse(format!("invalid kline high '{}': {e}", record.high)))?;

    let low: f64 = record
        .low
        .parse()
        .map_err(|e| DataError::DataParse(format!("invalid kline low '{}': {e}", record.low)))?;

    let close: f64 = record.close.parse().map_err(|e| {
        DataError::DataParse(format!("invalid kline close '{}': {e}", record.close))
    })?;

    let volume: f64 = record.volume.parse().map_err(|e| {
        DataError::DataParse(format!("invalid kline volume '{}': {e}", record.volume))
    })?;

    let quote_volume: f64 = record.quote_volume.parse().map_err(|e| {
        DataError::DataParse(format!(
            "invalid kline quote_volume '{}': {e}",
            record.quote_volume
        ))
    })?;

    Ok(Candle {
        open_time,
        close_time,
        open,
        high,
        low,
        close,
        volume,
        quote_volume: Some(quote_volume),
        trade_count: record.count,
        is_closed: true,
    })
}

/// Parse bulk klines CSV data into a vector of [`Candle`]s.
pub fn parse_klines(csv_data: &[u8], has_headers: bool) -> Result<Vec<Candle>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .flexible(true)
        .from_reader(csv_data);

    let mut candles = Vec::new();
    for result in reader.deserialize::<BinanceBulkKline>() {
        let record = result
            .map_err(|e| DataError::DataParse(format!("failed to parse kline CSV row: {e}")))?;
        candles.push(convert_kline(record)?);
    }

    Ok(candles)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spot CSV: 12 columns, no header.
    const SPOT_KLINE_CSV: &str = "\
1499040000000,0.01634000,0.80000000,0.01575800,0.01577100,148976.11427815,1499644799999,2434.19055334,308,1756.87402397,28.46694368,17928899.62484339
";

    /// Futures CSV: 12 columns, with header.
    const FUTURES_KLINE_CSV: &str = "\
open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_base_asset_volume,taker_buy_quote_asset_volume,ignore
1569888000000,8042.44000000,8200.00000000,7842.00000000,7971.86000000,71251.33200000,1569974399999,572438577.55620790,182536,35765.48700000,288349993.37842010,0
";

    #[test]
    fn test_parse_spot_klines() {
        let candles = parse_klines(SPOT_KLINE_CSV.as_bytes(), false).unwrap();
        assert_eq!(candles.len(), 1);

        let c = &candles[0];
        assert_eq!(
            c.open_time,
            DateTime::from_timestamp_millis(1499040000000).unwrap()
        );
        assert_eq!(
            c.close_time,
            DateTime::from_timestamp_millis(1499644799999).unwrap()
        );
        assert_eq!(c.open, 0.01634);
        assert_eq!(c.high, 0.8);
        assert_eq!(c.low, 0.015758);
        assert_eq!(c.close, 0.015771);
        assert_eq!(c.volume, 148976.11427815);
        assert_eq!(c.quote_volume, Some(2434.19055334));
        assert_eq!(c.trade_count, 308);
        assert!(c.is_closed);
    }

    #[test]
    fn test_parse_futures_klines() {
        let candles = parse_klines(FUTURES_KLINE_CSV.as_bytes(), true).unwrap();
        assert_eq!(candles.len(), 1);

        let c = &candles[0];
        assert_eq!(
            c.open_time,
            DateTime::from_timestamp_millis(1569888000000).unwrap()
        );
        assert_eq!(
            c.close_time,
            DateTime::from_timestamp_millis(1569974399999).unwrap()
        );
        assert_eq!(c.open, 8042.44);
        assert_eq!(c.high, 8200.0);
        assert_eq!(c.low, 7842.0);
        assert_eq!(c.close, 7971.86);
        assert_eq!(c.volume, 71251.332);
        assert_eq!(c.quote_volume, Some(572438577.5562079));
        assert_eq!(c.trade_count, 182536);
        assert!(c.is_closed);
    }

    #[test]
    fn test_empty_csv_returns_empty_vec() {
        let candles = parse_klines(b"", false).unwrap();
        assert!(candles.is_empty());
    }

    #[test]
    fn test_invalid_open_price_returns_error() {
        let csv = "1499040000000,not_a_number,0.8,0.01,0.015,100.0,1499644799999,2434.0,308,1756.0,28.0,0\n";
        let result = parse_klines(csv.as_bytes(), false);
        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_kline_rows() {
        let csv = "\
1499040000000,100.0,110.0,90.0,105.0,1000.0,1499126399999,50000.0,100,500.0,25000.0,0
1499126400000,105.0,115.0,95.0,110.0,1200.0,1499212799999,60000.0,120,600.0,30000.0,0
";
        let candles = parse_klines(csv.as_bytes(), false).unwrap();
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].open, 100.0);
        assert_eq!(candles[1].open, 105.0);
    }
}
