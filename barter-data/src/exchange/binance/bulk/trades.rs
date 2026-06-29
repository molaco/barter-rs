use crate::{error::DataError, trade::RestTrade};
use barter_instrument::Side;
use chrono::DateTime;
use serde::{Deserialize, Deserializer, de};

/// CSV record for Binance aggTrades bulk archives.
///
/// **Spot CSV (no header, 8 columns):**
/// `agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker, is_best_match`
///
/// **Futures CSV (with header, 7 columns):**
/// Same fields minus `is_best_match`.
///
/// Use `flexible(true)` on the CSV reader so both 7-column and 8-column rows parse correctly.
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct BinanceBulkAggTrade {
    pub agg_trade_id: u64,
    pub price: String,
    pub quantity: String,
    pub first_trade_id: u64,
    pub last_trade_id: u64,
    pub transact_time: u64,
    #[serde(deserialize_with = "deserialize_bool_case_insensitive")]
    pub is_buyer_maker: bool,
    /// Only present in spot CSVs (8th column).
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_bool_case_insensitive")]
    pub is_best_match: Option<bool>,
}

/// Convert a bulk aggTrade record to a [`RestTrade`].
///
/// - If `may_use_microsecond_timestamps` is true and the timestamp exceeds 1e15,
///   it is treated as microseconds and divided by 1000 to yield milliseconds.
/// - `is_buyer_maker == true` → [`Side::Sell`] (maker was buyer, so taker sold).
/// - `is_buyer_maker == false` → [`Side::Buy`] (maker was seller, so taker bought).
pub fn convert_trade(
    record: BinanceBulkAggTrade,
    may_use_microsecond_timestamps: bool,
) -> Result<RestTrade, DataError> {
    let ts_ms = if may_use_microsecond_timestamps && record.transact_time > 1_000_000_000_000_000 {
        record.transact_time / 1000
    } else {
        record.transact_time
    };

    let time = DateTime::from_timestamp_millis(ts_ms as i64).ok_or_else(|| {
        DataError::DataParse(format!("invalid trade timestamp: {}", record.transact_time))
    })?;

    let price: f64 = record.price.parse().map_err(|e| {
        DataError::DataParse(format!("invalid trade price '{}': {e}", record.price))
    })?;

    let amount: f64 = record.quantity.parse().map_err(|e| {
        DataError::DataParse(format!("invalid trade quantity '{}': {e}", record.quantity))
    })?;

    let side = if record.is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };

    Ok(RestTrade {
        id: record.agg_trade_id.to_string(),
        time,
        price,
        amount,
        side,
    })
}

/// Parse bulk aggTrades CSV data into a vector of [`RestTrade`]s.
pub fn parse_trades(
    csv_data: &[u8],
    has_headers: bool,
    may_use_microsecond_timestamps: bool,
) -> Result<Vec<RestTrade>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .flexible(true)
        .from_reader(csv_data);

    let mut trades = Vec::new();
    for result in reader.deserialize::<BinanceBulkAggTrade>() {
        let record = result
            .map_err(|e| DataError::DataParse(format!("failed to parse aggTrade CSV row: {e}")))?;
        trades.push(convert_trade(record, may_use_microsecond_timestamps)?);
    }

    Ok(trades)
}

/// CSV record for Binance **raw** `trades` bulk archives (individual trades,
/// matching the live `@trade` stream — not the aggregated `aggTrades` dataset).
///
/// The raw `trades` CSV layout differs by market variant:
///
/// **Spot CSV (no header, 7 columns, positional):**
/// `id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch`
/// Booleans are capitalised (`True`/`False`).
///
/// **USD-M futures CSV (with header, 6 columns):**
/// `id, price, qty, quote_qty, time, is_buyer_maker`
///
/// **COIN-M futures CSV (with header, 6 columns):**
/// `id, price, qty, base_qty, time, is_buyer_maker`
/// (column 4 is `base_qty` rather than `quote_qty`).
///
/// The struct mirrors the trick used by [`BinanceBulkAggTrade`]: field *order*
/// matches the header-less spot positional layout, while field *names* (plus the
/// `base_qty` alias on `quote_qty`) match the futures header columns. Use
/// `flexible(true)` on the CSV reader so both 6-column and 7-column rows parse.
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct BinanceBulkTrade {
    pub id: u64,
    pub price: String,
    pub qty: String,
    /// Column 3 is unused by the converter. Aliased so both futures headers
    /// (`quote_qty` for USD-M, `base_qty` for COIN-M) match by name, and
    /// positionally consumed for the header-less spot CSV (`quoteQty`).
    #[serde(rename = "quote_qty", alias = "base_qty")]
    pub quote_qty: String,
    pub time: u64,
    #[serde(deserialize_with = "deserialize_bool_case_insensitive")]
    pub is_buyer_maker: bool,
    /// Only present in spot CSVs (7th column).
    #[serde(default)]
    #[serde(deserialize_with = "deserialize_optional_bool_case_insensitive")]
    pub is_best_match: Option<bool>,
}

/// Convert a bulk raw-`trades` record to a [`RestTrade`].
///
/// Uses the same microsecond guard, price/quantity parsing, and side mapping as
/// [`convert_trade`]:
/// - If `may_use_microsecond_timestamps` is true and the timestamp exceeds 1e15,
///   it is treated as microseconds and divided by 1000 to yield milliseconds.
/// - `is_buyer_maker == true` → [`Side::Sell`] (maker was buyer, so taker sold).
/// - `is_buyer_maker == false` → [`Side::Buy`] (maker was seller, so taker bought).
pub fn convert_raw_trade(
    record: BinanceBulkTrade,
    may_use_microsecond_timestamps: bool,
) -> Result<RestTrade, DataError> {
    let ts_ms = if may_use_microsecond_timestamps && record.time > 1_000_000_000_000_000 {
        record.time / 1000
    } else {
        record.time
    };

    let time = DateTime::from_timestamp_millis(ts_ms as i64)
        .ok_or_else(|| DataError::DataParse(format!("invalid trade timestamp: {}", record.time)))?;

    let price: f64 = record.price.parse().map_err(|e| {
        DataError::DataParse(format!("invalid trade price '{}': {e}", record.price))
    })?;

    let amount: f64 = record
        .qty
        .parse()
        .map_err(|e| DataError::DataParse(format!("invalid trade quantity '{}': {e}", record.qty)))?;

    let side = if record.is_buyer_maker {
        Side::Sell
    } else {
        Side::Buy
    };

    Ok(RestTrade {
        id: record.id.to_string(),
        time,
        price,
        amount,
        side,
    })
}

/// Parse bulk raw-`trades` CSV data into a vector of [`RestTrade`]s.
pub fn parse_raw_trades(
    csv_data: &[u8],
    has_headers: bool,
    may_use_microsecond_timestamps: bool,
) -> Result<Vec<RestTrade>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(has_headers)
        .flexible(true)
        .from_reader(csv_data);

    let mut trades = Vec::new();
    for result in reader.deserialize::<BinanceBulkTrade>() {
        let record = result
            .map_err(|e| DataError::DataParse(format!("failed to parse trade CSV row: {e}")))?;
        trades.push(convert_raw_trade(record, may_use_microsecond_timestamps)?);
    }

    Ok(trades)
}

/// Deserialize a boolean case-insensitively.
///
/// Handles "True", "true", "TRUE", "False", "false", "FALSE", "1", "0".
fn deserialize_bool_case_insensitive<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.to_lowercase().as_str() {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        other => Err(de::Error::custom(format!(
            "expected boolean string, got '{other}'"
        ))),
    }
}

/// Deserialize an optional boolean case-insensitively.
///
/// Returns `None` when the field is absent (via `#[serde(default)]`).
fn deserialize_optional_bool_case_insensitive<'de, D>(
    deserializer: D,
) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.to_lowercase().as_str() {
        "true" | "1" => Ok(Some(true)),
        "false" | "0" => Ok(Some(false)),
        other => Err(de::Error::custom(format!(
            "expected boolean string, got '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Spot CSV: 8 columns, no header, `True`/`False` booleans.
    const SPOT_CSV: &str = "\
26129,0.00100000,88.00000000,26132,26132,1498793709153,True,True
26130,0.00100000,11.00000000,26133,26133,1498793709153,False,True
";

    /// Futures CSV: 7 columns, with header, `true`/`false` booleans.
    const FUTURES_CSV: &str = "\
agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
1,4261.48000000,0.051000,1,1,1569109379925,false
2,4261.48000000,0.005000,2,2,1569109379925,true
";

    #[test]
    fn test_parse_spot_trades() {
        let trades = parse_trades(SPOT_CSV.as_bytes(), false, false).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "26129");
        assert_eq!(trades[0].price, 0.001);
        assert_eq!(trades[0].amount, 88.0);
        assert_eq!(trades[0].side, Side::Sell); // is_buyer_maker=True → Sell
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1498793709153).unwrap()
        );

        assert_eq!(trades[1].id, "26130");
        assert_eq!(trades[1].side, Side::Buy); // is_buyer_maker=False → Buy
    }

    #[test]
    fn test_parse_futures_trades() {
        let trades = parse_trades(FUTURES_CSV.as_bytes(), true, false).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "1");
        assert_eq!(trades[0].price, 4261.48);
        assert_eq!(trades[0].amount, 0.051);
        assert_eq!(trades[0].side, Side::Buy); // is_buyer_maker=false → Buy
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1569109379925).unwrap()
        );

        assert_eq!(trades[1].id, "2");
        assert_eq!(trades[1].side, Side::Sell); // is_buyer_maker=true → Sell
    }

    #[test]
    fn test_microsecond_timestamp_conversion() {
        // A microsecond timestamp (> 1e15)
        let csv = "100,50000.00,1.0,100,100,1704067200000000,false,true\n";
        let trades = parse_trades(csv.as_bytes(), false, true).unwrap();
        assert_eq!(trades.len(), 1);
        // 1704067200000000 µs → 1704067200000 ms
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1704067200000).unwrap()
        );
    }

    #[test]
    fn test_millisecond_timestamp_unchanged() {
        let csv = "100,50000.00,1.0,100,100,1704067200000,false,true\n";
        let trades = parse_trades(csv.as_bytes(), false, true).unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1704067200000).unwrap()
        );
    }

    #[test]
    fn test_empty_csv_returns_empty_vec() {
        let trades = parse_trades(b"", false, false).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn test_invalid_price_returns_error() {
        let csv = "1,not_a_number,1.0,1,1,1704067200000,false,true\n";
        let result = parse_trades(csv.as_bytes(), false, false);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Raw `trades` parser tests (parse_raw_trades / BinanceBulkTrade)
    // -----------------------------------------------------------------------

    /// USD-M futures raw trades: 6 columns, with header, `quote_qty`,
    /// `true`/`false` booleans, millisecond timestamps.
    const RAW_FUTURES_UM_CSV: &str = "\
id,price,qty,quote_qty,time,is_buyer_maker
3997656,4261.48000000,0.05100000,217.33548000,1569312000277,false
3997657,4261.48000000,0.00500000,21.30740000,1569312000281,true
";

    /// COIN-M futures raw trades: 6 columns, with header, `base_qty` (not
    /// `quote_qty`), `true`/`false` booleans, millisecond timestamps.
    const RAW_FUTURES_CM_CSV: &str = "\
id,price,qty,base_qty,time,is_buyer_maker
123456,40000.0,10,0.00025000,1640995200000,true
123457,40001.5,5,0.00012500,1640995200100,false
";

    /// Spot raw trades: 7 columns, NO header, `True`/`False` booleans.
    const RAW_SPOT_CSV: &str = "\
28457,4986.00000000,5.00000000,24930.00000000,1499040001000,True,True
28458,4986.10000000,2.00000000,9972.20000000,1499040002000,False,True
";

    #[test]
    fn test_parse_raw_futures_um_trades() {
        // USD-M: header present, no microseconds (ms timestamps).
        let trades = parse_raw_trades(RAW_FUTURES_UM_CSV.as_bytes(), true, false).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "3997656");
        assert_eq!(trades[0].price, 4261.48);
        assert_eq!(trades[0].amount, 0.051);
        assert_eq!(trades[0].side, Side::Buy); // is_buyer_maker=false → Buy
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1569312000277).unwrap()
        );

        assert_eq!(trades[1].id, "3997657");
        assert_eq!(trades[1].amount, 0.005);
        assert_eq!(trades[1].side, Side::Sell); // is_buyer_maker=true → Sell
    }

    #[test]
    fn test_parse_raw_futures_cm_trades_base_qty_header() {
        // COIN-M: 4th column header is `base_qty`, matched via serde alias.
        let trades = parse_raw_trades(RAW_FUTURES_CM_CSV.as_bytes(), true, false).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "123456");
        assert_eq!(trades[0].price, 40000.0);
        assert_eq!(trades[0].amount, 10.0);
        assert_eq!(trades[0].side, Side::Sell); // is_buyer_maker=true → Sell
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1640995200000).unwrap()
        );

        assert_eq!(trades[1].id, "123457");
        assert_eq!(trades[1].price, 40001.5);
        assert_eq!(trades[1].side, Side::Buy); // is_buyer_maker=false → Buy
    }

    #[test]
    fn test_parse_raw_spot_trades_microsecond() {
        // Spot: no header (positional), `True`/`False` booleans, microseconds enabled.
        let trades = parse_raw_trades(RAW_SPOT_CSV.as_bytes(), false, true).unwrap();
        assert_eq!(trades.len(), 2);

        assert_eq!(trades[0].id, "28457");
        assert_eq!(trades[0].price, 4986.0);
        assert_eq!(trades[0].amount, 5.0);
        assert_eq!(trades[0].side, Side::Sell); // isBuyerMaker=True → Sell
        // 1499040001000 is below the 1e15 threshold → treated as milliseconds.
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1499040001000).unwrap()
        );

        assert_eq!(trades[1].id, "28458");
        assert_eq!(trades[1].side, Side::Buy); // isBuyerMaker=False → Buy
    }

    #[test]
    fn test_parse_raw_spot_microsecond_divided() {
        // Spot microsecond timestamp (> 1e15) must be divided by 1000.
        let csv = "100,50000.00,1.0,50000.00,1704067200000000,False,True\n";
        let trades = parse_raw_trades(csv.as_bytes(), false, true).unwrap();
        assert_eq!(trades.len(), 1);
        // 1704067200000000 µs → 1704067200000 ms
        assert_eq!(
            trades[0].time,
            DateTime::from_timestamp_millis(1704067200000).unwrap()
        );
        assert_eq!(trades[0].id, "100");
        assert_eq!(trades[0].side, Side::Buy); // isBuyerMaker=False → Buy
    }

    #[test]
    fn test_parse_raw_empty_csv_returns_empty_vec() {
        let trades = parse_raw_trades(b"", false, false).unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn test_parse_raw_invalid_price_returns_error() {
        let csv = "1,not_a_number,1.0,1.0,1704067200000,false,true\n";
        let result = parse_raw_trades(csv.as_bytes(), false, false);
        assert!(result.is_err());
    }
}
