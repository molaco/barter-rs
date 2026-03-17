pub mod checksum;

use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
    trade::RestTrade,
};
use chrono::NaiveDate;
use std::future::Future;
use std::pin::Pin;

/// Date-range request for bulk trade archives.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkTradeRequest {
    /// Exchange-specific symbol (e.g., "BTCUSDT").
    pub market: String,
    /// Start date, inclusive.
    pub start: NaiveDate,
    /// End date, inclusive.
    pub end: NaiveDate,
}

/// Date-range request for bulk kline archives.
#[derive(Clone, Debug, PartialEq)]
pub struct BulkKlineRequest {
    /// Exchange-specific symbol (e.g., "BTCUSDT").
    pub market: String,
    /// Candlestick interval period.
    pub interval: Interval,
    /// Start date, inclusive.
    pub start: NaiveDate,
    /// End date, inclusive.
    pub end: NaiveDate,
}

/// Configuration for bulk downloads.
#[derive(Debug, Clone)]
pub struct BulkConfig {
    /// Whether to verify checksums when available.
    pub verify_checksum: bool,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            verify_checksum: true,
        }
    }
}

/// Fetch a single day's trades from a bulk archive.
///
/// Returns `Ok(Some(trades))` on success, `Ok(None)` if the date is not
/// available (e.g. HTTP 404), or `Err` on failure.
pub trait BulkDayTradeFetcher: Send + Sync {
    fn fetch_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<RestTrade>>, DataError>> + Send + 'a>>;
}

/// Fetch a single day's klines from a bulk archive.
///
/// Returns `Ok(Some(candles))` on success, `Ok(None)` if the date is not
/// available (e.g. HTTP 404), or `Err` on failure.
pub trait BulkDayKlineFetcher: Send + Sync {
    fn fetch_day_klines<'a>(
        &'a self,
        market: &'a str,
        interval: Interval,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<Candle>>, DataError>> + Send + 'a>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bulk_config_defaults() {
        let config = BulkConfig::default();
        assert!(config.verify_checksum);
    }
}
