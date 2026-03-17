pub mod checksum;

use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
    trade::RestTrade,
};
use chrono::NaiveDate;
use std::future::Future;
use std::path::PathBuf;
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

/// Configuration for concurrent bulk downloads.
#[derive(Debug, Clone)]
pub struct BulkConfig {
    /// Maximum number of concurrent downloads.
    pub concurrency: usize,
    /// Whether to verify checksums when available.
    pub verify_checksum: bool,
    /// Optional directory for caching downloaded files and `.verified` markers.
    /// When set, successfully verified downloads write a marker file so
    /// subsequent runs can skip re-downloading.
    pub cache_dir: Option<PathBuf>,
}

impl Default for BulkConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            verify_checksum: true,
            cache_dir: None,
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

/// Generate an inclusive sequence of dates from `start` to `end`.
///
/// Note: canonical implementation is in `barter-collector::scheduling`.
/// This is a temporary re-implementation kept until `stream_bulk_*` methods
/// move to the collector crate.
pub fn date_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current = start;
    while current <= end {
        dates.push(current);
        current = current.succ_opt().unwrap_or(current);
        if current == start {
            // Overflow protection
            break;
        }
    }
    dates
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_date_range_single_day() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let dates = date_range(d, d);
        assert_eq!(dates, vec![d]);
    }

    #[test]
    fn test_date_range_multiple_days() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 3).unwrap();
        let dates = date_range(start, end);
        assert_eq!(
            dates,
            vec![
                NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
                NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
            ]
        );
    }

    #[test]
    fn test_date_range_empty_when_start_after_end() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let dates = date_range(start, end);
        assert!(dates.is_empty());
    }

    #[test]
    fn test_bulk_config_defaults() {
        let config = BulkConfig::default();
        assert_eq!(config.concurrency, 4);
        assert!(config.verify_checksum);
    }
}
