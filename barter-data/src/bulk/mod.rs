pub mod checksum;
pub(crate) mod streaming;

use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
    trade::RestTrade,
};
use chrono::NaiveDate;
use std::{future::Future, pin::Pin};

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
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct BulkConfig {
    /// Whether to verify checksums when available.
    pub verify_checksum: bool,
}

impl BulkConfig {
    pub fn new(verify_checksum: bool) -> Self {
        Self { verify_checksum }
    }
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
    /// Fetch all trades for a single day, collected into a `Vec`.
    fn fetch_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<RestTrade>>, DataError>> + Send + 'a>>;

    /// Stream trades for a single day through a channel in batches.
    ///
    /// Returns `Ok(true)` if data was found, `Ok(false)` if 404, `Err` on
    /// failure. The default implementation calls [`Self::fetch_day_trades`]
    /// and sends the entire `Vec` as one batch.
    ///
    /// Implementations should override this to send smaller batches as
    /// records are parsed, keeping memory bounded regardless of day size.
    fn stream_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
        tx: &'a tokio::sync::mpsc::Sender<Vec<RestTrade>>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DataError>> + Send + 'a>> {
        Box::pin(async move {
            match self.fetch_day_trades(market, date).await? {
                Some(trades) => {
                    if !trades.is_empty() {
                        tx.send(trades)
                            .await
                            .map_err(|_| DataError::BulkArchive("receiver dropped".into()))?;
                    }
                    Ok(true)
                }
                None => Ok(false),
            }
        })
    }
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
