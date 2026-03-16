pub mod checksum;

use barter_instrument::exchange::ExchangeId;
use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
    trade::RestTrade,
};
use chrono::NaiveDate;
use futures::Stream;
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

/// Stream daily batches of trades from archive files.
///
/// Each `Ok(Vec<RestTrade>)` represents one day's trades in chronological order.
/// HTTP 404 dates are silently skipped. 5xx/429 errors are retried.
pub trait BulkTradeFetcher: Send + Sync {
    fn stream_bulk_trades(
        &self,
        request: BulkTradeRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<RestTrade>, DataError>> + Send + '_>>;
}

/// Stream daily batches of klines from archive files.
///
/// Only Binance has kline archives.
pub trait BulkKlineFetcher {
    fn stream_bulk_klines(
        &self,
        request: BulkKlineRequest,
    ) -> impl Stream<Item = Result<Vec<Candle>, DataError>> + Send;
}

/// Generate an inclusive sequence of dates from `start` to `end`.
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

/// Create a boxed bulk trade client for the given exchange.
///
/// Returns `None` if the exchange does not support bulk archive downloading.
#[cfg(feature = "bulk")]
pub fn bulk_client(exchange_id: ExchangeId, config: BulkConfig) -> Option<Box<dyn BulkTradeFetcher>> {
    use crate::exchange::{
        binance::{
            bulk::{BinanceBulkClient, BinanceServerFuturesCoin},
            futures::BinanceServerFuturesUsd,
            spot::BinanceServerSpot,
        },
        bybit::{bulk::BybitBulkClient, futures::BybitServerPerpetualsUsd, spot::BybitServerSpot},
        hyperliquid::bulk::HyperliquidBulkClient,
        okx::bulk::OkxBulkClient,
    };
    match exchange_id {
        ExchangeId::BinanceSpot => Some(Box::new(BinanceBulkClient::<BinanceServerSpot>::with_config(config))),
        ExchangeId::BinanceFuturesUsd => Some(Box::new(BinanceBulkClient::<BinanceServerFuturesUsd>::with_config(config))),
        ExchangeId::BinanceFuturesCoin => Some(Box::new(BinanceBulkClient::<BinanceServerFuturesCoin>::with_config(config))),
        ExchangeId::BybitSpot => Some(Box::new(BybitBulkClient::<BybitServerSpot>::with_config(config))),
        ExchangeId::BybitPerpetualsUsd => Some(Box::new(BybitBulkClient::<BybitServerPerpetualsUsd>::with_config(config))),
        ExchangeId::OkxSpot | ExchangeId::OkxPerpetualsUsd => Some(Box::new(OkxBulkClient::with_config(config))),
        ExchangeId::Hyperliquid => Some(Box::new(HyperliquidBulkClient::with_config(config))),
        _ => None,
    }
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
