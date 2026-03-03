pub mod klines;
pub mod trades;

use crate::{
    bulk::{
        BulkConfig, BulkKlineFetcher, BulkKlineRequest, BulkTradeFetcher, BulkTradeRequest,
        checksum::{parse_binance_checksum, verify_sha256},
        date_range,
    },
    error::DataError,
    exchange::binance::{binance_interval, futures::BinanceServerFuturesUsd, spot::BinanceServerSpot},
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    subscription::candle::Candle,
    trade::RestTrade,
};
use chrono::NaiveDate;
use futures::stream::{self, StreamExt};
use futures::Stream;
use std::{io::Cursor, marker::PhantomData};

/// Trait for Binance bulk archive server variants.
///
/// Implemented by exchange server types that support bulk data.binance.vision archives.
pub trait BulkArchiveServer: Send + Sync + 'static {
    /// Market segment for URL construction: `"spot"`, `"futures/um"`, or `"futures/cm"`.
    fn market_segment() -> &'static str;

    /// Whether CSV files have a header row (spot = false, futures = true).
    fn csv_has_headers() -> bool;

    /// Whether timestamps may use microseconds (spot = true from Jan 2025, futures = false).
    fn may_use_microsecond_timestamps() -> bool;
}

impl BulkArchiveServer for BinanceServerSpot {
    fn market_segment() -> &'static str {
        "spot"
    }
    fn csv_has_headers() -> bool {
        false
    }
    fn may_use_microsecond_timestamps() -> bool {
        true
    }
}

impl BulkArchiveServer for BinanceServerFuturesUsd {
    fn market_segment() -> &'static str {
        "futures/um"
    }
    fn csv_has_headers() -> bool {
        true
    }
    fn may_use_microsecond_timestamps() -> bool {
        false
    }
}

/// Binance COIN-M futures server variant for bulk archives.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerFuturesCoin;

impl BulkArchiveServer for BinanceServerFuturesCoin {
    fn market_segment() -> &'static str {
        "futures/cm"
    }
    fn csv_has_headers() -> bool {
        true
    }
    fn may_use_microsecond_timestamps() -> bool {
        false
    }
}

/// Binance bulk archive download client, generic over server variant.
#[derive(Debug)]
pub struct BinanceBulkClient<Server> {
    pub client: reqwest::Client,
    pub config: BulkConfig,
    pub retry: RetryPolicy,
    _server: PhantomData<Server>,
}

impl<Server> BinanceBulkClient<Server> {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            config: BulkConfig::default(),
            retry: RetryPolicy::default(),
            _server: PhantomData,
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
            retry: RetryPolicy::default(),
            _server: PhantomData,
        }
    }
}

impl<Server> Default for BinanceBulkClient<Server> {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// URL construction
// ---------------------------------------------------------------------------

fn agg_trades_url(segment: &str, market: &str, date: NaiveDate) -> String {
    format!(
        "https://data.binance.vision/data/{segment}/daily/aggTrades/{market}/{market}-aggTrades-{date}.zip",
        segment = segment,
        market = market,
        date = date.format("%Y-%m-%d"),
    )
}

fn klines_url(segment: &str, market: &str, interval: &str, date: NaiveDate) -> String {
    format!(
        "https://data.binance.vision/data/{segment}/daily/klines/{market}/{interval}/{market}-{interval}-{date}.zip",
        segment = segment,
        market = market,
        interval = interval,
        date = date.format("%Y-%m-%d"),
    )
}

// ---------------------------------------------------------------------------
// Download and extraction helpers
// ---------------------------------------------------------------------------

/// Download bytes from the given URL with retry. Returns `Ok(None)` on 404.
async fn download_bytes(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    url: &str,
) -> Result<Option<Vec<u8>>, DataError> {
    let url_owned = url.to_string();
    let result = retry_with_backoff(retry, is_retriable_data_error, || {
        let client = client.clone();
        let url = url_owned.clone();
        async move {
            let response = client
                .get(&url)
                .send()
                .await
                .map_err(|e| DataError::Socket(format!("HTTP request failed for {url}: {e}")))?;

            let status = response.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }
            if !status.is_success() {
                return Err(DataError::Socket(format!(
                    "HTTP {status} for {url}"
                )));
            }

            let bytes = response
                .bytes()
                .await
                .map_err(|e| DataError::Socket(format!("failed to read response body for {url}: {e}")))?;
            Ok(Some(bytes.to_vec()))
        }
    })
    .await?;

    Ok(result)
}

/// Download and parse a `.CHECKSUM` file. Returns `Ok(None)` on 404.
async fn download_checksum(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    url: &str,
) -> Result<Option<String>, DataError> {
    let checksum_url = format!("{url}.CHECKSUM");
    let bytes = download_bytes(client, retry, &checksum_url).await?;
    match bytes {
        None => Ok(None),
        Some(data) => {
            let content = String::from_utf8(data)
                .map_err(|e| DataError::Socket(format!("checksum file is not UTF-8: {e}")))?;
            let checksum = parse_binance_checksum(&content)?;
            Ok(Some(checksum))
        }
    }
}

/// Extract the first file from a ZIP archive into raw bytes.
fn extract_zip_csv(zip_bytes: &[u8]) -> Result<Vec<u8>, DataError> {
    let cursor = Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| DataError::Socket(format!("failed to open ZIP archive: {e}")))?;

    if archive.is_empty() {
        return Err(DataError::Socket(
            "ZIP archive contains no files".to_string(),
        ));
    }

    let mut file = archive
        .by_index(0)
        .map_err(|e| DataError::Socket(format!("failed to read first ZIP entry: {e}")))?;

    let mut buf = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut buf)
        .map_err(|e| DataError::Socket(format!("failed to decompress ZIP entry: {e}")))?;

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Download-and-parse orchestration
// ---------------------------------------------------------------------------

/// Download, verify, and parse trades for a single date. Returns `Ok(None)` on 404.
async fn download_and_parse_trades<Server: BulkArchiveServer>(
    client: &reqwest::Client,
    config: &BulkConfig,
    retry: &RetryPolicy,
    market: &str,
    date: NaiveDate,
) -> Result<Option<Vec<RestTrade>>, DataError> {
    let url = agg_trades_url(Server::market_segment(), market, date);

    let zip_bytes = match download_bytes(client, retry, &url).await? {
        Some(b) => b,
        None => return Ok(None),
    };

    if config.verify_checksum {
        if let Some(expected) = download_checksum(client, retry, &url).await? {
            verify_sha256(&zip_bytes, &expected)?;
        }
    }

    let csv_data = extract_zip_csv(&zip_bytes)?;
    let trades = trades::parse_trades(
        &csv_data,
        Server::csv_has_headers(),
        Server::may_use_microsecond_timestamps(),
    )?;

    Ok(Some(trades))
}

/// Download, verify, and parse klines for a single date. Returns `Ok(None)` on 404.
async fn download_and_parse_klines<Server: BulkArchiveServer>(
    client: &reqwest::Client,
    config: &BulkConfig,
    retry: &RetryPolicy,
    market: &str,
    interval: &str,
    date: NaiveDate,
) -> Result<Option<Vec<Candle>>, DataError> {
    let url = klines_url(Server::market_segment(), market, interval, date);

    let zip_bytes = match download_bytes(client, retry, &url).await? {
        Some(b) => b,
        None => return Ok(None),
    };

    if config.verify_checksum {
        if let Some(expected) = download_checksum(client, retry, &url).await? {
            verify_sha256(&zip_bytes, &expected)?;
        }
    }

    let csv_data = extract_zip_csv(&zip_bytes)?;
    let candles = klines::parse_klines(&csv_data, Server::csv_has_headers())?;

    Ok(Some(candles))
}

// ---------------------------------------------------------------------------
// BulkTradeFetcher / BulkKlineFetcher implementations
// ---------------------------------------------------------------------------

impl<Server: BulkArchiveServer> BulkTradeFetcher for BinanceBulkClient<Server> {
    fn stream_bulk_trades(
        &self,
        request: BulkTradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
        let dates = date_range(request.start, request.end);
        let client = self.client.clone();
        let config = self.config.clone();
        let concurrency = self.config.concurrency;
        let retry = self.retry.clone();
        let market = request.market;

        stream::iter(dates)
            .map(move |date| {
                let client = client.clone();
                let config = config.clone();
                let retry = retry.clone();
                let market = market.clone();
                async move {
                    download_and_parse_trades::<Server>(&client, &config, &retry, &market, date)
                        .await
                }
            })
            .buffer_unordered(concurrency)
            .filter_map(|result| async {
                match result {
                    Ok(None) => None,
                    Ok(Some(trades)) => Some(Ok(trades)),
                    Err(e) => Some(Err(e)),
                }
            })
    }
}

impl<Server: BulkArchiveServer> BulkKlineFetcher for BinanceBulkClient<Server> {
    fn stream_bulk_klines(
        &self,
        request: BulkKlineRequest,
    ) -> impl Stream<Item = Result<Vec<Candle>, DataError>> + Send {
        let dates = date_range(request.start, request.end);
        let client = self.client.clone();
        let config = self.config.clone();
        let concurrency = self.config.concurrency;
        let retry = self.retry.clone();
        let market = request.market;
        let interval = binance_interval(request.interval);

        stream::iter(dates)
            .map(move |date| {
                let client = client.clone();
                let config = config.clone();
                let retry = retry.clone();
                let market = market.clone();
                async move {
                    download_and_parse_klines::<Server>(
                        &client, &config, &retry, &market, interval, date,
                    )
                    .await
                }
            })
            .buffer_unordered(concurrency)
            .filter_map(|result| async {
                match result {
                    Ok(None) => None,
                    Ok(Some(candles)) => Some(Ok(candles)),
                    Err(e) => Some(Err(e)),
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::binance::spot::BinanceServerSpot;

    #[test]
    fn test_agg_trades_url_spot() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let url = agg_trades_url("spot", "BTCUSDT", date);
        assert_eq!(
            url,
            "https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2024-06-15.zip"
        );
    }

    #[test]
    fn test_agg_trades_url_futures_um() {
        let date = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let url = agg_trades_url("futures/um", "ETHUSDT", date);
        assert_eq!(
            url,
            "https://data.binance.vision/data/futures/um/daily/aggTrades/ETHUSDT/ETHUSDT-aggTrades-2024-01-01.zip"
        );
    }

    #[test]
    fn test_klines_url_spot() {
        let date = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
        let url = klines_url("spot", "BTCUSDT", "1h", date);
        assert_eq!(
            url,
            "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1h/BTCUSDT-1h-2024-03-10.zip"
        );
    }

    #[test]
    fn test_klines_url_futures_cm() {
        let date = NaiveDate::from_ymd_opt(2024, 12, 31).unwrap();
        let url = klines_url("futures/cm", "BTCUSD_PERP", "1d", date);
        assert_eq!(
            url,
            "https://data.binance.vision/data/futures/cm/daily/klines/BTCUSD_PERP/1d/BTCUSD_PERP-1d-2024-12-31.zip"
        );
    }

    #[test]
    fn test_extract_zip_csv_valid() {
        // Create a minimal ZIP in memory with a single CSV file.
        let mut buf = Vec::new();
        {
            let cursor = Cursor::new(&mut buf);
            let mut writer = zip::ZipWriter::new(cursor);
            let options = zip::write::SimpleFileOptions::default()
                .compression_method(zip::CompressionMethod::Stored);
            writer.start_file("data.csv", options).unwrap();
            std::io::Write::write_all(&mut writer, b"hello,world\n").unwrap();
            writer.finish().unwrap();
        }

        let csv_data = extract_zip_csv(&buf).unwrap();
        assert_eq!(csv_data, b"hello,world\n");
    }

    #[test]
    fn test_extract_zip_csv_invalid_archive() {
        let result = extract_zip_csv(b"this is not a zip file");
        assert!(result.is_err());
    }

    #[test]
    fn test_bulk_client_default() {
        let client: BinanceBulkClient<BinanceServerSpot> = BinanceBulkClient::default();
        assert_eq!(client.config.concurrency, 4);
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_bulk_client_with_config() {
        let config = BulkConfig {
            concurrency: 8,
            verify_checksum: false,
        };
        let client: BinanceBulkClient<BinanceServerSpot> = BinanceBulkClient::with_config(config);
        assert_eq!(client.config.concurrency, 8);
        assert!(!client.config.verify_checksum);
    }

    #[test]
    fn test_bulk_archive_server_spot() {
        assert_eq!(BinanceServerSpot::market_segment(), "spot");
        assert!(!BinanceServerSpot::csv_has_headers());
        assert!(BinanceServerSpot::may_use_microsecond_timestamps());
    }

    #[test]
    fn test_bulk_archive_server_futures_usd() {
        assert_eq!(BinanceServerFuturesUsd::market_segment(), "futures/um");
        assert!(BinanceServerFuturesUsd::csv_has_headers());
        assert!(!BinanceServerFuturesUsd::may_use_microsecond_timestamps());
    }

    #[test]
    fn test_bulk_archive_server_futures_coin() {
        assert_eq!(BinanceServerFuturesCoin::market_segment(), "futures/cm");
        assert!(BinanceServerFuturesCoin::csv_has_headers());
        assert!(!BinanceServerFuturesCoin::may_use_microsecond_timestamps());
    }
}
