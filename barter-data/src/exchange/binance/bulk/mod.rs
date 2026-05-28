pub mod klines;
pub mod trades;

use crate::{
    bulk::{
        BulkConfig, BulkDayKlineFetcher, BulkDayTradeFetcher,
        checksum::{parse_binance_checksum, verify_sha256},
        streaming::{
            DEFAULT_BATCH_SIZE, parse_zip_csv_into_sender, parse_zip_csv_stream,
            response_to_async_read,
        },
    },
    error::DataError,
    exchange::binance::{
        binance_interval, futures::BinanceServerFuturesUsd, spot::BinanceServerSpot,
    },
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    subscription::candle::{Candle, Interval},
    trade::RestTrade,
};
use chrono::NaiveDate;
use futures::StreamExt;
use std::{
    future::Future,
    io::Cursor,
    marker::PhantomData,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};

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
#[non_exhaustive]
#[derive(Debug)]
pub struct BinanceBulkClient<Server> {
    pub(crate) client: reqwest::Client,
    pub(crate) config: BulkConfig,
    _server: PhantomData<Server>,
}

impl<Server> BinanceBulkClient<Server> {
    /// Return a reference to the bulk download configuration.
    pub fn config(&self) -> &BulkConfig {
        &self.config
    }

    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            config: BulkConfig::default(),
            _server: PhantomData,
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
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

/// Minimum file size (1 MB) below which range resume is not attempted.
const RANGE_RESUME_THRESHOLD: usize = 1_048_576;

/// Download bytes from the given URL with retry and HTTP Range resume support.
///
/// On retry after a partial download (>= 1 MB received), sends a
/// `Range: bytes=N-` header to resume where the connection dropped.
/// If the server responds with `200` instead of `206`, the partial buffer
/// is discarded and the full download is accepted. A `416` response
/// triggers a full restart from scratch.
///
/// Uses an owned `Vec<u8>` buffer across a manual retry loop — no
/// `Arc<Mutex<>>` needed since retries are sequential.
///
/// Returns `Ok(None)` on 404.
async fn download_bytes_resumable(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    url: &str,
) -> Result<Option<Vec<u8>>, DataError> {
    use crate::retry::apply_backoff;

    let mut partial = Vec::new();
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        let existing_len = partial.len();

        // Build request, optionally with Range header for resume.
        let mut request = client.get(url);
        if existing_len >= RANGE_RESUME_THRESHOLD {
            request = request.header("Range", format!("bytes={existing_len}-"));
            tracing::debug!(
                url = %url,
                resume_from = existing_len,
                "resuming download with Range header"
            );
        }

        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                let err = DataError::Http {
                    status: None,
                    url: url.to_string(),
                    message: format!("request failed: {e}"),
                };
                if !is_retriable_data_error(&err) || attempts > retry.max_retries {
                    return Err(err);
                }
                tracing::warn!(
                    attempt = attempts,
                    url = %url,
                    %err,
                    "retriable download error, will retry"
                );
                apply_backoff(retry, attempts).await;
                continue;
            }
        };

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        // 416 Range Not Satisfiable: file changed or already complete, restart.
        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            tracing::debug!(url = %url, "got 416, restarting download from scratch");
            partial.clear();
            if attempts > retry.max_retries {
                return Err(DataError::Http {
                    status: Some(416),
                    url: url.to_string(),
                    message: "Range Not Satisfiable".into(),
                });
            }
            apply_backoff(retry, attempts).await;
            continue;
        }

        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            let err = DataError::Http {
                status: Some(status.as_u16()),
                url: url.to_string(),
                message: format!("HTTP {status}"),
            };
            if !is_retriable_data_error(&err) || attempts > retry.max_retries {
                return Err(err);
            }
            tracing::warn!(
                attempt = attempts,
                url = %url,
                %err,
                "retriable HTTP status, will retry"
            );
            apply_backoff(retry, attempts).await;
            continue;
        }

        if status == reqwest::StatusCode::OK && existing_len > 0 {
            // Server does not support Range: discard partial, accept full body.
            tracing::debug!(
                url = %url,
                "server returned 200 (no range support), restarting"
            );
            partial.clear();
        }

        // Stream the response body into the partial buffer.
        let mut byte_stream = response.bytes_stream();
        let mut stream_err = None;
        while let Some(chunk_result) = byte_stream.next().await {
            match chunk_result {
                Ok(chunk) => partial.extend_from_slice(&chunk),
                Err(e) => {
                    stream_err = Some(e);
                    break;
                }
            }
        }

        if let Some(e) = stream_err {
            let err = DataError::Http {
                status: None,
                url: url.to_string(),
                message: format!("body read failed: {e}"),
            };
            if !is_retriable_data_error(&err) || attempts > retry.max_retries {
                return Err(err);
            }
            // Partial buffer retained for Range resume on next attempt.
            tracing::warn!(
                attempt = attempts,
                url = %url,
                partial_bytes = partial.len(),
                "stream error, will retry with partial buffer"
            );
            apply_backoff(retry, attempts).await;
            continue;
        }

        return Ok(Some(std::mem::take(&mut partial)));
    }
}

/// Download and parse a `.CHECKSUM` file. Returns `Ok(None)` on 404.
async fn download_checksum(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    url: &str,
) -> Result<Option<String>, DataError> {
    let checksum_url = format!("{url}.CHECKSUM");
    let bytes = download_bytes_resumable(client, retry, &checksum_url).await?;
    match bytes {
        None => Ok(None),
        Some(data) => {
            let content = String::from_utf8(data)
                .map_err(|e| DataError::DataParse(format!("checksum file is not UTF-8: {e}")))?;
            let checksum = parse_binance_checksum(&content)?;
            Ok(Some(checksum))
        }
    }
}

/// Maximum allowed uncompressed archive entry size (2 GiB).
const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Extract the first file from a ZIP archive into raw bytes.
fn extract_zip_csv(zip_bytes: &[u8]) -> Result<Vec<u8>, DataError> {
    let cursor = Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| DataError::BulkArchive(format!("failed to open ZIP archive: {e}")))?;

    if archive.is_empty() {
        return Err(DataError::BulkArchive(
            "ZIP archive contains no files".to_string(),
        ));
    }

    let file = archive
        .by_index(0)
        .map_err(|e| DataError::BulkArchive(format!("failed to read first ZIP entry: {e}")))?;

    if file.size() > MAX_DECOMPRESSED_SIZE {
        return Err(DataError::BulkArchive(format!(
            "ZIP entry too large: {} bytes (max {MAX_DECOMPRESSED_SIZE} bytes)",
            file.size(),
        )));
    }

    let mut buf = Vec::with_capacity(file.size() as usize);
    std::io::Read::read_to_end(&mut { file }, &mut buf)
        .map_err(|e| DataError::BulkArchive(format!("failed to decompress ZIP entry: {e}")))?;

    Ok(buf)
}

// ---------------------------------------------------------------------------
// Download-and-parse orchestration
// ---------------------------------------------------------------------------

/// Download and parse trades for a single date using streaming ZIP+CSV.
///
/// The HTTP response body is streamed through `async_zip` for decompression
/// and then into `csv-async` for record-by-record parsing. No intermediate
/// ZIP or CSV buffers are allocated — peak memory is just the final
/// `Vec<RestTrade>`.
///
/// Returns `Ok(None)` on 404.
async fn download_and_parse_trades<Server: BulkArchiveServer>(
    client: &reqwest::Client,
    config: &BulkConfig,
    market: &str,
    date: NaiveDate,
) -> Result<Option<Vec<RestTrade>>, DataError> {
    static CHECKSUM_SKIP_WARNED: AtomicBool = AtomicBool::new(false);

    let url = agg_trades_url(Server::market_segment(), market, date);

    if config.verify_checksum && !CHECKSUM_SKIP_WARNED.swap(true, Ordering::Relaxed) {
        tracing::warn!(
            "checksum verification is not supported in streaming mode, skipping for all downloads"
        );
    }

    let policy = RetryPolicy::default();
    retry_with_backoff(&policy, is_retriable_data_error, || async {
        let resp = client.get(&url).send().await.map_err(|e| DataError::Http {
            status: None,
            url: url.clone(),
            message: format!("request failed: {e}"),
        })?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(DataError::Http {
                status: Some(status.as_u16()),
                url: url.clone(),
                message: format!("HTTP {status}"),
            });
        }

        let buf_reader = response_to_async_read(resp);
        let trades = parse_zip_csv_stream::<_, trades::BinanceBulkAggTrade, _>(
            buf_reader,
            Server::csv_has_headers(),
            true,
            |r| trades::convert_trade(r, Server::may_use_microsecond_timestamps()),
        )
        .await?;

        Ok(Some(trades))
    })
    .await
}

/// Download, verify, and parse klines for a single date.
/// Returns `Ok(None)` on 404.
async fn download_and_parse_klines<Server: BulkArchiveServer>(
    client: &reqwest::Client,
    config: &BulkConfig,
    market: &str,
    interval: &str,
    date: NaiveDate,
) -> Result<Option<Vec<Candle>>, DataError> {
    let url = klines_url(Server::market_segment(), market, interval, date);

    let zip_bytes = match download_and_verify(client, config, &url).await? {
        Some(b) => b,
        None => return Ok(None),
    };

    let csv_data = tokio::task::spawn_blocking(move || extract_zip_csv(&zip_bytes))
        .await
        .map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")))??;
    let candles = klines::parse_klines(&csv_data, Server::csv_has_headers())?;

    Ok(Some(candles))
}

/// Download a ZIP archive with resumable retry and verify its SHA-256 checksum.
///
/// Returns `Ok(None)` on 404.
async fn download_and_verify(
    client: &reqwest::Client,
    config: &BulkConfig,
    url: &str,
) -> Result<Option<Vec<u8>>, DataError> {
    let retry = RetryPolicy::default();

    let zip_bytes = match download_bytes_resumable(client, &retry, url).await? {
        Some(b) => b,
        None => return Ok(None),
    };

    if config.verify_checksum {
        if let Some(expected) = download_checksum(client, &retry, url).await? {
            verify_sha256(&zip_bytes, &expected)?;
        }
    }

    Ok(Some(zip_bytes))
}

// ---------------------------------------------------------------------------
// BulkDayTradeFetcher / BulkDayKlineFetcher trait implementations
// ---------------------------------------------------------------------------

impl<Server: BulkArchiveServer> BulkDayTradeFetcher for BinanceBulkClient<Server> {
    #[tracing::instrument(skip(self), fields(exchange = "binance", %date))]
    fn fetch_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<RestTrade>>, DataError>> + Send + 'a>> {
        Box::pin(download_and_parse_trades::<Server>(
            &self.client,
            &self.config,
            market,
            date,
        ))
    }

    fn stream_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
        tx: &'a tokio::sync::mpsc::Sender<Vec<RestTrade>>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DataError>> + Send + 'a>> {
        static CHECKSUM_SKIP_WARNED: AtomicBool = AtomicBool::new(false);

        let url = agg_trades_url(Server::market_segment(), market, date);

        if self.config.verify_checksum && !CHECKSUM_SKIP_WARNED.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "checksum verification is not supported in streaming mode, skipping for all downloads"
            );
        }

        let policy = RetryPolicy::default();
        Box::pin(async move {
            retry_with_backoff(&policy, is_retriable_data_error, || async {
                let resp = self
                    .client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| DataError::Http {
                        status: None,
                        url: url.clone(),
                        message: format!("request failed: {e}"),
                    })?;

                let status = resp.status();
                if status == reqwest::StatusCode::NOT_FOUND {
                    return Ok(false);
                }
                if !status.is_success() {
                    return Err(DataError::Http {
                        status: Some(status.as_u16()),
                        url: url.clone(),
                        message: format!("HTTP {status}"),
                    });
                }

                let buf_reader = response_to_async_read(resp);
                parse_zip_csv_into_sender::<_, trades::BinanceBulkAggTrade, _>(
                    buf_reader,
                    Server::csv_has_headers(),
                    true,
                    |r| trades::convert_trade(r, Server::may_use_microsecond_timestamps()),
                    tx,
                    DEFAULT_BATCH_SIZE,
                )
                .await?;

                Ok(true)
            })
            .await
        })
    }
}

impl<Server: BulkArchiveServer> BulkDayKlineFetcher for BinanceBulkClient<Server> {
    #[tracing::instrument(skip(self), fields(exchange = "binance", %date, %interval))]
    fn fetch_day_klines<'a>(
        &'a self,
        market: &'a str,
        interval: Interval,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<Candle>>, DataError>> + Send + 'a>> {
        let bi = binance_interval(interval);
        Box::pin(download_and_parse_klines::<Server>(
            &self.client,
            &self.config,
            market,
            bi,
            date,
        ))
    }
}

// ---------------------------------------------------------------------------
// S3 listing for available-date discovery
// ---------------------------------------------------------------------------

/// S3 listing base URL for Binance data archives.
const S3_LISTING_BASE: &str = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";

/// List available daily trade ZIP files from Binance's S3 bucket.
///
/// Uses the S3 XML listing API at:
/// `https://s3-ap-northeast-1.amazonaws.com/data.binance.vision?prefix=data/{market_type}/daily/trades/{symbol}/&delimiter=/`
///
/// Returns a sorted `Vec<NaiveDate>` for which ZIP files exist.
///
/// `market_type` should be `"spot"`, `"futures/um"`, or `"futures/cm"`.
pub async fn list_available_dates(
    client: &reqwest::Client,
    symbol: &str,
    market_type: &str,
) -> Result<Vec<NaiveDate>, DataError> {
    let prefix = format!("data/{market_type}/daily/trades/{symbol}/");
    let mut dates = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let mut url = format!("{S3_LISTING_BASE}?prefix={prefix}&delimiter=/");
        if let Some(ref m) = marker {
            url.push_str(&format!("&marker={m}"));
        }

        let body = client
            .get(&url)
            .send()
            .await
            .map_err(|e| DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("S3 listing failed: {e}"),
            })?
            .text()
            .await
            .map_err(|e| DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("S3 listing body read failed: {e}"),
            })?;

        let (page_dates, is_truncated, next_marker) = parse_s3_listing_xml(&body, symbol)?;
        dates.extend(page_dates);

        if !is_truncated {
            break;
        }

        marker = next_marker.or_else(|| {
            dates.last().map(|d| {
                // Fallback marker: use the last key we saw
                format!("{prefix}{symbol}-trades-{}.zip", d.format("%Y-%m-%d"))
            })
        });

        if marker.is_none() {
            break;
        }
    }

    dates.sort();
    dates.dedup();
    Ok(dates)
}

/// Parse an S3 XML listing response and extract dates from `<Key>` elements.
///
/// Returns `(dates, is_truncated, next_marker)`.
fn parse_s3_listing_xml(
    xml: &str,
    symbol: &str,
) -> Result<(Vec<NaiveDate>, bool, Option<String>), DataError> {
    use quick_xml::{Reader, events::Event};

    let mut reader = Reader::from_str(xml);
    let mut dates = Vec::new();
    let mut is_truncated = false;
    let mut next_marker: Option<String> = None;
    let mut current_element = String::new();
    let mut buf = Vec::new();

    // Pattern: {SYMBOL}-trades-{YYYY-MM-DD}.zip
    let suffix_pattern = format!("{symbol}-trades-");

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
            }
            Ok(Event::Text(ref e)) => {
                let text = e
                    .unescape()
                    .map_err(|err| DataError::DataParse(format!("S3 XML unescape error: {err}")))?;
                match current_element.as_str() {
                    "Key" => {
                        if let Some(date) = extract_date_from_key(&text, &suffix_pattern) {
                            dates.push(date);
                        }
                    }
                    "IsTruncated" => {
                        is_truncated = text.trim() == "true";
                    }
                    "NextMarker" => {
                        let t = text.trim().to_string();
                        if !t.is_empty() {
                            next_marker = Some(t);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::End(_)) => {
                current_element.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(DataError::DataParse(format!("S3 XML parse error: {e}")));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok((dates, is_truncated, next_marker))
}

/// Extract a `NaiveDate` from an S3 key like
/// `data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip`.
fn extract_date_from_key(key: &str, suffix_pattern: &str) -> Option<NaiveDate> {
    // Find the filename portion after the last '/'
    let filename = key.rsplit('/').next()?;
    // Strip the prefix pattern (e.g. "BTCUSDT-trades-")
    let rest = filename.strip_prefix(suffix_pattern)?;
    // Strip the ".zip" suffix
    let date_str = rest.strip_suffix(".zip")?;
    // Parse the date
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
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
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_bulk_client_with_config() {
        let config = BulkConfig {
            verify_checksum: false,
        };
        let client: BinanceBulkClient<BinanceServerSpot> = BinanceBulkClient::with_config(config);
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

    // -----------------------------------------------------------------------
    // S3 listing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_date_from_key_valid() {
        let pattern = "BTCUSDT-trades-";
        let key = "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip";
        let date = extract_date_from_key(key, pattern);
        assert_eq!(date, Some(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()));
    }

    #[test]
    fn test_extract_date_from_key_wrong_symbol() {
        let pattern = "BTCUSDT-trades-";
        let key = "data/spot/daily/trades/ETHUSDT/ETHUSDT-trades-2024-01-15.zip";
        let date = extract_date_from_key(key, pattern);
        assert_eq!(date, None);
    }

    #[test]
    fn test_extract_date_from_key_no_zip_suffix() {
        let pattern = "BTCUSDT-trades-";
        let key = "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.csv";
        let date = extract_date_from_key(key, pattern);
        assert_eq!(date, None);
    }

    #[test]
    fn test_extract_date_from_key_invalid_date() {
        let pattern = "BTCUSDT-trades-";
        let key = "data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-not-a-date.zip";
        let date = extract_date_from_key(key, pattern);
        assert_eq!(date, None);
    }

    #[test]
    fn test_parse_s3_listing_xml_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>data.binance.vision</Name>
  <Prefix>data/spot/daily/trades/BTCUSDT/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip</Key>
    <Size>123456</Size>
  </Contents>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-16.zip</Key>
    <Size>234567</Size>
  </Contents>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-17.zip</Key>
    <Size>345678</Size>
  </Contents>
</ListBucketResult>"#;

        let (dates, is_truncated, next_marker) = parse_s3_listing_xml(xml, "BTCUSDT").unwrap();
        assert_eq!(dates.len(), 3);
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(dates[1], NaiveDate::from_ymd_opt(2024, 1, 16).unwrap());
        assert_eq!(dates[2], NaiveDate::from_ymd_opt(2024, 1, 17).unwrap());
        assert!(!is_truncated);
        assert!(next_marker.is_none());
    }

    #[test]
    fn test_parse_s3_listing_xml_truncated_with_marker() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>data.binance.vision</Name>
  <Prefix>data/spot/daily/trades/BTCUSDT/</Prefix>
  <IsTruncated>true</IsTruncated>
  <NextMarker>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip</NextMarker>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-14.zip</Key>
    <Size>111111</Size>
  </Contents>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip</Key>
    <Size>222222</Size>
  </Contents>
</ListBucketResult>"#;

        let (dates, is_truncated, next_marker) = parse_s3_listing_xml(xml, "BTCUSDT").unwrap();
        assert_eq!(dates.len(), 2);
        assert!(is_truncated);
        assert_eq!(
            next_marker,
            Some("data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip".to_string())
        );
    }

    #[test]
    fn test_parse_s3_listing_xml_empty() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>data.binance.vision</Name>
  <Prefix>data/spot/daily/trades/UNKNOWN/</Prefix>
  <IsTruncated>false</IsTruncated>
</ListBucketResult>"#;

        let (dates, is_truncated, _) = parse_s3_listing_xml(xml, "UNKNOWN").unwrap();
        assert!(dates.is_empty());
        assert!(!is_truncated);
    }

    #[test]
    fn test_parse_s3_listing_xml_ignores_checksum_files() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>data.binance.vision</Name>
  <Prefix>data/spot/daily/trades/BTCUSDT/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip</Key>
    <Size>123456</Size>
  </Contents>
  <Contents>
    <Key>data/spot/daily/trades/BTCUSDT/BTCUSDT-trades-2024-01-15.zip.CHECKSUM</Key>
    <Size>64</Size>
  </Contents>
</ListBucketResult>"#;

        let (dates, _, _) = parse_s3_listing_xml(xml, "BTCUSDT").unwrap();
        // Only the .zip should be extracted, not the .CHECKSUM
        assert_eq!(dates.len(), 1);
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
    }

    #[test]
    fn test_parse_s3_listing_xml_futures_path() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>data.binance.vision</Name>
  <Prefix>data/futures/um/daily/trades/ETHUSDT/</Prefix>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>data/futures/um/daily/trades/ETHUSDT/ETHUSDT-trades-2024-06-01.zip</Key>
    <Size>999999</Size>
  </Contents>
</ListBucketResult>"#;

        let (dates, _, _) = parse_s3_listing_xml(xml, "ETHUSDT").unwrap();
        assert_eq!(dates.len(), 1);
        assert_eq!(dates[0], NaiveDate::from_ymd_opt(2024, 6, 1).unwrap());
    }
}
