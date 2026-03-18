pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkDayTradeFetcher},
    error::DataError,
    trade::RestTrade,
};
use async_compression::tokio::bufread::GzipDecoder;
use chrono::NaiveDate;
use futures::TryStreamExt;
use std::{future::Future, marker::PhantomData, pin::Pin};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio_util::io::StreamReader;

use super::{futures::BybitServerPerpetualsUsd, spot::BybitServerSpot};

/// Marker trait for Bybit server variants used in bulk downloads.
pub trait BybitBulkServer: Send + Sync + 'static {
    /// URL path prefix: `"spot"` or `"trading"`.
    fn path_prefix() -> &'static str;
    /// Separator between market symbol and date in archive filenames.
    /// Spot uses `"_"` (e.g. `BTCUSDT_2025-12-09.csv.gz`),
    /// perpetuals use no separator (e.g. `BTCUSDT2025-12-09.csv.gz`).
    fn date_separator() -> &'static str;
    /// Parse CSV data into trades. Spot and perpetuals have different CSV schemas.
    fn parse_csv(data: &[u8]) -> Result<Vec<RestTrade>, DataError>;
}

impl BybitBulkServer for BybitServerSpot {
    fn path_prefix() -> &'static str {
        "spot"
    }
    fn date_separator() -> &'static str {
        "_"
    }
    fn parse_csv(data: &[u8]) -> Result<Vec<RestTrade>, DataError> {
        trades::parse_spot_trades(data)
    }
}

impl BybitBulkServer for BybitServerPerpetualsUsd {
    fn path_prefix() -> &'static str {
        "trading"
    }
    fn date_separator() -> &'static str {
        ""
    }
    fn parse_csv(data: &[u8]) -> Result<Vec<RestTrade>, DataError> {
        trades::parse_trades(data)
    }
}

/// Bulk archive download client for Bybit exchange variants.
///
/// Downloads daily GZ-compressed CSV trade archives from
/// `https://public.bybit.com/{prefix}/{SYMBOL}/{SYMBOL}{YYYY-MM-DD}.csv.gz`.
///
/// Uses streaming gzip decompression via `async_compression` to avoid loading
/// the entire compressed file into memory before decompressing.
#[non_exhaustive]
#[derive(Debug)]
pub struct BybitBulkClient<Server> {
    pub(crate) client: reqwest::Client,
    pub(crate) config: BulkConfig,
    _server: PhantomData<Server>,
}

impl<Server> BybitBulkClient<Server> {
    /// Return a reference to the bulk download configuration.
    pub fn config(&self) -> &BulkConfig {
        &self.config
    }

    fn build_client() -> reqwest::Client {
        reqwest::Client::builder()
            .user_agent("Mozilla/5.0")
            .build()
            .expect("failed to build reqwest client")
    }

    pub fn new() -> Self {
        Self {
            client: Self::build_client(),
            config: BulkConfig::default(),
            _server: PhantomData,
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        Self {
            client: Self::build_client(),
            config,
            _server: PhantomData,
        }
    }
}

impl<Server> Default for BybitBulkClient<Server> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Server: BybitBulkServer> BybitBulkClient<Server> {
    /// Download and parse trades using streaming gzip decompression.
    ///
    /// The HTTP response body is streamed through `GzipDecoder` line by line,
    /// avoiding buffering the entire compressed payload in memory.
    async fn download_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let prefix = Server::path_prefix();
        let sep = Server::date_separator();
        let date_str = date.format("%Y-%m-%d");
        let url =
            format!("https://public.bybit.com/{prefix}/{market}/{market}{sep}{date_str}.csv.gz");

        let resp =
            self.client.get(&url).send().await.map_err(|e| {
                DataError::Http {
                    status: None,
                    url: url.clone(),
                    message: format!("Bybit bulk request failed: {e}"),
                }
            })?;

        let status = resp.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }

        if !status.is_success() {
            return Err(DataError::Http {
                status: Some(status.as_u16()),
                url: url.clone(),
                message: format!("Bybit bulk HTTP {status}"),
            });
        }

        let response = resp;

        // Stream the response body through async gzip decoder.
        // This avoids loading the entire compressed file into memory before
        // decompressing -- the HTTP chunks flow through the gzip decoder as
        // they arrive over the network.
        let byte_stream = response
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        let stream_reader = StreamReader::new(byte_stream);
        let gzip_reader = GzipDecoder::new(BufReader::new(stream_reader));
        let buf_reader = BufReader::new(gzip_reader);
        let mut lines = buf_reader.lines();

        // Collect all decompressed lines into a buffer for CSV parsing.
        // We accumulate into a Vec<u8> so that the existing CSV parser
        // (which expects &[u8]) can be reused unchanged.
        const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024 * 1024;

        let mut csv_buf = Vec::new();
        while let Some(line) = lines.next_line().await.map_err(|e: std::io::Error| {
            DataError::BulkArchive(format!("Bybit bulk streaming decompress failed: {e}"))
        })? {
            csv_buf.extend_from_slice(line.as_bytes());
            csv_buf.push(b'\n');
            if csv_buf.len() as u64 > MAX_DECOMPRESSED_SIZE {
                return Err(DataError::BulkArchive(format!(
                    "Bybit decompressed data too large: >{MAX_DECOMPRESSED_SIZE} bytes",
                )));
            }
        }

        if csv_buf.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let trades = Server::parse_csv(&csv_buf)?;
        Ok(Some(trades))
    }
}

impl<Server: BybitBulkServer> BulkDayTradeFetcher for BybitBulkClient<Server> {
    #[tracing::instrument(skip(self), fields(exchange = "bybit", %date))]
    fn fetch_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<RestTrade>>, DataError>> + Send + 'a>> {
        Box::pin(self.download_and_parse_trades(market, date))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bybit_bulk_client_default() {
        let client = BybitBulkClient::<BybitServerSpot>::default();
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_bybit_bulk_client_with_config() {
        let config = BulkConfig {
            verify_checksum: false,
        };
        let client = BybitBulkClient::<BybitServerPerpetualsUsd>::with_config(config);
        assert!(!client.config.verify_checksum);
    }

    #[test]
    fn test_bybit_spot_path_prefix() {
        assert_eq!(BybitServerSpot::path_prefix(), "spot");
    }

    #[test]
    fn test_bybit_perpetuals_path_prefix() {
        assert_eq!(BybitServerPerpetualsUsd::path_prefix(), "trading");
    }
}
