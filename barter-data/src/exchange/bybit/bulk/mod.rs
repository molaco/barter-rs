pub mod trades;

use crate::{
    bulk::{
        streaming::{parse_csv_into_sender, parse_csv_stream, response_to_async_read, DEFAULT_BATCH_SIZE},
        BulkConfig, BulkDayTradeFetcher,
    },
    error::DataError,
    trade::RestTrade,
};
use async_compression::tokio::bufread::GzipDecoder;
use chrono::NaiveDate;
use std::{future::Future, marker::PhantomData, pin::Pin};
use tokio::io::AsyncRead;

use super::{futures::BybitServerPerpetualsUsd, spot::BybitServerSpot};

/// Marker trait for Bybit server variants used in bulk downloads.
pub trait BybitBulkServer: Send + Sync + 'static {
    /// URL path prefix: `"spot"` or `"trading"`.
    fn path_prefix() -> &'static str;
    /// Separator between market symbol and date in archive filenames.
    /// Spot uses `"_"` (e.g. `BTCUSDT_2025-12-09.csv.gz`),
    /// perpetuals use no separator (e.g. `BTCUSDT2025-12-09.csv.gz`).
    fn date_separator() -> &'static str;
    /// Parse CSV data into trades (sync fallback). Spot and perpetuals have
    /// different CSV schemas.
    fn parse_csv(data: &[u8]) -> Result<Vec<RestTrade>, DataError>;
    /// Parse CSV trades from an async reader (streaming). Records are
    /// deserialized one at a time — peak memory is bounded regardless of
    /// archive size.
    ///
    /// Default impl falls back to reading all bytes and calling [`Self::parse_csv`].
    /// Override with a streaming implementation for bounded memory usage.
    fn parse_csv_async(
        reader: impl AsyncRead + Unpin + Send,
    ) -> impl Future<Output = Result<Vec<RestTrade>, DataError>> + Send {
        async move {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            let mut reader = reader;
            reader.read_to_end(&mut buf).await.map_err(|e| {
                DataError::BulkArchive(format!("failed to read CSV data: {e}"))
            })?;
            Self::parse_csv(&buf)
        }
    }

    /// Parse CSV trades from an async reader, sending batches through a channel.
    ///
    /// Default impl falls back to reading all bytes, calling [`Self::parse_csv`],
    /// and sending the result as one batch. Override with a streaming
    /// implementation for bounded memory usage.
    fn parse_csv_async_into_sender(
        reader: impl AsyncRead + Unpin + Send,
        tx: &tokio::sync::mpsc::Sender<Vec<RestTrade>>,
        _batch_size: usize,
    ) -> impl Future<Output = Result<u64, DataError>> + Send {
        async move {
            use tokio::io::AsyncReadExt;
            let mut buf = Vec::new();
            let mut reader = reader;
            reader.read_to_end(&mut buf).await.map_err(|e| {
                DataError::BulkArchive(format!("failed to read CSV data: {e}"))
            })?;
            let trades = Self::parse_csv(&buf)?;
            let count = trades.len() as u64;
            if !trades.is_empty() {
                tx.send(trades).await.map_err(|_| {
                    DataError::BulkArchive("receiver dropped".into())
                })?;
            }
            Ok(count)
        }
    }
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
    async fn parse_csv_async(
        reader: impl AsyncRead + Unpin + Send,
    ) -> Result<Vec<RestTrade>, DataError> {
        parse_csv_stream::<_, trades::BybitSpotBulkTrade, _>(reader, true, false, RestTrade::try_from)
            .await
    }
    async fn parse_csv_async_into_sender(
        reader: impl AsyncRead + Unpin + Send,
        tx: &tokio::sync::mpsc::Sender<Vec<RestTrade>>,
        batch_size: usize,
    ) -> Result<u64, DataError> {
        parse_csv_into_sender::<_, trades::BybitSpotBulkTrade, _>(reader, true, false, RestTrade::try_from, tx, batch_size)
            .await
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
    async fn parse_csv_async(
        reader: impl AsyncRead + Unpin + Send,
    ) -> Result<Vec<RestTrade>, DataError> {
        parse_csv_stream::<_, trades::BybitBulkTrade, _>(reader, true, false, RestTrade::try_from).await
    }
    async fn parse_csv_async_into_sender(
        reader: impl AsyncRead + Unpin + Send,
        tx: &tokio::sync::mpsc::Sender<Vec<RestTrade>>,
        batch_size: usize,
    ) -> Result<u64, DataError> {
        parse_csv_into_sender::<_, trades::BybitBulkTrade, _>(reader, true, false, RestTrade::try_from, tx, batch_size)
            .await
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
    /// Download and parse trades using fully streaming gzip + CSV parsing.
    ///
    /// The HTTP response body is streamed through `GzipDecoder` and then
    /// directly into `csv-async`'s async deserializer. Records are parsed
    /// one at a time — peak memory is bounded regardless of archive size.
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

        let resp = self.client.get(&url).send().await.map_err(|e| {
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

        // Fully streaming pipeline: HTTP chunks → gzip decompress → CSV parse.
        // No intermediate Vec<u8> buffer — records flow one at a time.
        let buf_reader = response_to_async_read(resp);
        let gzip_reader = GzipDecoder::new(buf_reader);
        let trades = Server::parse_csv_async(gzip_reader).await?;

        Ok(Some(trades))
    }

    /// Stream trades for a single day through a channel in batches.
    ///
    /// Same HTTP + gzip pipeline as [`download_and_parse_trades`](Self::download_and_parse_trades),
    /// but sends parsed records through `tx` in batches of `batch_size`
    /// instead of collecting into a `Vec`.
    ///
    /// Returns `Ok(true)` if data was found, `Ok(false)` on 404.
    async fn stream_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
        tx: &tokio::sync::mpsc::Sender<Vec<RestTrade>>,
        batch_size: usize,
    ) -> Result<bool, DataError> {
        let prefix = Server::path_prefix();
        let sep = Server::date_separator();
        let date_str = date.format("%Y-%m-%d");
        let url =
            format!("https://public.bybit.com/{prefix}/{market}/{market}{sep}{date_str}.csv.gz");

        let resp = self.client.get(&url).send().await.map_err(|e| {
            DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("Bybit bulk request failed: {e}"),
            }
        })?;

        let status = resp.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }

        if !status.is_success() {
            return Err(DataError::Http {
                status: Some(status.as_u16()),
                url: url.clone(),
                message: format!("Bybit bulk HTTP {status}"),
            });
        }

        let buf_reader = response_to_async_read(resp);
        let gzip_reader = GzipDecoder::new(buf_reader);
        Server::parse_csv_async_into_sender(gzip_reader, tx, batch_size).await?;

        Ok(true)
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

    fn stream_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
        tx: &'a tokio::sync::mpsc::Sender<Vec<RestTrade>>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DataError>> + Send + 'a>> {
        Box::pin(self.stream_and_parse_trades(market, date, tx, DEFAULT_BATCH_SIZE))
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
