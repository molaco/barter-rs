pub mod s3_signer;
pub mod trades;

use crate::{
    bulk::{
        streaming::{response_to_async_read, DEFAULT_BATCH_SIZE},
        BulkConfig, BulkDayTradeFetcher,
    },
    error::DataError,
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    trade::RestTrade,
};
use async_compression::tokio::bufread::Lz4Decoder;
use chrono::NaiveDate;
use s3_signer::AwsCredentials;
use std::{collections::HashMap, future::Future, pin::Pin};
use tokio::io::AsyncBufReadExt;
use trades::{parse_json_line, parse_json_line_multi};

/// Base URL for Hyperliquid hourly node fill data (S3).
const BASE_URL: &str = "https://hl-mainnet-node-data.s3.ap-northeast-1.amazonaws.com/node_fills_by_block/hourly";

/// S3 bucket region for Hyperliquid node data.
const S3_REGION: &str = "ap-northeast-1";

/// Bulk archive download client for Hyperliquid perpetuals.
///
/// Downloads hourly LZ4-compressed JSON fill data from the public S3 bucket.
/// Requires the `x-amz-request-payer: requester` header (requester-pays bucket).
/// AWS credentials are loaded from environment variables for SigV4 signing.
#[non_exhaustive]
#[derive(Debug)]
pub struct HyperliquidBulkClient {
    pub(crate) client: reqwest::Client,
    pub(crate) config: BulkConfig,
    credentials: Option<AwsCredentials>,
}

impl HyperliquidBulkClient {
    /// Return a reference to the bulk download configuration.
    pub fn config(&self) -> &BulkConfig {
        &self.config
    }

    pub fn new() -> Self {
        let credentials = AwsCredentials::from_env();
        if credentials.is_none() {
            tracing::warn!(
                "AWS credentials not found (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY). \
                 Hyperliquid S3 downloads will fail with 403."
            );
        }

        let client = reqwest::Client::builder()
            .build()
            .expect("failed to build reqwest client");
        Self {
            client,
            config: BulkConfig::default(),
            credentials,
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        let mut this = Self::new();
        this.config = config;
        this
    }

    /// Build a signed S3 GET request for the given URL.
    fn signed_request(&self, url: &str) -> reqwest::RequestBuilder {
        let mut request = self.client.get(url);
        if let Some(ref creds) = self.credentials {
            let signed = s3_signer::sign_s3_get(url, creds, S3_REGION);
            request = request
                .header("Authorization", &signed.authorization)
                .header("x-amz-date", &signed.x_amz_date)
                .header("x-amz-content-sha256", signed.x_amz_content_sha256)
                .header("x-amz-request-payer", "requester");
            if let Some(ref token) = signed.x_amz_security_token {
                request = request.header("x-amz-security-token", token);
            }
        }
        request
    }

    /// Download and parse a single hour using streaming LZ4 decompression.
    ///
    /// The HTTP response is streamed through `Lz4Decoder` and parsed line
    /// by line — no intermediate compressed or decompressed buffers are held
    /// in memory. On failure the full download+parse is retried.
    ///
    /// Returns `Ok(None)` if 404.
    async fn download_and_parse_hour(
        &self,
        market: &str,
        date: NaiveDate,
        hour: u8,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let date_str = date.format("%Y%m%d");
        let url = format!("{BASE_URL}/{date_str}/{hour}.lz4");
        let policy = RetryPolicy::default();

        retry_with_backoff(&policy, is_retriable_data_error, || async {
            let resp = self.signed_request(&url).send().await.map_err(|e| DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("HTTP request failed: {e}"),
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
            let lz4_reader = Lz4Decoder::new(buf_reader);
            let mut lines = tokio::io::BufReader::new(lz4_reader).lines();

            let mut trades = Vec::new();
            while let Some(line) = lines.next_line().await.map_err(|e| {
                DataError::BulkArchive(format!("LZ4 stream read error: {e}"))
            })? {
                trades.extend(parse_json_line(&line, market)?);
            }

            Ok(Some(trades))
        })
        .await
    }

    /// Download and parse all 24 hours of fill data for one day.
    async fn download_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let futures: Vec<_> = (0..24u8)
            .map(|hour| self.download_and_parse_hour(market, date, hour))
            .collect();
        let results = futures::future::try_join_all(futures).await?;

        let mut all_trades = Vec::new();
        let mut any_found = false;
        for maybe_trades in results {
            if let Some(trades) = maybe_trades {
                any_found = true;
                all_trades.extend(trades);
            }
        }

        if any_found {
            Ok(Some(all_trades))
        } else {
            Ok(None)
        }
    }

    /// Download and parse all 24 hours, returning ALL coins' trades grouped by coin.
    ///
    /// Hours are processed sequentially to avoid holding multiple hours'
    /// data in memory simultaneously.
    async fn download_and_parse_trades_multi(
        &self,
        date: NaiveDate,
    ) -> Result<Option<HashMap<String, Vec<RestTrade>>>, DataError> {
        let mut merged: HashMap<String, Vec<RestTrade>> = HashMap::new();
        let mut any_found = false;
        let date_str = date.format("%Y%m%d");

        for hour in 0..24u8 {
            let url = format!("{BASE_URL}/{date_str}/{hour}.lz4");
            let policy = RetryPolicy::default();

            // Collect each hour's data into a local map so retries don't
            // leave partial data in `merged`.
            let hour_map: Option<HashMap<String, Vec<RestTrade>>> =
                retry_with_backoff(&policy, is_retriable_data_error, || async {
                    let resp = self.signed_request(&url).send().await.map_err(|e| DataError::Http {
                        status: None,
                        url: url.clone(),
                        message: format!("HTTP request failed: {e}"),
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
                    let lz4_reader = Lz4Decoder::new(buf_reader);
                    let mut lines = tokio::io::BufReader::new(lz4_reader).lines();

                    let mut local: HashMap<String, Vec<RestTrade>> = HashMap::new();
                    while let Some(line) = lines.next_line().await.map_err(|e| {
                        DataError::BulkArchive(format!("LZ4 stream read error: {e}"))
                    })? {
                        parse_json_line_multi(&line, &mut local)?;
                    }

                    Ok(Some(local))
                })
                .await?;

            if let Some(hour_map) = hour_map {
                any_found = true;
                for (coin, trades) in hour_map {
                    merged.entry(coin).or_default().extend(trades);
                }
            }
        }

        if any_found {
            Ok(Some(merged))
        } else {
            Ok(None)
        }
    }

    /// Fetch all coins' trades for a single day from the Hyperliquid S3 archive.
    ///
    /// Returns `Ok(Some(HashMap<coin, trades>))` on success, `Ok(None)` if the date
    /// is not available (404), or `Err` on failure. Each S3 hourly file contains all
    /// coins — this method downloads all 24 hours and groups trades by coin.
    pub async fn fetch_day_trades_multi(
        &self,
        date: NaiveDate,
    ) -> Result<Option<HashMap<String, Vec<RestTrade>>>, DataError> {
        self.download_and_parse_trades_multi(date).await
    }

    /// Stream trades for a single day through a channel in batches.
    ///
    /// Downloads all 24 hourly LZ4 files concurrently (same as
    /// [`download_and_parse_trades`]), but instead of collecting into a single
    /// `Vec` the trades are sent through `tx` in batches of
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// Returns `Ok(true)` if any hour had data, `Ok(false)` if all were 404.
    async fn stream_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
        tx: &tokio::sync::mpsc::Sender<Vec<RestTrade>>,
    ) -> Result<bool, DataError> {
        let date_str = date.format("%Y%m%d").to_string();

        let futures: Vec<_> = (0..24u8)
            .map(|hour| {
                let url = format!("{BASE_URL}/{date_str}/{hour}.lz4");
                let policy = RetryPolicy::default();

                async move {
                    retry_with_backoff(&policy, is_retriable_data_error, || async {
                        let resp =
                            self.signed_request(&url).send().await.map_err(|e| DataError::Http {
                                status: None,
                                url: url.clone(),
                                message: format!("HTTP request failed: {e}"),
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
                        let lz4_reader = Lz4Decoder::new(buf_reader);
                        let mut lines = tokio::io::BufReader::new(lz4_reader).lines();

                        let mut batch = Vec::with_capacity(DEFAULT_BATCH_SIZE);
                        while let Some(line) = lines.next_line().await.map_err(|e| {
                            DataError::BulkArchive(format!("LZ4 stream read error: {e}"))
                        })? {
                            batch.extend(parse_json_line(&line, market)?);
                            if batch.len() >= DEFAULT_BATCH_SIZE {
                                tx.send(std::mem::replace(
                                    &mut batch,
                                    Vec::with_capacity(DEFAULT_BATCH_SIZE),
                                ))
                                .await
                                .map_err(|_| {
                                    DataError::BulkArchive("receiver dropped".into())
                                })?;
                            }
                        }
                        if !batch.is_empty() {
                            tx.send(batch).await.map_err(|_| {
                                DataError::BulkArchive("receiver dropped".into())
                            })?;
                        }

                        Ok(true)
                    })
                    .await
                }
            })
            .collect();

        let results = futures::future::try_join_all(futures).await?;
        Ok(results.into_iter().any(|found| found))
    }
}

impl Default for HyperliquidBulkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkDayTradeFetcher for HyperliquidBulkClient {
    #[tracing::instrument(skip(self), fields(exchange = "hyperliquid", %date))]
    fn fetch_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<RestTrade>>, DataError>> + Send + 'a>> {
        Box::pin(self.download_and_parse_trades(market, date))
    }

    #[tracing::instrument(skip(self, tx), fields(exchange = "hyperliquid", %date))]
    fn stream_day_trades<'a>(
        &'a self,
        market: &'a str,
        date: NaiveDate,
        tx: &'a tokio::sync::mpsc::Sender<Vec<RestTrade>>,
    ) -> Pin<Box<dyn Future<Output = Result<bool, DataError>> + Send + 'a>> {
        Box::pin(self.stream_and_parse_trades(market, date, tx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client() {
        let client = HyperliquidBulkClient::new();
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_with_config() {
        let config = BulkConfig {
            verify_checksum: false,
        };
        let client = HyperliquidBulkClient::with_config(config);
        assert!(!client.config.verify_checksum);
    }

    #[test]
    fn test_url_format() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let date_str = date.format("%Y%m%d");
        let url = format!("{BASE_URL}/{date_str}/{}.lz4", 3u8);
        assert_eq!(
            url,
            "https://hl-mainnet-node-data.s3.ap-northeast-1.amazonaws.com/node_fills_by_block/hourly/20240615/3.lz4"
        );
    }
}
