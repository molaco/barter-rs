pub mod s3_signer;
pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkDayTradeFetcher},
    error::DataError,
    retry::{RetryPolicy, is_retriable_data_error},
    trade::RestTrade,
};
use chrono::NaiveDate;
use s3_signer::AwsCredentials;
use std::{collections::HashMap, future::Future, pin::Pin};
use trades::{parse_fills, parse_fills_multi};

/// Base URL for Hyperliquid hourly node fill data (S3).
const BASE_URL: &str = "https://hl-mainnet-node-data.s3.ap-northeast-1.amazonaws.com/node_fills_by_block/hourly";

/// S3 bucket region for Hyperliquid node data.
const S3_REGION: &str = "ap-northeast-1";

/// Bulk archive download client for Hyperliquid perpetuals.
///
/// Downloads hourly LZ4-compressed JSON fill data from the public S3 bucket.
/// Requires the `x-amz-request-payer: requester` header (requester-pays bucket).
/// AWS credentials are loaded from environment variables for SigV4 signing.
#[derive(Debug)]
pub struct HyperliquidBulkClient {
    pub client: reqwest::Client,
    pub config: BulkConfig,
    credentials: Option<AwsCredentials>,
}

impl HyperliquidBulkClient {
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

    /// Minimum file size (1 MB) below which range resume is not attempted.
    const RANGE_RESUME_THRESHOLD: usize = 1_048_576;

    /// Download and LZ4-decompress a single hour file with HTTP Range resume.
    ///
    /// On retry after a partial download (>= 1 MB received), sends a
    /// `Range: bytes=N-` header to resume. S3 natively supports Range requests.
    ///
    /// Uses an owned `Vec<u8>` buffer across a manual retry loop — no
    /// `Arc<Mutex<>>` needed since retries are sequential.
    ///
    /// Returns `None` if 404.
    async fn download_and_decompress_hour(
        &self,
        date: NaiveDate,
        hour: u8,
    ) -> Result<Option<Vec<u8>>, DataError> {
        use crate::retry::apply_backoff;
        use futures::StreamExt;

        let date_str = date.format("%Y%m%d");
        let url = format!("{BASE_URL}/{date_str}/{hour}.lz4");
        let retry = RetryPolicy::default();
        let mut partial = Vec::new();
        let mut attempts = 0u32;

        let compressed = loop {
            attempts += 1;
            let existing_len = partial.len();

            let mut request = self.client.get(&url);

            // Add Range header for resume when enough data has been accumulated.
            if existing_len >= Self::RANGE_RESUME_THRESHOLD {
                request = request.header("Range", format!("bytes={existing_len}-"));
                tracing::debug!(
                    url = %url,
                    resume_from = existing_len,
                    "resuming S3 download with Range header"
                );
            }

            // Sign each attempt (credentials may rotate).
            if let Some(ref creds) = self.credentials {
                let signed = s3_signer::sign_s3_get(&url, creds, S3_REGION);
                request = request
                    .header("Authorization", &signed.authorization)
                    .header("x-amz-date", &signed.x_amz_date)
                    .header("x-amz-content-sha256", signed.x_amz_content_sha256)
                    .header("x-amz-request-payer", "requester");
                if let Some(ref token) = signed.x_amz_security_token {
                    request = request.header("x-amz-security-token", token);
                }
            }

            let response = match request.send().await {
                Ok(r) => r,
                Err(e) => {
                    let err = DataError::Http {
                        status: None,
                        url: url.clone(),
                        message: format!("HTTP request failed: {e}"),
                    };
                    if !is_retriable_data_error(&err) || attempts > retry.max_retries {
                        return Err(err);
                    }
                    apply_backoff(&retry, attempts).await;
                    continue;
                }
            };

            let status = response.status();
            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            // 416 Range Not Satisfiable: restart from scratch.
            if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
                tracing::debug!(url = %url, "got 416, restarting download from scratch");
                partial.clear();
                if attempts > retry.max_retries {
                    return Err(DataError::Http {
                        status: Some(416),
                        url: url.clone(),
                        message: "Range Not Satisfiable".into(),
                    });
                }
                apply_backoff(&retry, attempts).await;
                continue;
            }

            if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
                let err = DataError::Http {
                    status: Some(status.as_u16()),
                    url: url.clone(),
                    message: format!("HTTP {status}"),
                };
                if !is_retriable_data_error(&err) || attempts > retry.max_retries {
                    return Err(err);
                }
                apply_backoff(&retry, attempts).await;
                continue;
            }

            // If server returned 200 instead of 206, discard partial and accept full body.
            if status == reqwest::StatusCode::OK && existing_len > 0 {
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
                    url: url.clone(),
                    message: format!("body read failed: {e}"),
                };
                if !is_retriable_data_error(&err) || attempts > retry.max_retries {
                    return Err(err);
                }
                apply_backoff(&retry, attempts).await;
                continue;
            }

            break std::mem::take(&mut partial);
        };

        // Decompress on the blocking thread pool to avoid stalling
        // the async executor with synchronous LZ4 I/O.
        const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024 * 1024;

        let decompressed = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed.as_slice());
            let mut buf = Vec::new();
            let read = Read::take(&mut decoder, MAX_DECOMPRESSED_SIZE + 1)
                .read_to_end(&mut buf)
                .map_err(|e| DataError::BulkArchive(format!("LZ4 decompression failed: {e}")))?;
            if read as u64 > MAX_DECOMPRESSED_SIZE {
                return Err(DataError::BulkArchive(format!(
                    "LZ4 decompressed data too large: >{MAX_DECOMPRESSED_SIZE} bytes",
                )));
            }
            Ok::<_, DataError>(buf)
        })
        .await
        .map_err(|e| DataError::BulkArchive(format!("LZ4 task panicked: {e}")))??;

        Ok(Some(decompressed))
    }

    /// Download and parse a single hour, filtering for one market.
    async fn download_and_parse_hour(
        &self,
        market: &str,
        date: NaiveDate,
        hour: u8,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        match self.download_and_decompress_hour(date, hour).await? {
            Some(bytes) => Ok(Some(parse_fills(&bytes, market)?)),
            None => Ok(None),
        }
    }

    /// Download and parse all 24 hours of fill data for one day.
    async fn download_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        // Download all 24 hours concurrently for maximum throughput.
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
    /// Hours are processed sequentially to avoid holding all 24 decompressed
    /// buffers (~100 MB each) in memory simultaneously.
    async fn download_and_parse_trades_multi(
        &self,
        date: NaiveDate,
    ) -> Result<Option<HashMap<String, Vec<RestTrade>>>, DataError> {
        let mut merged: HashMap<String, Vec<RestTrade>> = HashMap::new();
        let mut any_found = false;

        for hour in 0..24u8 {
            if let Some(bytes) = self.download_and_decompress_hour(date, hour).await? {
                any_found = true;
                for (coin, trades) in parse_fills_multi(&bytes)? {
                    merged.entry(coin).or_default().extend(trades);
                }
                // `bytes` dropped here — only one hour's decompressed data at a time
            }
        }

        if any_found {
            Ok(Some(merged))
        } else {
            Ok(None)
        }
    }
}

impl Default for HyperliquidBulkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkDayTradeFetcher for HyperliquidBulkClient {
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
