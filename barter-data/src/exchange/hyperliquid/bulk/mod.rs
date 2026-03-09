pub mod s3_signer;
pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkTradeFetcher, BulkTradeRequest, date_range},
    error::DataError,
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    trade::RestTrade,
};
use chrono::NaiveDate;
use futures::Stream;
use s3_signer::AwsCredentials;
use trades::parse_fills;

/// Base URL for Hyperliquid hourly node fill data (S3).
const BASE_URL: &str = "https://hl-mainnet-node-data.s3.amazonaws.com/node_fills_by_block/hourly";

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
    pub retry: RetryPolicy,
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
            retry: RetryPolicy::default(),
            credentials,
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        let mut this = Self::new();
        this.config = config;
        this
    }

    /// Download and parse a single hour of fill data.
    ///
    /// Returns `Ok(None)` if the hour file does not exist (HTTP 404).
    async fn download_and_parse_hour(
        &self,
        market: &str,
        date: NaiveDate,
        hour: u8,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let date_str = date.format("%Y%m%d");
        let url = format!("{BASE_URL}/{date_str}/{hour}.lz4");
        let market = market.to_owned();
        let credentials = self.credentials.clone();

        let result = retry_with_backoff(&self.retry, is_retriable_data_error, || {
            let url = url.clone();
            let client = self.client.clone();
            let market = market.clone();
            let credentials = credentials.clone();
            async move {
                let mut request = client.get(&url);

                // Sign with SigV4 if AWS credentials are available
                if let Some(ref creds) = credentials {
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

                let response = request
                    .send()
                    .await
                    .map_err(|e| DataError::Socket(format!("HTTP request failed: {e}")))?;

                let status = response.status();
                if status == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                if !status.is_success() {
                    return Err(DataError::Socket(format!("HTTP {status} fetching {url}")));
                }

                let compressed = response
                    .bytes()
                    .await
                    .map_err(|e| DataError::Socket(format!("failed to read response: {e}")))?;

                let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed.as_ref());
                let mut decompressed = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut decompressed)
                    .map_err(|e| DataError::Socket(format!("LZ4 decompression failed: {e}")))?;

                let trades = parse_fills(&decompressed, &market)?;
                Ok(Some(trades))
            }
        })
        .await?;

        Ok(result)
    }

    /// Download and parse all 24 hours of fill data for one day.
    ///
    /// Returns `Ok(None)` if every hour returned 404.
    async fn download_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let mut all_trades = Vec::new();
        let mut any_found = false;

        for hour in 0..24u8 {
            if let Some(trades) = self.download_and_parse_hour(market, date, hour).await? {
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
}

impl Default for HyperliquidBulkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkTradeFetcher for HyperliquidBulkClient {
    fn stream_bulk_trades(
        &self,
        request: BulkTradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
        let dates = date_range(request.start, request.end);
        let concurrency = self.config.concurrency;
        let market = request.market;

        // Each date downloads its 24 hours sequentially; dates are buffered concurrently.
        let client = reqwest::Client::clone(&self.client);
        let retry = self.retry.clone();
        let credentials = self.credentials.clone();

        futures::stream::iter(dates.into_iter().map(move |date| {
            let market = market.clone();
            let client = client.clone();
            let retry = retry.clone();
            let credentials = credentials.clone();
            async move {
                let bulk = HyperliquidBulkClient {
                    client,
                    config: BulkConfig::default(),
                    retry,
                    credentials,
                };
                match bulk.download_and_parse_trades(&market, date).await {
                    Ok(Some(trades)) => Some(Ok(trades)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            }
        }))
        .buffer_unordered(concurrency)
        .filter_map(|opt| async { opt })
    }
}

// Required imports for the stream combinators.
use futures::StreamExt;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_client() {
        let client = HyperliquidBulkClient::new();
        assert_eq!(client.config.concurrency, 4);
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_with_config() {
        let config = BulkConfig {
            concurrency: 8,
            verify_checksum: false,
        };
        let client = HyperliquidBulkClient::with_config(config);
        assert_eq!(client.config.concurrency, 8);
        assert!(!client.config.verify_checksum);
    }

    #[test]
    fn test_url_format() {
        let date = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let date_str = date.format("%Y%m%d");
        let url = format!("{BASE_URL}/{date_str}/{}.lz4", 3u8);
        assert_eq!(
            url,
            "https://hl-mainnet-node-data.s3.amazonaws.com/node_fills_by_block/hourly/20240615/3.lz4"
        );
    }
}
