pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkTradeFetcher, BulkTradeRequest, date_range},
    error::DataError,
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    trade::RestTrade,
};
use chrono::NaiveDate;
use futures::Stream;
use trades::parse_fills;

/// Base URL for Hyperliquid hourly node fill data (S3).
const BASE_URL: &str =
    "https://hl-mainnet-node-data.s3.amazonaws.com/node_fills_by_block/hourly";

/// Bulk archive download client for Hyperliquid perpetuals.
///
/// Downloads hourly LZ4-compressed JSON fill data from the public S3 bucket.
/// Requires the `x-amz-request-payer: requester` header (requester-pays bucket).
#[derive(Debug)]
pub struct HyperliquidBulkClient {
    pub client: reqwest::Client,
    pub config: BulkConfig,
    pub retry: RetryPolicy,
}

impl HyperliquidBulkClient {
    pub fn new() -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "x-amz-request-payer",
            reqwest::header::HeaderValue::from_static("requester"),
        );
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .expect("failed to build reqwest client");
        Self {
            client,
            config: BulkConfig::default(),
            retry: RetryPolicy::default(),
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
        let url = format!("{BASE_URL}/{date}/{hour:02}");
        let market = market.to_owned();

        let result = retry_with_backoff(&self.retry, is_retriable_data_error, || {
            let url = url.clone();
            let client = self.client.clone();
            let market = market.clone();
            async move {
                let response = client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| DataError::Socket(format!("HTTP request failed: {e}")))?;

                let status = response.status();
                if status == reqwest::StatusCode::NOT_FOUND {
                    return Ok(None);
                }
                if !status.is_success() {
                    return Err(DataError::Socket(format!(
                        "HTTP {status} fetching {url}"
                    )));
                }

                let compressed = response
                    .bytes()
                    .await
                    .map_err(|e| DataError::Socket(format!("failed to read response: {e}")))?;

                let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed.as_ref());
                let mut decompressed = Vec::new();
                std::io::Read::read_to_end(&mut decoder, &mut decompressed).map_err(|e| {
                    DataError::Socket(format!("LZ4 decompression failed: {e}"))
                })?;

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

        futures::stream::iter(dates.into_iter().map(move |date| {
            let market = market.clone();
            let client = client.clone();
            let retry = retry.clone();
            async move {
                let bulk = HyperliquidBulkClient {
                    client,
                    config: BulkConfig::default(),
                    retry,
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
        let url = format!("{BASE_URL}/{date}/{:02}", 3u8);
        assert_eq!(
            url,
            "https://hl-mainnet-node-data.s3.amazonaws.com/node_fills_by_block/hourly/2024-06-15/03"
        );
    }
}
