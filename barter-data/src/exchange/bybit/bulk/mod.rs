pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkTradeFetcher, BulkTradeRequest, date_range},
    error::DataError,
    retry::{RetryPolicy, retry_with_backoff, is_retriable_data_error},
    trade::RestTrade,
};
use chrono::NaiveDate;
use flate2::read::GzDecoder;
use futures::{Stream, StreamExt, stream};
use std::{io::Read, marker::PhantomData};

use super::futures::BybitServerPerpetualsUsd;
use super::spot::BybitServerSpot;

/// Marker trait for Bybit server variants used in bulk downloads.
pub trait BybitBulkServer: Send + Sync + 'static {
    /// URL path prefix: `"spot"` or `"trading"`.
    fn path_prefix() -> &'static str;
}

impl BybitBulkServer for BybitServerSpot {
    fn path_prefix() -> &'static str {
        "spot"
    }
}

impl BybitBulkServer for BybitServerPerpetualsUsd {
    fn path_prefix() -> &'static str {
        "trading"
    }
}

/// Bulk archive download client for Bybit exchange variants.
///
/// Downloads daily GZ-compressed CSV trade archives from
/// `https://public.bybit.com/{prefix}/{SYMBOL}/{SYMBOL}{YYYY-MM-DD}.csv.gz`.
#[derive(Debug)]
pub struct BybitBulkClient<Server> {
    pub client: reqwest::Client,
    pub config: BulkConfig,
    pub retry: RetryPolicy,
    _server: PhantomData<Server>,
}

impl<Server> BybitBulkClient<Server> {
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

impl<Server> Default for BybitBulkClient<Server> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Server: BybitBulkServer> BybitBulkClient<Server> {
    async fn download_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let prefix = Server::path_prefix();
        let date_str = date.format("%Y-%m-%d");
        let url = format!(
            "https://public.bybit.com/{prefix}/{market}/{market}{date_str}.csv.gz"
        );

        let client = self.client.clone();
        let url_clone = url.clone();

        let response = retry_with_backoff(
            &self.retry,
            is_retriable_data_error,
            || {
                let client = client.clone();
                let url = url_clone.clone();
                async move {
                    let resp = client
                        .get(&url)
                        .send()
                        .await
                        .map_err(|e| DataError::Socket(format!("Bybit bulk request failed: {e}")))?;

                    let status = resp.status();

                    if status == reqwest::StatusCode::NOT_FOUND {
                        return Ok(None);
                    }

                    if !status.is_success() {
                        return Err(DataError::Socket(format!(
                            "Bybit bulk HTTP {status} for {url}"
                        )));
                    }

                    let bytes = resp
                        .bytes()
                        .await
                        .map_err(|e| DataError::Socket(format!("Bybit bulk read body failed: {e}")))?;

                    Ok(Some(bytes))
                }
            },
        )
        .await?;

        let bytes = match response {
            Some(b) => b,
            None => return Ok(None),
        };

        // Decompress GZ
        let mut decoder = GzDecoder::new(std::io::Cursor::new(&bytes));
        let mut csv_data = Vec::new();
        decoder.read_to_end(&mut csv_data).map_err(|e| {
            DataError::Socket(format!("Bybit bulk GZ decompress failed: {e}"))
        })?;

        let trades = trades::parse_trades(&csv_data)?;
        Ok(Some(trades))
    }
}

impl<Server: BybitBulkServer> BulkTradeFetcher for BybitBulkClient<Server> {
    fn stream_bulk_trades(
        &self,
        request: BulkTradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
        let dates = date_range(request.start, request.end);
        let concurrency = self.config.concurrency;
        let client = self.client.clone();
        let config = self.config.clone();
        let retry = self.retry.clone();

        stream::iter(dates)
            .map(move |date| {
                let client = BybitBulkClient::<Server> {
                    client: client.clone(),
                    config: config.clone(),
                    retry: retry.clone(),
                    _server: PhantomData,
                };
                let market = request.market.clone();
                async move { client.download_and_parse_trades(&market, date).await }
            })
            .buffer_unordered(concurrency)
            .filter_map(|result| async move {
                match result {
                    Ok(Some(trades)) => Some(Ok(trades)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bybit_bulk_client_default() {
        let client = BybitBulkClient::<BybitServerSpot>::default();
        assert_eq!(client.config.concurrency, 4);
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_bybit_bulk_client_with_config() {
        let config = BulkConfig {
            concurrency: 8,
            verify_checksum: false,
        };
        let client = BybitBulkClient::<BybitServerPerpetualsUsd>::with_config(config);
        assert_eq!(client.config.concurrency, 8);
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
