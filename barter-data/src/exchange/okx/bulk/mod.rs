pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkTradeFetcher, BulkTradeRequest, date_range},
    error::DataError,
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    trade::RestTrade,
};
use chrono::NaiveDate;
use futures::{Stream, StreamExt, stream};
use std::io::Read;

/// Bulk archive download client for OKX.
///
/// Downloads daily ZIP-compressed CSV trade archives from
/// `https://www.okx.com/cdn/okex/traderecords/trades/daily/{YYYYMMDD}/{instrument}-trades-{YYYY-MM-DD}.zip`.
#[derive(Debug)]
pub struct OkxBulkClient {
    pub client: reqwest::Client,
    pub config: BulkConfig,
    pub retry: RetryPolicy,
}

impl OkxBulkClient {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            config: BulkConfig::default(),
            retry: RetryPolicy::default(),
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
            retry: RetryPolicy::default(),
        }
    }

    async fn download_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Result<Option<Vec<RestTrade>>, DataError> {
        let date_folder = date.format("%Y%m%d");
        let date_file = date.format("%Y-%m-%d");
        let url = format!(
            "https://www.okx.com/cdn/okex/traderecords/trades/daily/{date_folder}/{market}-trades-{date_file}.zip"
        );

        let client = self.client.clone();
        let url_clone = url.clone();

        let response =
            retry_with_backoff(&self.retry, is_retriable_data_error, || {
                let client = client.clone();
                let url = url_clone.clone();
                async move {
                    let resp =
                        client.get(&url).send().await.map_err(|e| {
                            DataError::Socket(format!("OKX bulk request failed: {e}"))
                        })?;

                    let status = resp.status();

                    if status == reqwest::StatusCode::NOT_FOUND {
                        return Ok(None);
                    }

                    if !status.is_success() {
                        return Err(DataError::Socket(format!(
                            "OKX bulk HTTP {status} for {url}"
                        )));
                    }

                    let bytes = resp.bytes().await.map_err(|e| {
                        DataError::Socket(format!("OKX bulk read body failed: {e}"))
                    })?;

                    Ok(Some(bytes))
                }
            })
            .await?;

        let bytes = match response {
            Some(b) => b,
            None => return Ok(None),
        };

        // Extract ZIP and read first file
        let cursor = std::io::Cursor::new(&bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| DataError::Socket(format!("OKX bulk ZIP open failed: {e}")))?;

        if archive.len() == 0 {
            tracing::warn!("OKX bulk ZIP archive is empty for {market} on {date}");
            return Ok(Some(Vec::new()));
        }

        let mut csv_data = Vec::new();
        archive
            .by_index(0)
            .map_err(|e| DataError::Socket(format!("OKX bulk ZIP read failed: {e}")))?
            .read_to_end(&mut csv_data)
            .map_err(|e| DataError::Socket(format!("OKX bulk ZIP extract failed: {e}")))?;

        let trades = trades::parse_trades(&csv_data)?;
        Ok(Some(trades))
    }
}

impl Default for OkxBulkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkTradeFetcher for OkxBulkClient {
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
                let bulk_client = OkxBulkClient {
                    client: client.clone(),
                    config: config.clone(),
                    retry: retry.clone(),
                };
                let market = request.market.clone();
                async move { bulk_client.download_and_parse_trades(&market, date).await }
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
    fn test_okx_bulk_client_default() {
        let client = OkxBulkClient::default();
        assert_eq!(client.config.concurrency, 4);
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_okx_bulk_client_with_config() {
        let config = BulkConfig {
            concurrency: 16,
            verify_checksum: false,
        };
        let client = OkxBulkClient::with_config(config);
        assert_eq!(client.config.concurrency, 16);
        assert!(!client.config.verify_checksum);
    }
}
