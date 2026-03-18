pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkDayTradeFetcher},
    error::DataError,
    trade::RestTrade,
};
use chrono::NaiveDate;
use std::{future::Future, io::Read, pin::Pin};

/// Maximum allowed uncompressed archive entry size (2 GiB).
const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024 * 1024;

/// Bulk archive download client for OKX.
///
/// Downloads daily ZIP-compressed CSV trade archives from
/// `https://www.okx.com/cdn/okex/traderecords/trades/daily/{YYYYMMDD}/{instrument}-trades-{YYYY-MM-DD}.zip`.
#[non_exhaustive]
#[derive(Debug)]
pub struct OkxBulkClient {
    pub(crate) client: reqwest::Client,
    pub(crate) config: BulkConfig,
}

impl OkxBulkClient {
    /// Return a reference to the bulk download configuration.
    pub fn config(&self) -> &BulkConfig {
        &self.config
    }

    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            config: BulkConfig::default(),
        }
    }

    pub fn with_config(config: BulkConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config,
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

        let resp =
            self.client.get(&url).send().await.map_err(|e| {
                DataError::Http {
                    status: None,
                    url: url.clone(),
                    message: format!("OKX bulk request failed: {e}"),
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
                message: format!("OKX bulk HTTP {status} for {url}"),
            });
        }

        let bytes = resp.bytes().await.map_err(|e| {
            DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("OKX bulk read body failed: {e}"),
            }
        })?;

        // Extract ZIP on blocking thread pool to avoid stalling the async executor.
        let csv_data = tokio::task::spawn_blocking(move || {
            let cursor = std::io::Cursor::new(bytes);
            let mut archive = zip::ZipArchive::new(cursor)
                .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP open failed: {e}")))?;

            if archive.len() == 0 {
                return Ok(Vec::new());
            }

            let file = archive
                .by_index(0)
                .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP read failed: {e}")))?;

            if file.size() > MAX_DECOMPRESSED_SIZE {
                return Err(DataError::BulkArchive(format!(
                    "OKX ZIP entry too large: {} bytes (max {MAX_DECOMPRESSED_SIZE} bytes)",
                    file.size(),
                )));
            }

            let mut csv_data = Vec::with_capacity(file.size() as usize);
            { file }
                .read_to_end(&mut csv_data)
                .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP extract failed: {e}")))?;
            Ok::<_, DataError>(csv_data)
        })
        .await
        .map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")))??;

        if csv_data.is_empty() {
            tracing::warn!("OKX bulk ZIP archive is empty for {market} on {date}");
            return Ok(Some(Vec::new()));
        }

        let trades = trades::parse_trades(&csv_data)?;
        Ok(Some(trades))
    }
}

/// Download and parse trades from an OKX monthly archive.
///
/// Monthly archive URL:
/// `https://static.okx.com/cdn/okex/traderecords/trades/monthly/{instrument}/{instrument}-trades-{YYYY-MM}.zip`
///
/// Returns `Ok(None)` if the monthly file returns 404 (not available).
/// The ZIP contains CSV files with the same format as the daily archives.
pub async fn download_monthly_trades(
    client: &reqwest::Client,
    instrument_id: &str,
    year: i32,
    month: u32,
) -> Result<Option<Vec<RestTrade>>, DataError> {
    let url = format!(
        "https://static.okx.com/cdn/okex/traderecords/trades/monthly/{instrument_id}/{instrument_id}-trades-{year}-{month:02}.zip",
    );

    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| DataError::Http {
            status: None,
            url: url.clone(),
            message: format!("OKX monthly request failed: {e}"),
        })?;

    let status = resp.status();

    if status == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }

    if !status.is_success() {
        return Err(DataError::Http {
            status: Some(status.as_u16()),
            url: url.clone(),
            message: format!("OKX monthly HTTP {status} for {url}"),
        });
    }

    let bytes = resp
        .bytes()
        .await
        .map_err(|e| DataError::Http {
            status: None,
            url: url.clone(),
            message: format!("OKX monthly read body failed: {e}"),
        })?;

    // Extract ZIP on blocking thread pool.
    let csv_data = tokio::task::spawn_blocking(move || {
        let cursor = std::io::Cursor::new(bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| DataError::BulkArchive(format!("OKX monthly ZIP open failed: {e}")))?;

        if archive.len() == 0 {
            return Ok(Vec::new());
        }

        let file = archive
            .by_index(0)
            .map_err(|e| DataError::BulkArchive(format!("OKX monthly ZIP read failed: {e}")))?;

        if file.size() > MAX_DECOMPRESSED_SIZE {
            return Err(DataError::BulkArchive(format!(
                "OKX monthly ZIP entry too large: {} bytes (max {MAX_DECOMPRESSED_SIZE} bytes)",
                file.size(),
            )));
        }

        let mut csv_data = Vec::with_capacity(file.size() as usize);
        { file }
            .read_to_end(&mut csv_data)
            .map_err(|e| DataError::BulkArchive(format!("OKX monthly ZIP extract failed: {e}")))?;
        Ok::<_, DataError>(csv_data)
    })
    .await
    .map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")))??;

    if csv_data.is_empty() {
        tracing::warn!("OKX monthly ZIP archive is empty for {instrument_id} {year}-{month:02}");
        return Ok(Some(Vec::new()));
    }

    let trades = trades::parse_trades(&csv_data)?;
    Ok(Some(trades))
}

impl Default for OkxBulkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkDayTradeFetcher for OkxBulkClient {
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
    fn test_okx_bulk_client_default() {
        let client = OkxBulkClient::default();
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_okx_bulk_client_with_config() {
        let config = BulkConfig {
            verify_checksum: false,
        };
        let client = OkxBulkClient::with_config(config);
        assert!(!client.config.verify_checksum);
    }

}
