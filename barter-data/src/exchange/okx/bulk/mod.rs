pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkDayTradeFetcher},
    error::DataError,
    trade::RestTrade,
};
use chrono::NaiveDate;
use std::{future::Future, io::Read, pin::Pin};

/// Bulk archive download client for OKX.
///
/// Downloads daily ZIP-compressed CSV trade archives from
/// `https://www.okx.com/cdn/okex/traderecords/trades/daily/{YYYYMMDD}/{instrument}-trades-{YYYY-MM-DD}.zip`.
#[derive(Debug)]
pub struct OkxBulkClient {
    pub client: reqwest::Client,
    pub config: BulkConfig,
}

impl OkxBulkClient {
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

        // Extract ZIP and read first file
        let cursor = std::io::Cursor::new(&bytes);
        let mut archive = zip::ZipArchive::new(cursor)
            .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP open failed: {e}")))?;

        if archive.len() == 0 {
            tracing::warn!("OKX bulk ZIP archive is empty for {market} on {date}");
            return Ok(Some(Vec::new()));
        }

        let mut csv_data = Vec::new();
        archive
            .by_index(0)
            .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP read failed: {e}")))?
            .read_to_end(&mut csv_data)
            .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP extract failed: {e}")))?;

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

    let cursor = std::io::Cursor::new(&bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| DataError::BulkArchive(format!("OKX monthly ZIP open failed: {e}")))?;

    if archive.len() == 0 {
        tracing::warn!("OKX monthly ZIP archive is empty for {instrument_id} {year}-{month:02}");
        return Ok(Some(Vec::new()));
    }

    let mut csv_data = Vec::new();
    archive
        .by_index(0)
        .map_err(|e| DataError::BulkArchive(format!("OKX monthly ZIP read failed: {e}")))?
        .read_to_end(&mut csv_data)
        .map_err(|e| DataError::BulkArchive(format!("OKX monthly ZIP extract failed: {e}")))?;

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
        assert_eq!(client.config.concurrency, 4);
        assert!(client.config.verify_checksum);
    }

    #[test]
    fn test_okx_bulk_client_with_config() {
        let config = BulkConfig {
            concurrency: 16,
            verify_checksum: false,
            cache_dir: None,
        };
        let client = OkxBulkClient::with_config(config);
        assert_eq!(client.config.concurrency, 16);
        assert!(!client.config.verify_checksum);
    }

}
