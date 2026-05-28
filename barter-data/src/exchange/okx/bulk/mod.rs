pub mod trades;

use crate::{
    bulk::{
        BulkConfig, BulkDayTradeFetcher,
        streaming::{
            DEFAULT_BATCH_SIZE, parse_zip_csv_into_sender, parse_zip_csv_stream,
            response_to_async_read,
        },
    },
    error::DataError,
    trade::RestTrade,
};
use chrono::NaiveDate;
use std::{future::Future, pin::Pin};

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

    /// Download and parse trades using streaming ZIP+CSV.
    ///
    /// The HTTP response body is streamed through `async_zip` for decompression
    /// and then into `csv-async` for record-by-record parsing. No intermediate
    /// ZIP or CSV buffers are allocated.
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

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("OKX bulk request failed: {e}"),
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

        let buf_reader = response_to_async_read(resp);
        let trades = parse_zip_csv_stream::<_, trades::OkxBulkTrade, _>(
            buf_reader,
            true,  // OKX CSVs have headers
            false, // no flexible columns needed
            RestTrade::try_from,
        )
        .await?;

        Ok(Some(trades))
    }

    /// Stream and parse trades through a channel in batches.
    ///
    /// Same as [`download_and_parse_trades`](Self::download_and_parse_trades)
    /// but sends batches through `tx` instead of collecting into a `Vec`,
    /// keeping memory bounded regardless of day size.
    ///
    /// Returns `Ok(true)` if data was found, `Ok(false)` if 404.
    async fn stream_and_parse_trades(
        &self,
        market: &str,
        date: NaiveDate,
        tx: &tokio::sync::mpsc::Sender<Vec<RestTrade>>,
    ) -> Result<bool, DataError> {
        let date_folder = date.format("%Y%m%d");
        let date_file = date.format("%Y-%m-%d");
        let url = format!(
            "https://www.okx.com/cdn/okex/traderecords/trades/daily/{date_folder}/{market}-trades-{date_file}.zip"
        );

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| DataError::Http {
                status: None,
                url: url.clone(),
                message: format!("OKX bulk request failed: {e}"),
            })?;

        let status = resp.status();

        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }

        if !status.is_success() {
            return Err(DataError::Http {
                status: Some(status.as_u16()),
                url: url.clone(),
                message: format!("OKX bulk HTTP {status} for {url}"),
            });
        }

        let buf_reader = response_to_async_read(resp);
        parse_zip_csv_into_sender::<_, trades::OkxBulkTrade, _>(
            buf_reader,
            true,  // OKX CSVs have headers
            false, // no flexible columns needed
            RestTrade::try_from,
            tx,
            DEFAULT_BATCH_SIZE,
        )
        .await?;

        Ok(true)
    }
}

/// Download and parse trades from an OKX monthly archive using streaming ZIP+CSV.
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

    let resp = client.get(&url).send().await.map_err(|e| DataError::Http {
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

    let buf_reader = response_to_async_read(resp);
    let trades = parse_zip_csv_stream::<_, trades::OkxBulkTrade, _>(
        buf_reader,
        true,
        false,
        RestTrade::try_from,
    )
    .await?;

    Ok(Some(trades))
}

impl Default for OkxBulkClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BulkDayTradeFetcher for OkxBulkClient {
    #[tracing::instrument(skip(self), fields(exchange = "okx", %date))]
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
        Box::pin(self.stream_and_parse_trades(market, date, tx))
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
