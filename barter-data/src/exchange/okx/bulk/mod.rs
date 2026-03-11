pub mod trades;

use crate::{
    bulk::{BulkConfig, BulkTradeFetcher, BulkTradeRequest, date_range},
    error::DataError,
    retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    trade::RestTrade,
};
use chrono::{Datelike, NaiveDate};
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

/// Download and parse trades from an OKX monthly archive.
///
/// Monthly archive URL:
/// `https://static.okx.com/cdn/okex/traderecords/trades/monthly/{instrument}/{instrument}-trades-{YYYY-MM}.zip`
///
/// Returns `Ok(None)` if the monthly file returns 404 (not available).
/// The ZIP contains CSV files with the same format as the daily archives.
pub async fn download_monthly_trades(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    instrument_id: &str,
    year: i32,
    month: u32,
) -> Result<Option<Vec<RestTrade>>, DataError> {
    let url = format!(
        "https://static.okx.com/cdn/okex/traderecords/trades/monthly/{instrument_id}/{instrument_id}-trades-{year}-{month:02}.zip",
    );

    let client_clone = client.clone();
    let url_clone = url.clone();

    let response = retry_with_backoff(retry, is_retriable_data_error, || {
        let client = client_clone.clone();
        let url = url_clone.clone();
        async move {
            let resp = client
                .get(&url)
                .send()
                .await
                .map_err(|e| DataError::Socket(format!("OKX monthly request failed: {e}")))?;

            let status = resp.status();

            if status == reqwest::StatusCode::NOT_FOUND {
                return Ok(None);
            }

            if !status.is_success() {
                return Err(DataError::Socket(format!(
                    "OKX monthly HTTP {status} for {url}"
                )));
            }

            let bytes = resp
                .bytes()
                .await
                .map_err(|e| DataError::Socket(format!("OKX monthly read body failed: {e}")))?;

            Ok(Some(bytes))
        }
    })
    .await?;

    let bytes = match response {
        Some(b) => b,
        None => return Ok(None),
    };

    let cursor = std::io::Cursor::new(&bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| DataError::Socket(format!("OKX monthly ZIP open failed: {e}")))?;

    if archive.len() == 0 {
        tracing::warn!("OKX monthly ZIP archive is empty for {instrument_id} {year}-{month:02}");
        return Ok(Some(Vec::new()));
    }

    let mut csv_data = Vec::new();
    archive
        .by_index(0)
        .map_err(|e| DataError::Socket(format!("OKX monthly ZIP read failed: {e}")))?
        .read_to_end(&mut csv_data)
        .map_err(|e| DataError::Socket(format!("OKX monthly ZIP extract failed: {e}")))?;

    let trades = trades::parse_trades(&csv_data)?;
    Ok(Some(trades))
}

/// Choose monthly archives for complete months, daily for partial months.
///
/// Given an inclusive date range `[start, end]`, returns:
/// - `complete_months`: `Vec<(year, month)>` for months entirely within the range.
/// - `remaining_days`: `Vec<NaiveDate>` for days in partial months at the boundaries.
///
/// A month is "complete" if both its first and last day fall within `[start, end]`.
pub fn partition_date_range(start: NaiveDate, end: NaiveDate) -> (Vec<(i32, u32)>, Vec<NaiveDate>) {
    if start > end {
        return (Vec::new(), Vec::new());
    }

    let mut complete_months = Vec::new();
    let mut remaining_days = Vec::new();

    let mut current = start;
    while current <= end {
        let year = current.year();
        let month = current.month();
        let first_of_month = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let last_of_month = last_day_of_month(year, month);

        if first_of_month >= start && last_of_month <= end {
            // This entire month is within range -- use monthly archive
            complete_months.push((year, month));
            // Advance past this month
            current = if month == 12 {
                match NaiveDate::from_ymd_opt(year + 1, 1, 1) {
                    Some(d) => d,
                    None => break,
                }
            } else {
                NaiveDate::from_ymd_opt(year, month + 1, 1).unwrap()
            };
        } else {
            // Partial month -- add individual days
            let month_end = last_of_month.min(end);
            while current <= month_end {
                remaining_days.push(current);
                current = match current.succ_opt() {
                    Some(d) => d,
                    None => break,
                };
            }
        }
    }

    (complete_months, remaining_days)
}

/// Return the last day of the given month.
fn last_day_of_month(year: i32, month: u32) -> NaiveDate {
    if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap()
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
            .unwrap()
            .pred_opt()
            .unwrap()
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
            cache_dir: None,
        };
        let client = OkxBulkClient::with_config(config);
        assert_eq!(client.config.concurrency, 16);
        assert!(!client.config.verify_checksum);
    }

    // -----------------------------------------------------------------------
    // partition_date_range tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_partition_single_complete_month() {
        let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 3)]);
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_partial_month_start() {
        // Start mid-January, end at end of February
        let start = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 2, 29).unwrap();
        let (months, days) = partition_date_range(start, end);
        // February is complete (2024 is a leap year), January is partial
        assert_eq!(months, vec![(2024, 2)]);
        assert_eq!(days.len(), 17); // Jan 15..=31 = 17 days
        assert_eq!(days[0], NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(days[16], NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
    }

    #[test]
    fn test_partition_partial_month_end() {
        // Start at beginning of March, end mid-April
        let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 4, 15).unwrap();
        let (months, days) = partition_date_range(start, end);
        // March is complete, April is partial
        assert_eq!(months, vec![(2024, 3)]);
        assert_eq!(days.len(), 15); // Apr 1..=15 = 15 days
        assert_eq!(days[0], NaiveDate::from_ymd_opt(2024, 4, 1).unwrap());
        assert_eq!(days[14], NaiveDate::from_ymd_opt(2024, 4, 15).unwrap());
    }

    #[test]
    fn test_partition_multiple_complete_months() {
        let start = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 31).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 1), (2024, 2), (2024, 3)]);
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_no_complete_months() {
        // Mid-month to mid-month within same month
        let start = NaiveDate::from_ymd_opt(2024, 5, 10).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 5, 20).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert!(months.is_empty());
        assert_eq!(days.len(), 11); // May 10..=20
    }

    #[test]
    fn test_partition_cross_year_boundary() {
        let start = NaiveDate::from_ymd_opt(2024, 12, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2025, 1, 31).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 12), (2025, 1)]);
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_single_day() {
        let d = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let (months, days) = partition_date_range(d, d);
        assert!(months.is_empty());
        assert_eq!(days, vec![d]);
    }

    #[test]
    fn test_partition_empty_when_start_after_end() {
        let start = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert!(months.is_empty());
        assert!(days.is_empty());
    }

    #[test]
    fn test_partition_both_partial_months() {
        // Mid-January to mid-March: February is complete, Jan and Mar partial
        let start = NaiveDate::from_ymd_opt(2024, 1, 20).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 10).unwrap();
        let (months, days) = partition_date_range(start, end);
        assert_eq!(months, vec![(2024, 2)]);
        // Jan 20..=31 = 12 days + Mar 1..=10 = 10 days = 22 days
        assert_eq!(days.len(), 22);
        assert_eq!(days[0], NaiveDate::from_ymd_opt(2024, 1, 20).unwrap());
        assert_eq!(days[11], NaiveDate::from_ymd_opt(2024, 1, 31).unwrap());
        assert_eq!(days[12], NaiveDate::from_ymd_opt(2024, 3, 1).unwrap());
        assert_eq!(days[21], NaiveDate::from_ymd_opt(2024, 3, 10).unwrap());
    }

    // -----------------------------------------------------------------------
    // last_day_of_month tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_last_day_of_month_regular() {
        assert_eq!(
            last_day_of_month(2024, 1),
            NaiveDate::from_ymd_opt(2024, 1, 31).unwrap()
        );
        assert_eq!(
            last_day_of_month(2024, 4),
            NaiveDate::from_ymd_opt(2024, 4, 30).unwrap()
        );
    }

    #[test]
    fn test_last_day_of_month_february_leap() {
        assert_eq!(
            last_day_of_month(2024, 2),
            NaiveDate::from_ymd_opt(2024, 2, 29).unwrap()
        );
    }

    #[test]
    fn test_last_day_of_month_february_non_leap() {
        assert_eq!(
            last_day_of_month(2025, 2),
            NaiveDate::from_ymd_opt(2025, 2, 28).unwrap()
        );
    }

    #[test]
    fn test_last_day_of_month_december() {
        assert_eq!(
            last_day_of_month(2024, 12),
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap()
        );
    }
}
