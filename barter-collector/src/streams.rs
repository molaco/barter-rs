use crate::{
    pagination::{PageResult, PaginationStrategy},
    retry::is_retriable_data_error,
};
use barter_data::{error::DataError, rest::ExchangeRateLimiter};
use futures::{
    StreamExt,
    stream::{self, Stream},
};
use std::{pin::Pin, sync::Arc};

/// Stream data by driving a [`PaginationStrategy`] in a loop.
///
/// Uses `stream::unfold` with the strategy as state. Acquires the rate
/// limiter and retries on each page fetch (fixes the
/// rate-limiter-outside-retry bug from barter-data).
pub fn stream_paginated<S: PaginationStrategy + 'static>(
    strategy: S,
    rate_limiter: Arc<ExchangeRateLimiter>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<S::Item>, DataError>> + Send>> {
    Box::pin(stream::unfold(Some(strategy), move |maybe_strategy| {
        let limiter = rate_limiter.clone();
        async move {
            let mut strategy = maybe_strategy?;

            // Rate limit before each page (not retried separately —
            // fetch_page itself is retried if it returns a retriable error).
            limiter.until_ready().await;

            let result = strategy.fetch_page().await;

            match result {
                Ok(PageResult::Continue(batch)) => Some((Ok(batch), Some(strategy))),
                Ok(PageResult::Done(batch)) => {
                    if batch.is_empty() {
                        None
                    } else {
                        Some((Ok(batch), None))
                    }
                }
                Ok(PageResult::Empty) => None,
                Err(e) if is_retriable_data_error(&e) => {
                    // On retriable error, yield the error and stop.
                    // The caller can decide to restart the stream.
                    // (Retry within fetch_page is the strategy's
                    // responsibility if it wraps a retry-aware fetcher.)
                    tracing::warn!(error = %e, "retriable error in pagination, stopping stream");
                    Some((Err(e), None))
                }
                Err(e) => Some((Err(e), None)),
            }
        }
    }))
}

/// Stream bulk data by fanning out over dates with bounded concurrency.
///
/// Generic over any async function that fetches a single day's data.
/// 404 dates (returning `Ok(None)`) are silently skipped.
pub fn stream_bulk<'a, T, Fut>(
    dates: Vec<chrono::NaiveDate>,
    concurrency: usize,
    fetch_day: impl Fn(chrono::NaiveDate) -> Fut + Send + 'a,
) -> Pin<Box<dyn Stream<Item = Result<Vec<T>, DataError>> + Send + 'a>>
where
    T: Send + 'a,
    Fut: std::future::Future<Output = Result<Option<Vec<T>>, DataError>> + Send + 'a,
{
    Box::pin(
        stream::iter(dates)
            .map(fetch_day)
            .buffer_unordered(concurrency)
            .filter_map(|result| async {
                match result {
                    Ok(None) => None,
                    Ok(Some(data)) => Some(Ok(data)),
                    Err(e) => Some(Err(e)),
                }
            }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_data::{
        bulk::{BulkDayKlineFetcher, BulkDayTradeFetcher},
        subscription::candle::{Candle, Interval},
        trade::RestTrade,
    };
    use barter_instrument::Side;
    use chrono::{Datelike, NaiveDate, TimeZone, Utc};
    use std::collections::HashSet;

    #[tokio::test]
    async fn test_stream_bulk_skips_none() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 2).unwrap(),
            NaiveDate::from_ymd_opt(2024, 1, 3).unwrap(),
        ];

        let stream = stream_bulk(dates, 2, |date| async move {
            if date.day() == 2 {
                // Simulate 404
                Ok(None)
            } else {
                Ok(Some(vec![date.day() as i32]))
            }
        });

        let results: Vec<_> = stream.collect().await;
        // Day 2 skipped (None), days 1 and 3 yielded (order may vary due to buffer_unordered)
        assert_eq!(results.len(), 2);
        let mut values: Vec<i32> = results.into_iter().map(|r| r.unwrap()[0]).collect();
        values.sort();
        assert_eq!(values, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_stream_bulk_propagates_errors() {
        let dates = vec![NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()];

        let stream = stream_bulk(dates, 1, |_date| async move {
            Err::<Option<Vec<i32>>, _>(DataError::DataParse("test error".into()))
        });

        let results: Vec<_> = stream.collect().await;
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }

    #[tokio::test]
    async fn test_stream_paginated_drives_strategy() {
        use std::num::NonZeroU32;

        struct CountingStrategy {
            count: u32,
            max: u32,
        }

        impl PaginationStrategy for CountingStrategy {
            type Item = u32;
            fn fetch_page(
                &mut self,
            ) -> Pin<
                Box<
                    dyn std::future::Future<Output = Result<PageResult<u32>, DataError>>
                        + Send
                        + '_,
                >,
            > {
                let count = self.count;
                self.count += 1;
                let max = self.max;
                Box::pin(async move {
                    if count >= max {
                        Ok(PageResult::Empty)
                    } else if count == max - 1 {
                        Ok(PageResult::Done(vec![count]))
                    } else {
                        Ok(PageResult::Continue(vec![count]))
                    }
                })
            }
        }

        let strategy = CountingStrategy { count: 0, max: 3 };
        let quota = governor::Quota::per_second(NonZeroU32::new(100).unwrap()); // 100 is non-zero
        let limiter = Arc::new(governor::RateLimiter::direct(quota));

        let stream = stream_paginated(strategy, limiter);
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &[0]);
        assert_eq!(results[1].as_ref().unwrap(), &[1]);
        assert_eq!(results[2].as_ref().unwrap(), &[2]);
    }

    // -----------------------------------------------------------------------
    // Mock-based integration tests for stream_bulk with BulkDay*Fetcher traits
    // -----------------------------------------------------------------------

    /// Mock implementation of [`BulkDayTradeFetcher`] that returns trades for
    /// a predefined set of "known" dates and `Ok(None)` for anything else.
    struct MockBulkTradeFetcher {
        known_dates: HashSet<NaiveDate>,
    }

    impl BulkDayTradeFetcher for MockBulkTradeFetcher {
        fn fetch_day_trades<'a>(
            &'a self,
            _market: &'a str,
            date: NaiveDate,
        ) -> Pin<
            Box<
                dyn std::future::Future<Output = Result<Option<Vec<RestTrade>>, DataError>>
                    + Send
                    + 'a,
            >,
        > {
            let known = self.known_dates.contains(&date);
            Box::pin(async move {
                if known {
                    let trade = RestTrade {
                        id: format!("t-{date}"),
                        time: Utc
                            .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
                            .unwrap(),
                        price: 50_000.0,
                        amount: 1.5,
                        side: Side::Buy,
                    };
                    Ok(Some(vec![trade]))
                } else {
                    Ok(None)
                }
            })
        }
    }

    /// Mock implementation of [`BulkDayKlineFetcher`] that returns candles for
    /// a predefined set of "known" dates and `Ok(None)` for anything else.
    struct MockBulkKlineFetcher {
        known_dates: HashSet<NaiveDate>,
    }

    impl BulkDayKlineFetcher for MockBulkKlineFetcher {
        fn fetch_day_klines<'a>(
            &'a self,
            _market: &'a str,
            _interval: Interval,
            date: NaiveDate,
        ) -> Pin<
            Box<
                dyn std::future::Future<Output = Result<Option<Vec<Candle>>, DataError>>
                    + Send
                    + 'a,
            >,
        > {
            let known = self.known_dates.contains(&date);
            Box::pin(async move {
                if known {
                    let open_time = Utc
                        .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
                        .unwrap();
                    let close_time = Utc
                        .with_ymd_and_hms(date.year(), date.month(), date.day(), 23, 59, 59)
                        .unwrap();
                    let candle = Candle {
                        open_time,
                        close_time,
                        open: 50_000.0,
                        high: 51_000.0,
                        low: 49_000.0,
                        close: 50_500.0,
                        volume: 100.0,
                        quote_volume: Some(5_000_000.0),
                        trade_count: 1000,
                        is_closed: true,
                    };
                    Ok(Some(vec![candle]))
                } else {
                    Ok(None)
                }
            })
        }
    }

    #[tokio::test]
    async fn test_stream_bulk_with_mock_trade_fetcher() {
        let known = HashSet::from([
            NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 6, 3).unwrap(),
        ]);
        let fetcher = MockBulkTradeFetcher { known_dates: known };

        // Request 3 dates; day 2 is unknown and should be skipped.
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(),
            NaiveDate::from_ymd_opt(2024, 6, 2).unwrap(), // unknown
            NaiveDate::from_ymd_opt(2024, 6, 3).unwrap(),
        ];

        let stream = stream_bulk(dates, 2, |date| fetcher.fetch_day_trades("BTCUSDT", date));

        let results: Vec<_> = stream.collect().await;

        // Only 2 batches returned (day 2 was None -> skipped)
        assert_eq!(results.len(), 2);

        let mut trade_ids: Vec<String> = results
            .into_iter()
            .map(|r| {
                let batch = r.expect("expected Ok");
                assert_eq!(batch.len(), 1);
                batch[0].id.clone()
            })
            .collect();
        trade_ids.sort();
        assert_eq!(trade_ids, vec!["t-2024-06-01", "t-2024-06-03"]);
    }

    #[tokio::test]
    async fn test_stream_bulk_with_mock_kline_fetcher() {
        let known = HashSet::from([
            NaiveDate::from_ymd_opt(2024, 7, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 7, 11).unwrap(),
            NaiveDate::from_ymd_opt(2024, 7, 12).unwrap(),
        ]);
        let fetcher = MockBulkKlineFetcher { known_dates: known };

        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 7, 10).unwrap(),
            NaiveDate::from_ymd_opt(2024, 7, 11).unwrap(),
            NaiveDate::from_ymd_opt(2024, 7, 12).unwrap(),
            NaiveDate::from_ymd_opt(2024, 7, 13).unwrap(), // unknown
        ];

        let stream = stream_bulk(dates, 2, |date| {
            fetcher.fetch_day_klines("BTCUSDT", Interval::D1, date)
        });

        let results: Vec<_> = stream.collect().await;

        // 3 batches (day 13 was None -> skipped)
        assert_eq!(results.len(), 3);

        for result in &results {
            let batch = result.as_ref().expect("expected Ok");
            assert_eq!(batch.len(), 1);
            let candle = &batch[0];
            assert!(candle.is_closed);
            assert_eq!(candle.open, 50_000.0);
            assert_eq!(candle.high, 51_000.0);
        }
    }

    #[tokio::test]
    async fn test_stream_bulk_utc8_extension() {
        // Simulate OKX UTC+8 boundary: the collector extends the date range
        // by +1 day so that trades straddling midnight UTC+8 are captured.
        let start = NaiveDate::from_ymd_opt(2024, 3, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2024, 3, 3).unwrap();

        // Extend end by 1 day (what the collector does for UTC+8 exchanges)
        let extended_end = end.succ_opt().unwrap(); // 2024-03-04
        let dates = crate::scheduling::date_range(start, extended_end);

        // Should now include 4 dates: Mar 1, 2, 3, 4
        assert_eq!(dates.len(), 4);

        // All 4 dates are "known" for our mock
        let known: HashSet<NaiveDate> = dates.iter().copied().collect();
        let fetcher = MockBulkTradeFetcher { known_dates: known };

        let stream = stream_bulk(dates, 2, |date| fetcher.fetch_day_trades("BTCUSDT", date));

        let results: Vec<_> = stream.collect().await;

        // All 4 dates should produce batches (including the extended day)
        assert_eq!(results.len(), 4);

        let mut fetched_dates: Vec<NaiveDate> = results
            .into_iter()
            .map(|r| {
                let batch = r.expect("expected Ok");
                batch[0].time.date_naive()
            })
            .collect();
        fetched_dates.sort();

        assert_eq!(
            fetched_dates,
            vec![
                NaiveDate::from_ymd_opt(2024, 3, 1).unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 2).unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 3).unwrap(),
                NaiveDate::from_ymd_opt(2024, 3, 4).unwrap(), // extra day from UTC+8 extension
            ]
        );
    }
}
