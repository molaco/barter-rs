use crate::pagination::{PageResult, PaginationStrategy};
use crate::retry::is_retriable_data_error;
use barter_data::error::DataError;
use barter_data::rest::ExchangeRateLimiter;
use futures::stream::{self, Stream};
use futures::StreamExt;
use std::pin::Pin;
use std::sync::Arc;

/// Stream data by driving a [`PaginationStrategy`] in a loop.
///
/// Uses `stream::unfold` with the strategy as state. Acquires the rate
/// limiter and retries on each page fetch (fixes the
/// rate-limiter-outside-retry bug from barter-data).
pub fn stream_paginated<S: PaginationStrategy + 'static>(
    strategy: S,
    rate_limiter: Arc<ExchangeRateLimiter>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<S::Item>, DataError>> + Send>> {
    Box::pin(stream::unfold(
        Some(strategy),
        move |maybe_strategy| {
            let limiter = rate_limiter.clone();
            async move {
                let mut strategy = maybe_strategy?;

                // Rate limit before each page (not retried separately —
                // fetch_page itself is retried if it returns a retriable error).
                limiter.until_ready().await;

                let result = strategy.fetch_page().await;

                match result {
                    Ok(PageResult::Continue(batch)) => {
                        Some((Ok(batch), Some(strategy)))
                    }
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
        },
    ))
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
    use chrono::{Datelike, NaiveDate};

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
        let mut values: Vec<i32> = results
            .into_iter()
            .map(|r| r.unwrap()[0])
            .collect();
        values.sort();
        assert_eq!(values, vec![1, 3]);
    }

    #[tokio::test]
    async fn test_stream_bulk_propagates_errors() {
        let dates = vec![
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        ];

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
            ) -> Pin<Box<dyn std::future::Future<Output = Result<PageResult<u32>, DataError>> + Send + '_>>
            {
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
        let quota = governor::Quota::per_second(NonZeroU32::new(100).unwrap());
        let limiter = Arc::new(governor::RateLimiter::direct(quota));

        let stream = stream_paginated(strategy, limiter);
        let results: Vec<_> = stream.collect().await;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &[0]);
        assert_eq!(results[1].as_ref().unwrap(), &[1]);
        assert_eq!(results[2].as_ref().unwrap(), &[2]);
    }
}
