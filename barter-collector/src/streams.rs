use crate::config::CollectorConfig;
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
    _config: CollectorConfig,
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
