use crate::error::DataError;
use std::{future::Future, time::Duration};
use tokio::time::sleep;

/// Configuration for exponential backoff retry.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Initial backoff duration.
    pub initial_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Multiplier applied after each failed attempt.
    pub multiplier: u32,
    /// Maximum number of retry attempts.
    pub max_retries: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            multiplier: 2,
            max_retries: 3,
        }
    }
}

/// Execute a future-producing closure with exponential backoff retry.
///
/// The `should_retry` closure determines whether a given error is retriable.
/// Returns the first success, or the last error after all retries are exhausted.
pub async fn retry_with_backoff<F, Fut, T, E>(
    policy: &RetryPolicy,
    should_retry: impl Fn(&E) -> bool,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
{
    let mut backoff = policy.initial_backoff;

    for _ in 0..policy.max_retries {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(err) if should_retry(&err) => {
                sleep(backoff).await;
                backoff = backoff
                    .saturating_mul(policy.multiplier)
                    .min(policy.max_backoff);
            }
            Err(err) => return Err(err),
        }
    }

    // Final attempt after all retries.
    operation().await
}

/// Determine whether a [`DataError`] should be retried.
///
/// Retries on server errors and rate limits. Does not retry on
/// client errors (bad request, invalid symbol, etc).
pub fn is_retriable_data_error(error: &DataError) -> bool {
    match error {
        DataError::Http { status, message, .. } => match status {
            Some(429) => true, // Too Many Requests
            Some(418) => true, // Binance uses 418 for IP auto-ban on rate limit violation
            Some(500) => true,   // Internal Server Error
            Some(502) => true,   // Bad Gateway
            Some(503) => true,   // Service Unavailable
            Some(504) => true,   // Gateway Timeout
            Some(416) => true,   // Range Not Satisfiable (retry from scratch)
            None => {
                // Connection-level failures (no HTTP response received)
                let msg = message.to_lowercase();
                msg.contains("timeout")
                    || msg.contains("connection")
                    || msg.contains("reset by peer")
                    || msg.contains("broken pipe")
                    || msg.contains("error sending request")
            }
            _ => false,
        },
        DataError::ExchangeApi { .. } => false,
        DataError::DataParse(_) => false,
        DataError::BulkArchive(_) => false,
        DataError::ChecksumMismatch { .. } => false,
        DataError::UnsupportedInterval { .. } => false,
        DataError::Io(_) => false,
        DataError::PaginationLimit { .. } => false,
        // Legacy socket errors from WS layer
        DataError::Socket(msg) => {
            let msg = msg.to_lowercase();
            msg.contains("timeout")
                || msg.contains("connection")
                || msg.contains("reset by peer")
                || msg.contains("broken pipe")
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    };

    #[test]
    fn test_default_retry_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(30));
        assert_eq!(policy.multiplier, 2);
        assert_eq!(policy.max_retries, 3);
    }

    #[tokio::test]
    async fn test_retry_succeeds_immediately() {
        let policy = RetryPolicy::default();
        let result: Result<&str, &str> =
            retry_with_backoff(&policy, |_: &&str| true, || async { Ok("ok") }).await;
        assert_eq!(result, Ok("ok"));
    }

    #[tokio::test]
    async fn test_retry_non_retriable_error_returns_immediately() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let policy = RetryPolicy::default();
        let result: Result<(), &str> = retry_with_backoff(
            &policy,
            |_: &&str| false,
            move || {
                attempts_clone.fetch_add(1, Ordering::SeqCst);
                async { Err("non-retriable") }
            },
        )
        .await;

        assert_eq!(result, Err("non-retriable"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let policy = RetryPolicy {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            multiplier: 2,
            max_retries: 3,
        };

        let result: Result<&str, &str> = retry_with_backoff(
            &policy,
            |_: &&str| true,
            move || {
                let count = attempts_clone.fetch_add(1, Ordering::SeqCst);
                async move {
                    if count < 2 {
                        Err("retriable")
                    } else {
                        Ok("success")
                    }
                }
            },
        )
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_exhausts_all_retries() {
        let attempts = Arc::new(AtomicU32::new(0));
        let attempts_clone = Arc::clone(&attempts);

        let policy = RetryPolicy {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            multiplier: 2,
            max_retries: 3,
        };

        let result: Result<(), &str> = retry_with_backoff(
            &policy,
            |_: &&str| true,
            move || {
                attempts_clone.fetch_add(1, Ordering::SeqCst);
                async { Err("always fails") }
            },
        )
        .await;

        assert_eq!(result, Err("always fails"));
        // max_retries (3) + 1 final attempt = 4 total
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn test_retriable_http_429() {
        let err = DataError::Http { status: Some(429), url: "test".into(), message: "rate limit".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_http_418() {
        let err = DataError::Http { status: Some(418), url: "test".into(), message: "IP banned".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_http_5xx() {
        for code in [500u16, 502, 503, 504] {
            let err = DataError::Http { status: Some(code), url: "test".into(), message: "server error".into() };
            assert!(
                is_retriable_data_error(&err),
                "Expected retriable for HTTP {}",
                code
            );
        }
    }

    #[test]
    fn test_retriable_http_416() {
        let err = DataError::Http { status: Some(416), url: "test".into(), message: "range not satisfiable".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_http_400() {
        let err = DataError::Http { status: Some(400), url: "test".into(), message: "bad request".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_http_401() {
        let err = DataError::Http { status: Some(401), url: "test".into(), message: "unauthorized".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_http_403() {
        let err = DataError::Http { status: Some(403), url: "test".into(), message: "forbidden".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_http_404() {
        let err = DataError::Http { status: Some(404), url: "test".into(), message: "not found".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_connection_failure() {
        let err = DataError::Http { status: None, url: "test".into(), message: "connection reset by peer".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_connection_timeout() {
        let err = DataError::Http { status: None, url: "test".into(), message: "request timeout".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_connection_broken_pipe() {
        let err = DataError::Http { status: None, url: "test".into(), message: "broken pipe".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_connection_error_sending_request() {
        let err = DataError::Http { status: None, url: "test".into(), message: "error sending request for url".into() };
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_connection_unknown_message() {
        let err = DataError::Http { status: None, url: "test".into(), message: "some unknown error".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_parse() {
        let err = DataError::DataParse("invalid timestamp".into());
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_exchange_api() {
        let err = DataError::ExchangeApi { exchange: "okx".into(), code: "51001".into(), message: "not found".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_bulk_archive() {
        let err = DataError::BulkArchive("corrupt zip".into());
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_checksum_mismatch() {
        let err = DataError::ChecksumMismatch { expected: "abc".into(), actual: "def".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_unsupported_interval() {
        let err = DataError::UnsupportedInterval { exchange: "binance".into(), interval: "3m".into() };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_io() {
        let err = DataError::Io("disk full".into());
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_pagination_limit() {
        let err = DataError::PaginationLimit { exchange: "binance".into(), market: "btcusdt".into(), limit: 1000 };
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_socket_timeout() {
        let err = DataError::Socket("connection timeout".to_string());
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_retriable_socket_connection() {
        let err = DataError::Socket("connection refused".to_string());
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_socket_other() {
        let err = DataError::Socket("unknown ws error".to_string());
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_not_retriable_other_variant() {
        let err = DataError::SubscriptionsEmpty;
        assert!(!is_retriable_data_error(&err));
    }

    #[tokio::test]
    async fn test_retry_backoff_overflow_safety() {
        // initial_backoff * u32::MAX would overflow without saturating_mul;
        // max_backoff caps the result so the sleep stays short.
        let policy = RetryPolicy {
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(1),
            multiplier: u32::MAX,
            max_retries: 2,
        };

        let result: Result<(), &str> = retry_with_backoff(
            &policy,
            |_: &&str| true,
            || async { Err("fail") },
        )
        .await;

        assert_eq!(result, Err("fail"));
        // Key: did not panic from Duration overflow
    }
}
