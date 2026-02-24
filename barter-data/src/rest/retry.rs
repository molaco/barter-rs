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
                backoff = (backoff * policy.multiplier).min(policy.max_backoff);
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
        DataError::Socket(msg) => {
            let lower = msg.to_lowercase();
            lower.contains("timeout")
                || lower.contains("429")
                || lower.contains("418")
                || lower.contains("rate")
                || lower.contains("500")
                || lower.contains("502")
                || lower.contains("503")
                || lower.contains("504")
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
    fn test_is_retriable_data_error_timeout() {
        let err = DataError::Socket("connection timeout".to_string());
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_is_retriable_data_error_rate_limit_429() {
        let err = DataError::Socket("HTTP 429 Too Many Requests".to_string());
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_is_retriable_data_error_rate_limit_418() {
        let err = DataError::Socket("HTTP 418 IP banned".to_string());
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_is_retriable_data_error_rate_word() {
        let err = DataError::Socket("rate limit exceeded".to_string());
        assert!(is_retriable_data_error(&err));
    }

    #[test]
    fn test_is_retriable_data_error_server_errors() {
        for code in &["500", "502", "503", "504"] {
            let err = DataError::Socket(format!("HTTP {} Internal Server Error", code));
            assert!(
                is_retriable_data_error(&err),
                "Expected retriable for {}",
                code
            );
        }
    }

    #[test]
    fn test_is_retriable_data_error_non_retriable_socket() {
        let err = DataError::Socket("HTTP 400 Bad Request".to_string());
        assert!(!is_retriable_data_error(&err));
    }

    #[test]
    fn test_is_retriable_data_error_non_socket_variant() {
        let err = DataError::SubscriptionsEmpty;
        assert!(!is_retriable_data_error(&err));
    }
}
