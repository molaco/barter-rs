use crate::retry::RetryPolicy;
use std::path::PathBuf;

/// Central configuration for collector orchestration.
///
/// Owns concurrency, caching, retry, and pagination limits that were
/// previously scattered across `BulkConfig`, `RetryPolicy` fields on
/// individual clients, and `MAX_TRADE_PAGES` constants.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CollectorConfig {
    /// Maximum number of concurrent bulk downloads.
    pub concurrency: usize,
    /// Optional directory for caching downloaded files and `.verified` markers.
    pub cache_dir: Option<PathBuf>,
    /// Retry policy for all fetcher calls.
    pub retry: RetryPolicy,
    /// Maximum number of trade pagination pages before aborting (safety limit).
    pub max_trade_pages: u32,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            concurrency: 4,
            cache_dir: None,
            retry: RetryPolicy::default(),
            max_trade_pages: 10_000,
        }
    }
}
