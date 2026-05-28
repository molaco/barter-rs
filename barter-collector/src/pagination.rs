use barter_data::error::DataError;
use std::{future::Future, pin::Pin};

/// Outcome of one pagination step.
#[derive(Debug)]
#[non_exhaustive]
pub enum PageResult<T> {
    /// Yield this batch and continue with the next page.
    Continue(Vec<T>),
    /// Yield this batch and stop — no more pages.
    Done(Vec<T>),
    /// No more data — stop without yielding.
    Empty,
}

/// Strategy for paginating through a fetcher.
///
/// Each exchange provides its own implementation. The collector's
/// `stream::unfold` calls `fetch_page` in a loop until `Done`/`Empty`.
///
/// This replaces the 9 per-exchange `PaginationState` structs in
/// `barter-data` with one trait plus one impl per exchange x data-type
/// combination.
///
/// Implementations are stateful (carry cursor, page count, etc.).
pub trait PaginationStrategy: Send {
    /// The type of item yielded by each page (e.g., `Candle` or `RestTrade`).
    type Item: Send;

    /// Fetch one page of data. Advances internal cursor state.
    ///
    /// The collector wraps this call with retry + rate limiting.
    fn fetch_page(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<PageResult<Self::Item>, DataError>> + Send + '_>>;
}
