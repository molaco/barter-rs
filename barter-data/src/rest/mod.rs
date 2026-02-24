pub mod retry;

use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
};
use chrono::{DateTime, Utc};
use futures::Stream;
use std::future::Future;

/// Request parameters for fetching historical kline/candlestick data.
#[derive(Clone, Debug)]
pub struct KlineRequest {
    /// Exchange-specific market string (e.g., "BTCUSDT").
    pub market: String,
    /// Candlestick interval period.
    pub interval: Interval,
    /// Optional start time filter.
    pub start: Option<DateTime<Utc>>,
    /// Optional end time filter.
    pub end: Option<DateTime<Utc>>,
    /// Optional limit on the number of klines to return.
    pub limit: Option<u32>,
}

/// Trait for fetching historical kline/candlestick data from an exchange.
pub trait KlineFetcher {
    /// Fetch a single batch of klines for the given request parameters.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl Future<Output = Result<Vec<Candle>, DataError>> + Send;

    /// Stream paginated batches of klines for the given request parameters.
    fn stream_klines(
        &self,
        request: KlineRequest,
    ) -> impl Stream<Item = Result<Vec<Candle>, DataError>> + Send;
}
