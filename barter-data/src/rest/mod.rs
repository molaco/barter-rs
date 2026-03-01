pub mod retry;

use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
};
use barter_instrument::Side;
use chrono::{DateTime, Utc};
use futures::Stream;
use serde::{Deserialize, Serialize};
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
    /// Return the list of [`Interval`]s that this exchange supports for REST
    /// kline fetching.
    ///
    /// This is a static method (no `&self`) because the set of supported
    /// intervals is determined by the exchange, not by a particular client
    /// instance.
    fn supported_intervals() -> &'static [Interval];

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

/// A single trade returned by a REST API, with timestamp included.
///
/// Distinct from [`PublicTrade`] because REST trade responses always include
/// timestamps directly, whereas `PublicTrade` omits `time` (it's provided by
/// the `MarketEvent` wrapper in the WebSocket path).
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct RestTrade {
    pub id: String,
    pub time: DateTime<Utc>,
    pub price: f64,
    pub amount: f64,
    pub side: Side,
}

/// Request parameters for fetching historical trades.
#[derive(Clone, Debug)]
pub struct TradeRequest {
    /// Exchange-specific market string (e.g., "BTCUSDT").
    pub market: String,
    /// Optional start time filter.
    pub start: Option<DateTime<Utc>>,
    /// Optional end time filter.
    pub end: Option<DateTime<Utc>>,
    /// Optional limit on the number of trades to return per batch.
    pub limit: Option<u32>,
}

/// Trait for fetching historical trades from an exchange REST API.
pub trait TradeFetcher {
    /// Fetch a single batch of trades for the given request parameters.
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> impl Future<Output = Result<Vec<RestTrade>, DataError>> + Send;

    /// Stream paginated batches of trades in chronological order.
    /// Each batch is a `Vec<RestTrade>` sorted oldest-first.
    fn stream_trades(
        &self,
        request: TradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send;
}
