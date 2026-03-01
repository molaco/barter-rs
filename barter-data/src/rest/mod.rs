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
#[derive(Clone, Debug, PartialEq)]
pub struct KlineRequest {
    /// Exchange-specific market string (e.g., "BTCUSDT").
    pub market: String,
    /// Candlestick interval period.
    pub interval: Interval,
    /// Optional start time filter (inclusive). Klines at or after this time are included.
    pub start: Option<DateTime<Utc>>,
    /// Optional end time filter (inclusive). Klines at or before this time are included.
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
#[derive(Clone, Debug, PartialEq)]
pub struct TradeRequest {
    /// Exchange-specific market string (e.g., "BTCUSDT").
    pub market: String,
    /// Optional start time filter (inclusive). Trades at or after this time are included.
    pub start: Option<DateTime<Utc>>,
    /// Optional end time filter (inclusive). Trades at or before this time are included.
    pub end: Option<DateTime<Utc>>,
    /// Optional limit on the number of trades to return per batch.
    pub limit: Option<u32>,
}

/// Trait for fetching historical trades from an exchange REST API.
///
/// Implementations provide access to historical trade data via single-batch
/// fetching ([`TradeFetcher::fetch_trades`]) and paginated streaming
/// ([`TradeFetcher::stream_trades`]). Not all exchanges support all features;
/// for example, some exchanges do not support time-based filtering and will
/// ignore the `start`/`end` fields on [`TradeRequest`], returning only the
/// most recent trades instead.
///
/// # Exchange Support
///
/// Known exchange-specific limitations:
///
/// - **Binance**: Full pagination via aggregate trade ID cursor (`fromId`).
/// - **Bybit**: Single batch only, no time filtering.
/// - **Coinbase**: Time filtering with client-side validation.
/// - **Kraken**: Full pagination via trade ID cursor.
/// - **OKX**: Full pagination with 10K page safety limit.
pub trait TradeFetcher {
    /// Fetch a single batch of trades for the given request parameters.
    ///
    /// Returns a `Vec<RestTrade>` representing one page of historical trades.
    /// The `start` and `end` fields on [`TradeRequest`] are inclusive when the
    /// exchange supports time filtering. Exchanges that do not support time
    /// filtering (e.g., Bybit) will return recent trades and log a warning.
    ///
    /// Results should be sorted oldest-first when the exchange provides
    /// ordering guarantees. An empty `Vec` indicates that no trades exist in
    /// the requested range.
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> impl Future<Output = Result<Vec<RestTrade>, DataError>> + Send;

    /// Stream paginated batches of trades in chronological order (oldest-first).
    ///
    /// Each yielded item is a batch `Vec<RestTrade>`. The stream terminates
    /// when all trades in the requested time range have been fetched, or when
    /// the exchange reports that no more data is available.
    ///
    /// Not all exchanges support full pagination. For example, Bybit yields a
    /// single batch. Callers should not assume that all trades in a time range
    /// will be returned — some exchanges impose per-page or total-page limits.
    fn stream_trades(
        &self,
        request: TradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send;
}
