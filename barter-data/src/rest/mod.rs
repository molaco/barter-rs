use crate::{
    error::DataError,
    subscription::candle::{Candle, Interval},
};
use chrono::{DateTime, Utc};
use std::{future::Future, pin::Pin};

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
pub trait KlineFetcher: Send + Sync {
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
}

pub use crate::trade::RestTrade;

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
    /// Optional cursor carried forward from a previous fetch (e.g., a trade ID
    /// or pagination token) so the exchange can resume from where it left off.
    pub initial_cursor: Option<String>,
}

/// Unified rate-limiter type shared across all exchange REST clients.
///
/// Wrap in `Arc` and pass to multiple clients so that they share a single
/// token bucket.
pub type ExchangeRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// Trait for fetching historical trades from an exchange REST API.
///
/// Implementations provide access to historical trade data via single-batch
/// fetching ([`TradeFetcher::fetch_trades`]). Not all exchanges support all
/// features; for example, some exchanges do not support time-based filtering
/// and will ignore the `start`/`end` fields on [`TradeRequest`], returning
/// only the most recent trades instead.
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
pub trait TradeFetcher: Send + Sync {
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
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>>;

    /// Wait until the exchange-specific rate limiter permits the next request.
    ///
    /// Callers should invoke this **before** each [`fetch_trades`](Self::fetch_trades)
    /// call to respect the exchange's rate limits. The default implementation is a
    /// no-op for backwards compatibility.
    fn wait_for_rate_limit(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    /// Resolve a starting raw trade id at/after `start_ms`, for forward fromId pagination.
    /// Default None = unsupported / not needed.
    fn resolve_start_trade_id(
        &self,
        _market: &str,
        _start_ms: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, DataError>> + Send + '_>> {
        Box::pin(async { Ok(None) })
    }
}
