use crate::{
    error::DataError,
    exchange::RestExchangeServer,
    rest::{
        ExchangeRateLimiter, KlineFetcher, KlineRequest, RestTrade, TradeFetcher, TradeRequest,
        retry::{RetryPolicy, is_retriable_data_error, retry_with_backoff},
    },
    subscription::candle::{Candle, Interval},
};
use barter_integration::protocol::http::{
    HttpParser, public::PublicNoHeaders, rest::client::RestClient,
};
use chrono::{DateTime, TimeDelta, Utc};
use futures::stream::{self, Stream};
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, future::Future, marker::PhantomData, num::NonZeroU32, pin::Pin, sync::Arc};
use tracing::{Instrument, debug, info, warn};

/// Binance kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Binance aggregate trade REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

/// Binance REST API error payload.
///
/// Returned by the Binance API when a request fails, e.g.:
/// ```json
/// { "code": -1121, "msg": "Invalid symbol." }
/// ```
#[derive(Debug, Deserialize)]
pub struct BinanceApiError {
    pub code: i64,
    pub msg: String,
}

/// HTTP response parser for Binance REST API responses.
#[derive(Debug)]
pub struct BinanceHttpParser;

impl HttpParser for BinanceHttpParser {
    type ApiError = BinanceApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::Socket(format!(
            "Binance API error (code {}): {}",
            error.code, error.msg
        ))
    }
}

/// Generic REST client for Binance exchange variants.
///
/// The `Server` type parameter determines which Binance server variant
/// (spot or futures) this client connects to, via the
/// [`RestExchangeServer`] trait.
///
/// Includes a rate limiter configured for Binance weight-based limits
/// (6000 weight per minute, ~5 weight per klines request = 1200 req/min).
#[derive(Clone)]
pub struct BinanceRestClient<Server> {
    pub client: Arc<RestClient<'static, PublicNoHeaders, BinanceHttpParser>>,
    pub rate_limiter: Arc<ExchangeRateLimiter>,
    _server: PhantomData<Server>,
}

impl<Server> fmt::Debug for BinanceRestClient<Server> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
            .finish()
    }
}

impl<Server> Default for BinanceRestClient<Server>
where
    Server: RestExchangeServer,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Server> BinanceRestClient<Server>
where
    Server: RestExchangeServer,
{
    /// Construct a new [`BinanceRestClient`] using the base URL from
    /// [`Server::rest_base_url()`](RestExchangeServer::rest_base_url).
    ///
    /// Initialises a rate limiter with a quota of 1200 requests per minute,
    /// modelling the Binance 6000 weight/min limit at ~5 weight per request.
    pub fn new() -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            PublicNoHeaders,
            BinanceHttpParser,
        );

        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap()).allow_burst(NonZeroU32::new(20).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Construct a new [`BinanceRestClient`] with a shared rate limiter.
    ///
    /// Uses the base URL from [`Server::rest_base_url()`](RestExchangeServer::rest_base_url)
    /// but accepts an externally-owned [`ExchangeRateLimiter`] instead of creating one.
    /// This is useful when multiple clients should share the same rate-limit budget.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            PublicNoHeaders,
            BinanceHttpParser,
        );

        Self {
            client: Arc::new(client),
            rate_limiter,
            _server: PhantomData,
        }
    }
}

impl<Server> BinanceRestClient<Server> {
    /// Construct a [`BinanceRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time. Does not require the `Server` type to implement
    /// [`RestExchangeServer`] since the URL is provided directly.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, BinanceHttpParser);
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap()).allow_burst(NonZeroU32::new(20).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within Binance
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }
}

/// Internal pagination state used by [`KlineFetcher::stream_klines`] to
/// track the cursor position when fetching consecutive batches.
struct PaginationState<Server> {
    client: BinanceRestClient<Server>,
    market: String,
    interval: Interval,
    cursor: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    done: bool,
}

impl<Server> KlineFetcher for BinanceRestClient<Server>
where
    Server: RestExchangeServer + Sync + 'static,
{
    fn supported_intervals() -> &'static [Interval] {
        &[
            Interval::M1,
            Interval::M3,
            Interval::M5,
            Interval::M15,
            Interval::M30,
            Interval::H1,
            Interval::H2,
            Interval::H4,
            Interval::H6,
            Interval::H12,
            Interval::D1,
            Interval::D3,
            Interval::W1,
            Interval::Month1,
        ]
    }

    /// Fetch a single batch of klines from the Binance REST API.
    ///
    /// Builds a [`GetKlines`](klines::GetKlines) request from the provided
    /// [`KlineRequest`], waits for the rate limiter, executes the request with
    /// exponential-backoff retry, and converts raw DTOs into [`Candle`]s.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "binance",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building klines request");

            let get_klines_request = klines::GetKlines {
                path: Server::klines_path(),
                params: klines::GetKlinesParams {
                    symbol: request.market,
                    interval: klines::binance_interval(request.interval).to_string(),
                    start_time: request.start.map(|dt| dt.timestamp_millis()),
                    end_time: request.end.map(|dt| dt.timestamp_millis()),
                    limit: request.limit,
                },
            };

            this.wait_for_rate_limit().await;

            let raw_klines: Vec<klines::BinanceKlineRaw> =
                match retry_with_backoff(&RetryPolicy::default(), is_retriable_data_error, || {
                    let req = get_klines_request.clone();
                    let client = this.client.clone();
                    async move {
                        client
                            .execute(req)
                            .await
                            .map(|(response, _metric)| response)
                    }
                })
                .await
                {
                    Ok(klines) => klines,
                    Err(error) => {
                        warn!(?error, "klines fetch failed");
                        return Err(error);
                    }
                };

            let candles = raw_klines
                .into_iter()
                .map(Candle::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::Socket)?;

            debug!(count = candles.len(), "fetched klines batch");

            Ok(candles)
        }
        .instrument(span)
    }

    /// Stream paginated batches of klines using time-cursor based pagination.
    ///
    /// Uses [`futures::stream::unfold`] to repeatedly call [`fetch_klines`](Self::fetch_klines),
    /// advancing the `startTime` cursor past the last candle's `close_time` in each batch.
    /// The stream terminates when an empty batch is returned, when the cursor passes the
    /// requested end time, or on the first error (which is yielded before stopping).
    fn stream_klines(
        &self,
        request: KlineRequest,
    ) -> impl Stream<Item = Result<Vec<Candle>, DataError>> + Send {
        let state = PaginationState {
            client: self.clone(),
            market: request.market,
            interval: request.interval,
            cursor: request.start.unwrap_or(DateTime::UNIX_EPOCH),
            end: request.end,
            limit: request.limit,
            done: false,
        };

        stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }

            info!(
                market = %state.market,
                interval = %state.interval,
                cursor = %state.cursor,
                "starting klines pagination"
            );

            let request = KlineRequest {
                market: state.market.clone(),
                interval: state.interval,
                start: Some(state.cursor),
                end: state.end,
                limit: state.limit,
            };

            match state.client.fetch_klines(request).await {
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
                Ok(batch) if batch.is_empty() => {
                    debug!("klines pagination complete");
                    None
                }
                Ok(batch) => {
                    // Advance cursor past the close_time of the last candle
                    if let Some(last) = batch.last() {
                        state.cursor = last.close_time + TimeDelta::milliseconds(1);

                        // If cursor has passed the requested end, mark done
                        if let Some(end) = state.end
                            && state.cursor >= end
                        {
                            state.done = true;
                            debug!("klines pagination complete");
                        }
                    }

                    debug!(
                        batch_size = batch.len(),
                        cursor = %state.cursor,
                        "advancing klines pagination"
                    );

                    Some((Ok(batch), state))
                }
            }
        })
    }
}

/// Internal pagination state used by [`TradeFetcher::stream_trades`] to
/// track the cursor position when fetching consecutive batches.
///
/// Uses ID-based pagination via Binance's `fromId` parameter for precise
/// cursor advancement after the first request. The first request uses
/// `startTime`/`endTime` to establish the time range; subsequent requests
/// use `fromId` (set to `last_agg_trade_id + 1`) which avoids skipping
/// trades that share the same millisecond timestamp at batch boundaries.
struct TradePaginationState<Server> {
    client: BinanceRestClient<Server>,
    market: String,
    /// Time-based cursor used for the first request and for sub-window
    /// advancement when `max_trades_time_window` is set.
    cursor: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    /// The aggregate trade ID to use as the `fromId` parameter for the
    /// next request. `None` for the initial request (uses `startTime`).
    next_from_id: Option<u64>,
    done: bool,
}

impl<Server> TradeFetcher for BinanceRestClient<Server>
where
    Server: RestExchangeServer + Sync + 'static,
{
    /// Fetch a single batch of aggregate trades from the Binance REST API.
    ///
    /// Builds a [`GetAggTrades`](trades::GetAggTrades) request from the provided
    /// [`TradeRequest`], waits for the rate limiter, executes the request with
    /// exponential-backoff retry, and converts raw DTOs into [`RestTrade`]s.
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_trades",
            exchange = "binance",
            market = %request.market,
        );
        Box::pin(async move {
            debug!("building trades request");

            let get_trades_request = trades::GetAggTrades {
                path: Server::trades_path(),
                params: trades::GetAggTradesParams {
                    symbol: request.market,
                    start_time: request.start.map(|dt| dt.timestamp_millis()),
                    end_time: request.end.map(|dt| dt.timestamp_millis()),
                    from_id: None,
                    limit: request.limit,
                },
            };

            this.wait_for_rate_limit().await;

            let raw_trades: Vec<trades::BinanceAggTrade> =
                match retry_with_backoff(&RetryPolicy::default(), is_retriable_data_error, || {
                    let req = get_trades_request.clone();
                    let client = this.client.clone();
                    async move {
                        client
                            .execute(req)
                            .await
                            .map(|(response, _metric)| response)
                    }
                })
                .await
                {
                    Ok(trades) => trades,
                    Err(error) => {
                        warn!(?error, "trades fetch failed");
                        return Err(error);
                    }
                };

            let rest_trades = raw_trades
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::Socket)?;

            debug!(count = rest_trades.len(), "fetched trades batch");

            Ok(rest_trades)
        }
        .instrument(span))
    }

    /// Stream paginated batches of trades using ID-based cursor pagination.
    ///
    /// Uses [`futures::stream::unfold`] to repeatedly fetch trade batches.
    /// The first request uses `startTime`/`endTime` to establish the time
    /// range. Subsequent requests use `fromId` (set to last aggregate trade
    /// ID + 1) for precise cursor advancement that never skips trades
    /// sharing the same millisecond timestamp at batch boundaries.
    ///
    /// The `endTime` filter is kept on all requests to know when to stop.
    /// The stream terminates when an empty batch is returned, when the last
    /// trade's timestamp passes the requested end time, or on the first
    /// error (which is yielded before stopping).
    fn stream_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        Box::pin({let state = TradePaginationState {
            client: self.clone(),
            market: request.market,
            cursor: request.start.unwrap_or(DateTime::UNIX_EPOCH),
            end: request.end,
            limit: request.limit,
            next_from_id: None,
            done: false,
        };

        stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }

            info!(
                market = %state.market,
                cursor = %state.cursor,
                from_id = ?state.next_from_id,
                "starting trades pagination"
            );

            // Build request params depending on whether we have an ID cursor.
            // First request: use startTime + endTime (clamped by max window).
            // Subsequent requests: use fromId + endTime (no startTime needed).
            let (start_time, from_id, request_end) = if let Some(from_id) = state.next_from_id {
                // ID-based pagination: fromId is set, no startTime needed.
                // Don't send endTime with fromId — Binance Spot rejects this
                // combination (-1128). The client-side timestamp check below
                // handles stopping at the right boundary.
                (None, Some(from_id), None)
            } else {
                // First request: use time-based range with sub-window clamping.
                let request_end = match (state.end, Server::max_trades_time_window()) {
                    (Some(end), Some(max_window)) => Some(end.min(state.cursor + max_window)),
                    (Some(end), None) => Some(end),
                    (None, Some(max_window)) => Some(state.cursor + max_window),
                    (None, None) => None,
                };
                (Some(state.cursor.timestamp_millis()), None, request_end)
            };

            let get_trades_request = trades::GetAggTrades {
                path: Server::trades_path(),
                params: trades::GetAggTradesParams {
                    symbol: state.market.clone(),
                    start_time,
                    end_time: request_end.map(|dt| dt.timestamp_millis()),
                    from_id,
                    limit: state.limit,
                },
            };

            state.client.wait_for_rate_limit().await;

            let raw_result: Result<Vec<trades::BinanceAggTrade>, DataError> =
                retry_with_backoff(&RetryPolicy::default(), is_retriable_data_error, || {
                    let req = get_trades_request.clone();
                    let client = state.client.client.clone();
                    async move {
                        client
                            .execute(req)
                            .await
                            .map(|(response, _metric)| response)
                    }
                })
                .await;

            match raw_result {
                Err(err) => {
                    warn!(?err, "trades fetch failed");
                    state.done = true;
                    Some((Err(err), state))
                }
                Ok(raw_trades) if raw_trades.is_empty() => {
                    // If we're using sub-windows (first request path with
                    // max_trades_time_window) and haven't reached the overall end,
                    // an empty sub-window doesn't mean we're done — advance cursor.
                    if state.next_from_id.is_none() {
                        if let Some(max_window) = Server::max_trades_time_window() {
                            let window_end = state.cursor + max_window;
                            if state.end.is_none() || state.end.is_some_and(|end| window_end < end)
                            {
                                state.cursor = window_end + TimeDelta::milliseconds(1);
                                debug!(cursor = %state.cursor, "empty sub-window, advancing cursor");
                                return Some((Ok(Vec::new()), state));
                            }
                        }
                    }
                    debug!("trades pagination complete");
                    None
                }
                Ok(raw_trades) => {
                    // Extract the last aggregate trade ID for ID-based cursor.
                    if let Some(last_raw) = raw_trades.last() {
                        let last_id = last_raw.agg_trade_id;
                        let last_ts = last_raw.timestamp;
                        state.next_from_id = Some(last_id + 1);

                        // Update the time cursor for logging/debugging purposes
                        if let Some(ts) = DateTime::from_timestamp_millis(last_ts as i64) {
                            state.cursor = ts;
                        }

                        // If the last trade's timestamp has passed the requested end,
                        // mark done after yielding this batch.
                        if let Some(end) = state.end {
                            let last_ts_ms = last_ts as i64;
                            if last_ts_ms >= end.timestamp_millis() {
                                state.done = true;
                                debug!("trades pagination complete");
                            }
                        }
                    }

                    // Convert raw trades to RestTrade
                    let batch: Result<Vec<RestTrade>, DataError> = raw_trades
                        .into_iter()
                        .map(RestTrade::try_from)
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(DataError::Socket);

                    match batch {
                        Ok(trades) => {
                            debug!(
                                batch_size = trades.len(),
                                from_id = ?state.next_from_id,
                                cursor = %state.cursor,
                                "advancing trades pagination via ID cursor"
                            );
                            Some((Ok(trades), state))
                        }
                        Err(err) => {
                            state.done = true;
                            Some((Err(err), state))
                        }
                    }
                }
            }
        })})
    }
}
