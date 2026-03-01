use crate::{
    error::DataError,
    exchange::RestExchangeServer,
    rest::{
        KlineFetcher, KlineRequest, RestTrade, TradeFetcher, TradeRequest,
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
use std::{fmt, marker::PhantomData, num::NonZeroU32, sync::Arc};
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

/// Type alias for the direct (non-keyed) rate limiter used by the Binance REST client.
///
/// Uses an in-memory state with the default clock and no middleware.
type BinanceRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

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
    pub rate_limiter: Arc<BinanceRateLimiter>,
    _server: PhantomData<Server>,
}

impl<Server> fmt::Debug for BinanceRestClient<Server> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"BinanceRateLimiter { .. }")
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

        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
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
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap());
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
struct TradePaginationState<Server> {
    client: BinanceRestClient<Server>,
    market: String,
    cursor: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
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
    ) -> impl std::future::Future<Output = Result<Vec<RestTrade>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_trades",
            exchange = "binance",
            market = %request.market,
        );
        async move {
            debug!("building trades request");

            let get_trades_request = trades::GetAggTrades {
                path: Server::trades_path(),
                params: trades::GetAggTradesParams {
                    symbol: request.market,
                    start_time: request.start.map(|dt| dt.timestamp_millis()),
                    end_time: request.end.map(|dt| dt.timestamp_millis()),
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
        .instrument(span)
    }

    /// Stream paginated batches of trades using time-cursor based pagination.
    ///
    /// Uses [`futures::stream::unfold`] to repeatedly call [`fetch_trades`](Self::fetch_trades),
    /// advancing the `startTime` cursor past the last trade's timestamp in each batch.
    /// The stream terminates when an empty batch is returned, when the cursor passes the
    /// requested end time, or on the first error (which is yielded before stopping).
    fn stream_trades(
        &self,
        request: TradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
        let state = TradePaginationState {
            client: self.clone(),
            market: request.market,
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
                cursor = %state.cursor,
                "starting trades pagination"
            );

            // Clamp per-request end time to respect exchange time window limits
            // (e.g., Binance Futures limits endTime - startTime to < 1 hour)
            let request_end = match (state.end, Server::max_trades_time_window()) {
                (Some(end), Some(max_window)) => Some(end.min(state.cursor + max_window)),
                (Some(end), None) => Some(end),
                (None, Some(max_window)) => Some(state.cursor + max_window),
                (None, None) => None,
            };

            let request = TradeRequest {
                market: state.market.clone(),
                start: Some(state.cursor),
                end: request_end,
                limit: state.limit,
            };

            match state.client.fetch_trades(request).await {
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
                Ok(batch) if batch.is_empty() => {
                    debug!("trades pagination complete");
                    None
                }
                Ok(batch) => {
                    // Advance cursor past the timestamp of the last trade
                    if let Some(last) = batch.last() {
                        state.cursor = last.time + TimeDelta::milliseconds(1);

                        // If cursor has passed the requested end, mark done
                        if let Some(end) = state.end
                            && state.cursor >= end
                        {
                            state.done = true;
                            debug!("trades pagination complete");
                        }
                    }

                    debug!(
                        batch_size = batch.len(),
                        cursor = %state.cursor,
                        "advancing trades pagination"
                    );

                    Some((Ok(batch), state))
                }
            }
        })
    }
}
