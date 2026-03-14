use crate::{
    error::DataError,
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
use futures::stream::{self, Stream, StreamExt};
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, future::Future, num::NonZeroU32, pin::Pin, sync::Arc};
use tracing::{Instrument, debug, info, warn};

/// Coinbase kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Coinbase trade REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

/// Coinbase REST API error payload.
///
/// Returned by the Coinbase API when a request fails, e.g.:
/// ```json
/// { "error": "NOT_FOUND", "message": "product not found" }
/// ```
#[derive(Debug, Deserialize)]
pub struct CoinbaseApiError {
    pub error: String,
    pub message: String,
}

/// HTTP response parser for Coinbase REST API responses.
#[derive(Debug)]
pub struct CoinbaseHttpParser;

impl HttpParser for CoinbaseHttpParser {
    type ApiError = CoinbaseApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::Socket(format!(
            "Coinbase API error ({}): {}",
            error.error, error.message
        ))
    }
}

/// Type alias for the direct (non-keyed) rate limiter used by the Coinbase REST client.
///
/// Uses an in-memory state with the default clock and no middleware.
type CoinbaseRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// Base URL for the Coinbase REST API.
const COINBASE_REST_BASE_URL: &str = "https://api.coinbase.com";

/// REST client for the Coinbase exchange.
///
/// Unlike the Binance client this is not generic over a server variant
/// because Coinbase uses a single REST API endpoint for all markets.
///
/// Includes a rate limiter configured for 10 requests per second.
#[derive(Clone)]
pub struct CoinbaseRestClient {
    pub client: Arc<RestClient<'static, PublicNoHeaders, CoinbaseHttpParser>>,
    pub rate_limiter: Arc<CoinbaseRateLimiter>,
}

impl fmt::Debug for CoinbaseRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoinbaseRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"CoinbaseRateLimiter { .. }")
            .finish()
    }
}

impl Default for CoinbaseRestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CoinbaseRestClient {
    /// Construct a new [`CoinbaseRestClient`] using the default Coinbase
    /// REST API base URL.
    ///
    /// Initialises a rate limiter with a quota of 10 requests per second.
    pub fn new() -> Self {
        Self::with_base_url(COINBASE_REST_BASE_URL.to_owned())
    }

    /// Construct a [`CoinbaseRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, CoinbaseHttpParser);
        let quota = Quota::per_second(NonZeroU32::new(10).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within Coinbase
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }
}

/// Internal pagination state used by [`KlineFetcher::stream_klines`] to
/// track the cursor position when fetching consecutive batches.
struct PaginationState {
    client: CoinbaseRestClient,
    market: String,
    interval: Interval,
    cursor: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    done: bool,
}

impl KlineFetcher for CoinbaseRestClient {
    fn supported_intervals() -> &'static [Interval] {
        &[
            Interval::M1,
            Interval::M5,
            Interval::M15,
            Interval::M30,
            Interval::H1,
            Interval::H2,
            Interval::H6,
            Interval::D1,
        ]
    }

    /// Fetch a single batch of klines from the Coinbase REST API.
    ///
    /// Builds a [`GetCoinbaseKlines`](klines::GetCoinbaseKlines) request from the provided
    /// [`KlineRequest`], waits for the rate limiter, executes the request with
    /// exponential-backoff retry, and converts raw DTOs into [`Candle`]s.
    ///
    /// The Coinbase API returns candles newest-first, so the result is
    /// reversed to oldest-first before returning.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "coinbase",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building klines request");

            let granularity = klines::coinbase_interval(request.interval)?;

            let path = format!(
                "/api/v3/brokerage/market/products/{}/candles",
                request.market
            );

            let get_klines_request = klines::GetCoinbaseKlines {
                path,
                params: klines::GetCoinbaseKlinesParams {
                    start: request.start.map(|dt| dt.timestamp()),
                    end: request.end.map(|dt| dt.timestamp()),
                    granularity: granularity.to_string(),
                },
            };

            this.wait_for_rate_limit().await;

            let response: klines::CoinbaseKlinesResponse =
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
                    Ok(resp) => resp,
                    Err(error) => {
                        warn!(?error, "klines fetch failed");
                        return Err(error);
                    }
                };

            let interval = request.interval;
            let mut candles = response
                .candles
                .into_iter()
                .map(|raw| raw.into_candle(interval))
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::Socket)?;

            // Coinbase returns newest-first; reverse to oldest-first
            candles.reverse();

            debug!(count = candles.len(), "fetched klines batch");

            Ok(candles)
        }
        .instrument(span)
    }

    /// Stream paginated batches of klines using time-cursor based pagination.
    ///
    /// Uses [`futures::stream::unfold`] to repeatedly call [`fetch_klines`](Self::fetch_klines),
    /// advancing the `start` cursor past the last candle's `close_time` in each batch.
    /// The stream terminates when an empty batch is returned, when the cursor passes the
    /// requested end time, or on the first error (which is yielded before stopping).
    ///
    /// Coinbase uses seconds-based timestamps, so the cursor is advanced by
    /// one second past the last candle's close time.
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
                    // Coinbase uses seconds, so advance by 1 second
                    if let Some(last) = batch.last() {
                        state.cursor = last.close_time + TimeDelta::seconds(1);

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

impl TradeFetcher for CoinbaseRestClient {
    /// Fetch a single batch of trades from the Coinbase REST API.
    ///
    /// Builds a [`GetCoinbaseTrades`](trades::GetCoinbaseTrades) request from the provided
    /// [`TradeRequest`], waits for the rate limiter, executes the request with
    /// exponential-backoff retry, reverses the results (Coinbase returns newest-first),
    /// and converts raw DTOs into [`RestTrade`]s.
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        let this = self.clone();
        let start = request.start;
        let end = request.end;
        let span = tracing::info_span!(
            "fetch_trades",
            exchange = "coinbase",
            market = %request.market,
        );
        Box::pin(async move {
            debug!("building trades request");

            let path = format!(
                "{}/{}/ticker",
                trades::COINBASE_TICKER_PATH_PREFIX,
                request.market
            );

            let get_trades_request = trades::GetCoinbaseTrades {
                path,
                params: trades::GetCoinbaseTradesParams {
                    start: request.start.map(|dt| dt.timestamp()),
                    end: request.end.map(|dt| dt.timestamp()),
                    limit: request.limit,
                },
            };

            this.wait_for_rate_limit().await;

            let response: trades::CoinbaseTradesResponse =
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
                    Ok(resp) => resp,
                    Err(error) => {
                        warn!(?error, "trades fetch failed");
                        return Err(error);
                    }
                };

            // Coinbase returns newest-first; reverse to oldest-first
            let mut raw_trades = response.trades;
            raw_trades.reverse();

            let trades: Vec<RestTrade> = raw_trades
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::Socket)?;

            // Filter to [start, end] range if specified
            let trades: Vec<RestTrade> = trades
                .into_iter()
                .filter(|t| {
                    if let Some(start) = start
                        && t.time < start
                    {
                        return false;
                    }
                    if let Some(end) = end
                        && t.time > end
                    {
                        return false;
                    }
                    true
                })
                .collect();

            debug!(count = trades.len(), "fetched trades batch");

            Ok(trades)
        }
        .instrument(span))
    }

    /// Stream paginated batches of trades in oldest-first order.
    ///
    /// Coinbase's REST API returns the **newest** trades in a `[start, end]`
    /// window, so this method paginates **backward** from `end` toward
    /// `start`. Each batch moves the `end` cursor earlier using the oldest
    /// trade's timestamp minus one millisecond. Once all batches have been
    /// collected they are reversed so the final stream yields trades in
    /// chronological (oldest-first) order.
    ///
    /// The stream terminates when an empty batch is returned, when the cursor
    /// passes the requested start time, or on the first error (which is
    /// yielded before stopping).
    fn stream_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        let client = self.clone();
        let market = request.market;
        let start = request.start;
        let end = request.end;
        let _limit = request.limit;

        // Collect all batches by paginating backward, then yield them in
        // chronological order via `stream::iter`.
        let future = async move {
            let mut cursor = end.unwrap_or_else(Utc::now);
            let mut all_batches: Vec<Result<Vec<RestTrade>, DataError>> = Vec::new();

            loop {
                // If cursor is at or before the start bound we are done.
                if let Some(s) = start {
                    if cursor <= s {
                        debug!("trades backward pagination complete (cursor <= start)");
                        break;
                    }
                }

                info!(
                    market = %market,
                    cursor = %cursor,
                    "fetching trades batch (backward pagination)"
                );

                let req = TradeRequest {
                    market: market.clone(),
                    start,
                    end: Some(cursor),
                    limit: Some(1000),
                };

                match client.fetch_trades(req).await {
                    Err(err) => {
                        all_batches.push(Err(err));
                        break;
                    }
                    Ok(batch) if batch.is_empty() => {
                        debug!("trades backward pagination complete (empty batch)");
                        break;
                    }
                    Ok(batch) => {
                        // `batch` is already oldest-first (fetch_trades reverses).
                        // Move cursor to just before the oldest trade in this batch.
                        if let Some(first) = batch.first() {
                            let new_cursor = first.time - TimeDelta::milliseconds(1);

                            debug!(
                                batch_size = batch.len(),
                                cursor = %new_cursor,
                                "advancing trades backward pagination"
                            );

                            // If cursor did not move we would loop forever.
                            if new_cursor >= cursor {
                                all_batches.push(Ok(batch));
                                debug!(
                                    "trades backward pagination complete (cursor stalled)"
                                );
                                break;
                            }
                            cursor = new_cursor;
                        }

                        all_batches.push(Ok(batch));
                    }
                }
            }

            // Batches were collected newest-window-first; reverse so we yield
            // oldest-first overall.
            all_batches.reverse();
            stream::iter(all_batches)
        };

        Box::pin(stream::once(future).flatten())
    }
}
