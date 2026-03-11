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
use futures::stream::{self, Stream};
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, num::NonZeroU32, sync::Arc};
use tracing::{Instrument, debug, info, warn};

/// OKX kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// OKX historical trade REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

/// OKX REST API error payload.
///
/// Returned by the OKX API when a request fails, e.g.:
/// ```json
/// { "code": "51001", "msg": "Instrument ID does not exist", "data": [] }
/// ```
///
/// Note that `code` is a `String` (not an integer) in the OKX API.
#[derive(Debug, Deserialize)]
pub struct OkxApiError {
    pub code: String,
    pub msg: String,
}

/// HTTP response parser for OKX REST API responses.
#[derive(Debug)]
pub struct OkxHttpParser;

impl HttpParser for OkxHttpParser {
    type ApiError = OkxApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::Socket(format!(
            "OKX API error (code {}): {}",
            error.code, error.msg
        ))
    }
}

/// Type alias for the direct (non-keyed) rate limiter used by the OKX REST client.
///
/// Uses an in-memory state with the default clock and no middleware.
type OkxRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// OKX REST API base URL.
const OKX_REST_BASE_URL: &str = "https://www.okx.com";

/// REST client for the OKX exchange.
///
/// OKX is a single-variant exchange (no separate spot/futures servers for
/// REST klines), so this client is not generic over an `ExchangeServer` type.
///
/// Includes a rate limiter configured for OKX limits:
/// - `/api/v5/market/candles`: 40 req/2sec = 20 req/sec = 1200 req/min
/// - `/api/v5/market/history-candles`: 20 req/2sec = 10 req/sec = 600 req/min
///
/// We use the more conservative limit (600 req/min) to be safe.
#[derive(Clone)]
pub struct OkxRestClient {
    pub client: Arc<RestClient<'static, PublicNoHeaders, OkxHttpParser>>,
    pub rate_limiter: Arc<OkxRateLimiter>,
}

impl fmt::Debug for OkxRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkxRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"OkxRateLimiter { .. }")
            .finish()
    }
}

impl Default for OkxRestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl OkxRestClient {
    /// Construct a new [`OkxRestClient`] using the default OKX REST base URL.
    ///
    /// Initialises a rate limiter with a quota of 600 requests per minute,
    /// modelling the OKX 20 req/2sec limit for history-candles.
    pub fn new() -> Self {
        Self::with_base_url(OKX_REST_BASE_URL.to_owned())
    }

    /// Construct an [`OkxRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, OkxHttpParser);
        let quota = Quota::per_minute(NonZeroU32::new(600).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within OKX
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }

    /// Execute an OKX trades request with rate limiting, retry, and error checking.
    ///
    /// Returns the raw [`OkxRestTrade`](trades::OkxRestTrade) DTOs in the order
    /// returned by the API (newest-first). Callers are responsible for reversal,
    /// conversion, and filtering.
    ///
    /// This is the shared implementation used by both [`TradeFetcher::fetch_trades`]
    /// and [`TradeFetcher::stream_trades`] to avoid duplicating HTTP execution logic.
    async fn fetch_trades_raw(
        &self,
        request: trades::GetOkxTrades,
    ) -> Result<Vec<trades::OkxRestTrade>, DataError> {
        self.wait_for_rate_limit().await;

        let response: trades::OkxTradesResponse =
            match retry_with_backoff(&RetryPolicy::default(), is_retriable_data_error, || {
                let req = request.clone();
                let client = self.client.clone();
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

        // Check for OKX-level error (non-zero code)
        if response.code != "0" {
            let msg = format!("OKX API error (code {}): {}", response.code, response.msg);
            warn!(%msg, "trades fetch returned error");
            return Err(DataError::Socket(msg));
        }

        Ok(response.data)
    }
}

/// Internal pagination state used by [`KlineFetcher::stream_klines`] to
/// track the cursor position when fetching consecutive batches.
struct PaginationState {
    client: OkxRestClient,
    market: String,
    interval: Interval,
    /// Current forward cursor: we request data after this timestamp.
    cursor: DateTime<Utc>,
    /// End boundary: we request data before this timestamp.
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    done: bool,
}

impl KlineFetcher for OkxRestClient {
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

    /// Fetch a single batch of klines from the OKX REST API.
    ///
    /// Builds a [`GetOkxKlines`](klines::GetOkxKlines) request from the
    /// provided [`KlineRequest`], waits for the rate limiter, executes the
    /// request with exponential-backoff retry, reverses the result (OKX returns
    /// newest-first), and converts raw DTOs into [`Candle`]s.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "okx",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building klines request");

            let get_klines_request = klines::GetOkxKlines {
                path: klines::OKX_HISTORY_KLINES_PATH,
                params: klines::GetOkxKlinesParams {
                    inst_id: request.market,
                    bar: klines::okx_interval(request.interval).to_string(),
                    before: request.start.map(|dt| dt.timestamp_millis()),
                    after: request.end.map(|dt| dt.timestamp_millis()),
                    limit: request.limit,
                },
            };

            let interval = request.interval;

            this.wait_for_rate_limit().await;

            let response: klines::OkxKlinesResponse =
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

            // Check for OKX-level error (non-zero code)
            if response.code != "0" {
                let msg = format!("OKX API error (code {}): {}", response.code, response.msg);
                warn!(msg, "OKX returned error response");
                return Err(DataError::Socket(msg));
            }

            // OKX returns data newest-first; reverse to oldest-first
            let mut raw_klines = response.data;
            raw_klines.reverse();

            let candles = raw_klines
                .into_iter()
                .map(|raw| raw.try_into_candle(interval))
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::Socket)?;

            debug!(count = candles.len(), "fetched klines batch");

            Ok(candles)
        }
        .instrument(span)
    }

    /// Stream paginated batches of klines using time-cursor based pagination.
    ///
    /// Uses [`futures::stream::unfold`] to repeatedly call
    /// [`fetch_klines`](Self::fetch_klines), advancing the `before` cursor
    /// past the last candle's `close_time` in each batch.
    ///
    /// OKX pagination semantics:
    /// - `before`: returns records **newer** than this timestamp
    /// - `after`: returns records **older** than this timestamp
    ///
    /// So for forward (oldest-to-newest) pagination:
    /// - Set `before` = cursor (advancing start boundary)
    /// - Set `after` = end boundary
    ///
    /// Data comes newest-first from OKX, but `fetch_klines` reverses it.
    /// After reversing, the last element is the newest candle. Advance the
    /// cursor past its `close_time`.
    ///
    /// The stream terminates when an empty batch is returned, when the cursor
    /// passes the requested end time, or on the first error (which is yielded
    /// before stopping).
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
                    // Advance cursor past the close_time of the last (newest) candle
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

/// Filter trades to the `[start, end]` time range (inclusive on both bounds).
fn filter_trades_by_time(
    trades: Vec<RestTrade>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Vec<RestTrade> {
    trades
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
        .collect()
}

/// Maximum number of pages to fetch before terminating pagination.
///
/// This prevents runaway pagination on high-volume symbols when no `start`
/// boundary is specified. At 100 trades per page, 10,000 pages covers
/// 1,000,000 trades which is more than sufficient for any practical use case.
const MAX_TRADE_PAGES: u32 = 10_000;

/// Internal pagination state used by [`TradeFetcher::stream_trades`] to
/// track the cursor position when fetching consecutive batches of trades.
///
/// OKX uses ID-based backward pagination: the `after` parameter returns
/// trades with `tradeId` less than the given value (older trades).
struct TradePaginationState {
    client: OkxRestClient,
    market: String,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    /// Trade ID cursor for backward pagination.
    cursor: Option<String>,
    /// Number of pages fetched so far (safety limit to prevent runaway pagination).
    pages_fetched: u32,
    done: bool,
}

impl TradeFetcher for OkxRestClient {
    /// Fetch a single batch of historical trades from the OKX REST API.
    ///
    /// Builds a [`GetOkxTrades`](trades::GetOkxTrades) request from the provided
    /// [`TradeRequest`], delegates to [`fetch_trades_raw`](Self::fetch_trades_raw)
    /// for rate limiting, retry, and error checking, then reverses the results
    /// (OKX returns newest-first), converts raw DTOs into [`RestTrade`]s, and
    /// filters to the requested `[start, end]` range.
    ///
    /// **Note:** OKX's history-trades endpoint uses trade-ID-based cursors, not timestamps.
    /// This method always fetches the most recent trades and filters client-side.
    /// For historical time ranges far in the past, most or all results may be filtered out.
    /// Use `stream_trades` for paginated historical access, though it also starts from
    /// the most recent trades and walks backward.
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> impl std::future::Future<Output = Result<Vec<RestTrade>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_trades",
            exchange = "okx",
            market = %request.market,
        );
        async move {
            debug!("building trades request");

            // Capture start/end before moving request fields
            let request_start = request.start;
            let request_end = request.end;

            let get_trades_request = trades::GetOkxTrades {
                params: trades::GetOkxTradesParams {
                    inst_id: request.market,
                    after: None,
                    before: None,
                    limit: request.limit,
                },
            };

            // OKX returns data newest-first; reverse to oldest-first
            let mut raw_trades = this.fetch_trades_raw(get_trades_request).await?;
            raw_trades.reverse();

            let rest_trades = raw_trades
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::Socket)?;

            // Filter to [start, end] if specified
            let filtered = filter_trades_by_time(rest_trades, request_start, request_end);

            debug!(count = filtered.len(), "fetched trades batch");

            Ok(filtered)
        }
        .instrument(span)
    }

    /// Stream paginated batches of trades using ID-based backward pagination.
    ///
    /// OKX `history-trades` returns newest-first. The `after` parameter fetches
    /// trades with `tradeId` less than the given value (older trades). Each batch
    /// is reversed to oldest-first before yielding.
    ///
    /// Pagination advances by setting `after` to the smallest trade ID from the
    /// previous batch (the oldest trade after reversal, i.e., `batch.first().id`).
    ///
    /// Each batch is filtered to the `[start, end]` time range. The stream
    /// terminates when an empty batch is returned, when all trades in a batch
    /// are older than the requested start time, when the page limit is exceeded,
    /// or on the first error (which is yielded before stopping).
    ///
    /// **Limitation:** Pagination starts from the most recent trades and walks backward.
    /// For time ranges far in the past on high-volume symbols, this may require many pages
    /// and could hit the `MAX_TRADE_PAGES` safety limit before reaching the target range.
    fn stream_trades(
        &self,
        request: TradeRequest,
    ) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
        let state = TradePaginationState {
            client: self.clone(),
            market: request.market,
            start: request.start,
            end: request.end,
            limit: request.limit,
            cursor: None,
            pages_fetched: 0,
            done: false,
        };

        stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }

            // Safety limit: prevent runaway pagination
            if state.pages_fetched >= MAX_TRADE_PAGES {
                warn!(
                    market = %state.market,
                    pages_fetched = state.pages_fetched,
                    "trades pagination terminated: exceeded max page limit ({})",
                    MAX_TRADE_PAGES,
                );
                state.done = true;
                return Some((
                    Err(DataError::Socket(format!(
                        "OKX trades pagination exceeded max page limit ({}) for {}",
                        MAX_TRADE_PAGES, state.market,
                    ))),
                    state,
                ));
            }

            info!(
                market = %state.market,
                cursor = ?state.cursor,
                page = state.pages_fetched,
                "starting trades pagination"
            );

            let get_trades_request = trades::GetOkxTrades {
                params: trades::GetOkxTradesParams {
                    inst_id: state.market.clone(),
                    after: state.cursor.clone(),
                    before: None,
                    limit: state.limit,
                },
            };

            let data = match state.client.fetch_trades_raw(get_trades_request).await {
                Ok(data) => data,
                Err(error) => {
                    state.done = true;
                    return Some((Err(error), state));
                }
            };

            state.pages_fetched += 1;

            // Empty batch means no more data
            if data.is_empty() {
                debug!("trades pagination complete (empty batch)");
                return None;
            }

            // Advance cursor: before reversing, data is newest-first,
            // so data.last() is the oldest (smallest tradeId).
            // Use that as the `after` cursor for the next request.
            state.cursor = data.last().map(|t| t.trade_id.clone());

            // Reverse to oldest-first
            let mut batch_raw = data;
            batch_raw.reverse();

            // Check if the oldest trade in the batch (now batch_raw.first())
            // is older than our start boundary — if so, we've gone far enough.
            if let Some(start) = state.start
                && let Some(oldest) = batch_raw.first()
            {
                let oldest_ts: i64 = match oldest.ts.parse() {
                    Ok(ts) => ts,
                    Err(e) => {
                        state.done = true;
                        return Some((
                            Err(DataError::Socket(format!(
                                "failed to parse trade timestamp '{}': {}",
                                oldest.ts, e
                            ))),
                            state,
                        ));
                    }
                };
                if let Some(oldest_time) = DateTime::from_timestamp_millis(oldest_ts)
                    && oldest_time < start
                {
                    state.done = true;
                }
            }

            // Convert DTOs to RestTrade
            let rest_trades = match batch_raw
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(trades) => trades,
                Err(e) => {
                    state.done = true;
                    return Some((Err(DataError::Socket(e)), state));
                }
            };

            // Filter to [start, end] range
            let filtered = filter_trades_by_time(rest_trades, state.start, state.end);

            debug!(
                batch_size = filtered.len(),
                cursor = ?state.cursor,
                page = state.pages_fetched,
                "advancing trades pagination"
            );

            // If all trades were filtered out and pagination is done,
            // terminate cleanly instead of yielding an empty batch.
            if filtered.is_empty() && state.done {
                return None;
            }

            Some((Ok(filtered), state))
        })
    }
}
