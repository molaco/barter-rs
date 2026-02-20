use crate::{
    error::DataError,
    rest::{
        KlineFetcher, KlineRequest,
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

            let response: klines::OkxKlinesResponse = match retry_with_backoff(
                &RetryPolicy::default(),
                is_retriable_data_error,
                || {
                    let req = get_klines_request.clone();
                    let client = this.client.clone();
                    async move {
                        client
                            .execute(req)
                            .await
                            .map(|(response, _metric)| response)
                    }
                },
            )
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
                let msg = format!(
                    "OKX API error (code {}): {}",
                    response.code, response.msg
                );
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
                        state.cursor =
                            last.close_time + TimeDelta::milliseconds(1);

                        // If cursor has passed the requested end, mark done
                        if let Some(end) = state.end {
                            if state.cursor >= end {
                                state.done = true;
                                debug!("klines pagination complete");
                            }
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
