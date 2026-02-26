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

/// Hyperliquid kline/candlestick REST request, raw DTO, and conversion to
/// [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Hyperliquid REST API error payload.
///
/// Hyperliquid returns errors as plain text or simple JSON. This captures the
/// common case where the API returns a string error message.
#[derive(Debug, Deserialize)]
pub struct HyperliquidApiError {
    #[serde(default)]
    pub error: String,
}

/// HTTP response parser for Hyperliquid REST API responses.
#[derive(Debug)]
pub struct HyperliquidHttpParser;

impl HttpParser for HyperliquidHttpParser {
    type ApiError = HyperliquidApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::Socket(format!(
            "Hyperliquid API error (HTTP {}): {}",
            status, error.error
        ))
    }
}

/// Type alias for the direct (non-keyed) rate limiter used by the Hyperliquid REST client.
///
/// Uses an in-memory state with the default clock and no middleware.
type HyperliquidRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// Hyperliquid REST API base URL.
const HYPERLIQUID_REST_BASE_URL: &str = "https://api.hyperliquid.xyz";

/// REST client for the Hyperliquid exchange.
///
/// Hyperliquid is a single-variant exchange (no separate spot/futures servers
/// for REST klines), so this client is not generic over an `ExchangeServer`
/// type.
///
/// Includes a rate limiter configured conservatively at 600 requests per
/// minute (~10 req/sec). Hyperliquid's actual limits are more generous, but
/// we stay conservative to avoid rate limiting.
///
/// **Note:** Unlike most other exchange clients, Hyperliquid uses POST
/// requests to `/info` with a JSON body rather than GET with query
/// parameters. The [`RestClient`] framework supports this via the
/// [`RestRequest::body()`] and [`RestRequest::method()`] trait methods.
#[derive(Clone)]
pub struct HyperliquidRestClient {
    pub client: Arc<RestClient<'static, PublicNoHeaders, HyperliquidHttpParser>>,
    pub rate_limiter: Arc<HyperliquidRateLimiter>,
    pub retry: RetryPolicy,
}

impl fmt::Debug for HyperliquidRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperliquidRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"HyperliquidRateLimiter { .. }")
            .finish()
    }
}

impl HyperliquidRestClient {
    /// Construct a new [`HyperliquidRestClient`] using the default Hyperliquid
    /// REST API base URL.
    ///
    /// Initialises a rate limiter with a quota of 600 requests per minute.
    pub fn new() -> Self {
        Self::with_base_url(HYPERLIQUID_REST_BASE_URL.to_owned())
    }

    /// Construct a [`HyperliquidRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, HyperliquidHttpParser);
        let quota = Quota::per_minute(NonZeroU32::new(600).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            retry: RetryPolicy::default(),
        }
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within Hyperliquid
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }
}

/// Internal pagination state used by [`KlineFetcher::stream_klines`] to
/// track the cursor position when fetching consecutive batches.
struct PaginationState {
    client: HyperliquidRestClient,
    market: String,
    interval: Interval,
    /// Current forward cursor: we request data after this timestamp.
    cursor: DateTime<Utc>,
    /// End boundary: we stop when the cursor passes this timestamp.
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    done: bool,
}

impl KlineFetcher for HyperliquidRestClient {
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

    /// Fetch a single batch of klines from the Hyperliquid REST API.
    ///
    /// Builds a [`GetHyperliquidKlines`](klines::GetHyperliquidKlines) POST
    /// request from the provided [`KlineRequest`], waits for the rate limiter,
    /// executes the request with exponential-backoff retry, and converts raw
    /// DTOs into [`Candle`]s.
    ///
    /// Hyperliquid returns data oldest-first, so no reversal is needed.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "hyperliquid",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building klines request");

            let interval_str = klines::hyperliquid_interval(request.interval)?;

            // Compute default time boundaries if not provided.
            // Hyperliquid requires both startTime and endTime.
            let start_ms = request
                .start
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0);
            let end_ms = request
                .end
                .map(|dt| dt.timestamp_millis())
                .unwrap_or_else(|| Utc::now().timestamp_millis());

            let get_klines_request = klines::GetHyperliquidKlines {
                body: klines::PostHyperliquidKlines {
                    request_type: "candleSnapshot",
                    req: klines::HyperliquidKlineReq {
                        coin: request.market,
                        interval: interval_str.to_string(),
                        start_time: start_ms,
                        end_time: end_ms,
                    },
                },
            };

            this.wait_for_rate_limit().await;

            let raw_klines: Vec<klines::HyperliquidKlineRaw> =
                match retry_with_backoff(&this.retry, is_retriable_data_error, || {
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

            // Hyperliquid returns data oldest-first, no reversal needed.
            let candles = raw_klines
                .into_iter()
                .map(|raw| raw.try_into_candle())
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
    /// [`fetch_klines`](Self::fetch_klines), advancing the `startTime` cursor
    /// past the last candle's `close_time` in each batch.
    ///
    /// Hyperliquid returns data oldest-first. The cursor advances past the
    /// close time of the last (newest) candle in each batch.
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
                    // Advance cursor past the close_time of the last (newest) candle.
                    // Hyperliquid returns oldest-first, so the last element is newest.
                    if let Some(last) = batch.last() {
                        state.cursor = last.close_time + TimeDelta::milliseconds(1);

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
