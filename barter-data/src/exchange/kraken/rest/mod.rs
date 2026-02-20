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
use futures::stream::{self, Stream};
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, num::NonZeroU32, sync::Arc};
use tracing::{Instrument, debug, info, warn};

/// Kraken kline/OHLC REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Kraken REST API base URL.
const KRAKEN_REST_BASE_URL: &str = "https://api.kraken.com";

/// Kraken REST API error payload.
///
/// Returned by the Kraken API when a request fails, e.g.:
/// ```json
/// { "error": ["EGeneral:Invalid arguments"], "result": {} }
/// ```
///
/// Kraken returns errors as an array of strings in the top-level `error` field.
#[derive(Debug, Deserialize)]
pub struct KrakenApiError {
    pub error: Vec<String>,
}

/// HTTP response parser for Kraken REST API responses.
#[derive(Debug)]
pub struct KrakenHttpParser;

impl HttpParser for KrakenHttpParser {
    type ApiError = KrakenApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::Socket(format!("Kraken API error: {}", error.error.join("; ")))
    }
}

/// Type alias for the direct (non-keyed) rate limiter used by the Kraken REST client.
///
/// Uses an in-memory state with the default clock and no middleware.
type KrakenRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// REST client for the Kraken exchange.
///
/// Kraken is a single-variant exchange (no `ExchangeServer` pattern).
///
/// Includes a rate limiter configured for conservative Kraken limits
/// (~1 request per second = 60 req/min).
#[derive(Clone)]
pub struct KrakenRestClient {
    pub client: Arc<RestClient<'static, PublicNoHeaders, KrakenHttpParser>>,
    pub rate_limiter: Arc<KrakenRateLimiter>,
}

impl fmt::Debug for KrakenRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KrakenRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"KrakenRateLimiter { .. }")
            .finish()
    }
}

impl KrakenRestClient {
    /// Construct a new [`KrakenRestClient`] using the default Kraken base URL.
    ///
    /// Initialises a rate limiter with a quota of 60 requests per minute
    /// (~1 request per second), which is conservative for Kraken's public API.
    pub fn new() -> Self {
        let client = RestClient::new(
            KRAKEN_REST_BASE_URL.to_owned(),
            PublicNoHeaders,
            KrakenHttpParser,
        );

        let quota = Quota::per_minute(NonZeroU32::new(60).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Construct a [`KrakenRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, KrakenHttpParser);
        let quota = Quota::per_minute(NonZeroU32::new(60).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within Kraken
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }

    /// Fetch a single batch of OHLC data and return both the candles and the
    /// `last` pagination cursor from the Kraken response.
    ///
    /// This internal method provides access to the raw cursor for use in
    /// [`stream_klines`](Self::stream_klines) pagination.
    async fn fetch_raw(
        &self,
        market: &str,
        interval: Interval,
        since: Option<i64>,
    ) -> Result<(Vec<Candle>, Option<i64>), DataError> {
        let interval_minutes = klines::kraken_interval(interval)?;

        let request = klines::GetKrakenOhlc {
            params: klines::GetKrakenOhlcParams {
                pair: market.to_string(),
                interval: interval_minutes,
                since,
            },
        };

        self.wait_for_rate_limit().await;

        let response: klines::KrakenOhlcResponse = match retry_with_backoff(
            &RetryPolicy::default(),
            is_retriable_data_error,
            || {
                let req = request.clone();
                let client = self.client.clone();
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
                warn!(?error, "Kraken OHLC fetch failed");
                return Err(error);
            }
        };

        // Check for API-level errors in the response
        if !response.error.is_empty() {
            let msg = response.error.join("; ");
            warn!(errors = %msg, "Kraken API returned errors");
            return Err(DataError::Socket(format!("Kraken API error: {}", msg)));
        }

        let (raw_klines, last) = response.parse_klines()?;

        let candles = raw_klines
            .into_iter()
            .map(Candle::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DataError::Socket)?;

        debug!(count = candles.len(), ?last, "fetched Kraken OHLC batch");

        Ok((candles, last))
    }
}

/// Internal pagination state used by [`KlineFetcher::stream_klines`] to
/// track the cursor position when fetching consecutive batches.
struct PaginationState {
    client: KrakenRestClient,
    market: String,
    interval: Interval,
    /// Cursor-based pagination: the `last` value from the previous response.
    since: Option<i64>,
    done: bool,
}

impl KlineFetcher for KrakenRestClient {
    /// Fetch a single batch of OHLC data from the Kraken REST API.
    ///
    /// Builds a [`GetKrakenOhlc`](klines::GetKrakenOhlc) request from the
    /// provided [`KlineRequest`], waits for the rate limiter, executes the
    /// request with exponential-backoff retry, and converts raw DTOs into
    /// [`Candle`]s.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "kraken",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building Kraken OHLC request");

            let since = request.start.map(|dt| dt.timestamp());

            let (candles, _last) = this.fetch_raw(&request.market, request.interval, since).await?;

            Ok(candles)
        }
        .instrument(span)
    }

    /// Stream paginated batches of OHLC data using cursor-based pagination.
    ///
    /// Uses [`futures::stream::unfold`] to repeatedly call the internal
    /// [`fetch_raw`](Self::fetch_raw) method, advancing the `since` cursor
    /// to the `last` value from each response. The stream terminates when an
    /// empty batch is returned, when the `last` cursor does not advance, or
    /// on the first error (which is yielded before stopping).
    ///
    /// Kraken returns data oldest-first, so no reversal is needed.
    fn stream_klines(
        &self,
        request: KlineRequest,
    ) -> impl Stream<Item = Result<Vec<Candle>, DataError>> + Send {
        let state = PaginationState {
            client: self.clone(),
            market: request.market,
            interval: request.interval,
            since: request.start.map(|dt| dt.timestamp()),
            done: false,
        };

        stream::unfold(state, |mut state| async move {
            if state.done {
                return None;
            }

            info!(
                market = %state.market,
                interval = %state.interval,
                since = ?state.since,
                "starting Kraken OHLC pagination"
            );

            match state
                .client
                .fetch_raw(&state.market, state.interval, state.since)
                .await
            {
                Err(err) => {
                    state.done = true;
                    Some((Err(err), state))
                }
                Ok((batch, _last)) if batch.is_empty() => {
                    debug!("Kraken OHLC pagination complete (empty batch)");
                    None
                }
                Ok((batch, last)) => {
                    // Update the cursor to the `last` value from the response
                    // for the next pagination request.
                    match last {
                        Some(new_since) if state.since == Some(new_since) => {
                            // Cursor did not advance; pagination is complete
                            debug!("Kraken OHLC pagination complete (cursor unchanged)");
                            state.done = true;
                        }
                        Some(new_since) => {
                            state.since = Some(new_since);
                        }
                        None => {
                            // No `last` cursor in response; cannot paginate further
                            debug!("Kraken OHLC pagination complete (no cursor)");
                            state.done = true;
                        }
                    }

                    debug!(
                        batch_size = batch.len(),
                        since = ?state.since,
                        "advancing Kraken OHLC pagination"
                    );

                    Some((Ok(batch), state))
                }
            }
        })
    }
}
