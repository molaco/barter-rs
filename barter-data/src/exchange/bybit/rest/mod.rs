use crate::{
    error::DataError,
    exchange::RestExchangeServer,
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
use std::{fmt, marker::PhantomData, num::NonZeroU32, sync::Arc};
use tracing::{Instrument, debug, info, warn};

/// Bybit kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Bybit REST API error payload.
///
/// Returned by the Bybit API when a request fails, e.g.:
/// ```json
/// { "retCode": 10001, "retMsg": "Invalid symbol." }
/// ```
#[derive(Debug, Deserialize)]
pub struct BybitApiError {
    #[serde(rename = "retCode")]
    pub ret_code: i64,
    #[serde(rename = "retMsg")]
    pub ret_msg: String,
}

/// HTTP response parser for Bybit REST API responses.
#[derive(Debug)]
pub struct BybitHttpParser;

impl HttpParser for BybitHttpParser {
    type ApiError = BybitApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::Socket(format!(
            "Bybit API error (code {}): {}",
            error.ret_code, error.ret_msg
        ))
    }
}

/// Type alias for the direct (non-keyed) rate limiter used by the Bybit REST client.
///
/// Uses an in-memory state with the default clock and no middleware.
type BybitRateLimiter = governor::RateLimiter<
    governor::state::NotKeyed,
    governor::state::InMemoryState,
    governor::clock::DefaultClock,
    governor::middleware::NoOpMiddleware,
>;

/// Trait providing the Bybit-specific `category` parameter for REST requests.
///
/// Bybit requires a `category` query parameter ("spot" or "linear") to
/// distinguish between spot and perpetual futures markets.
pub trait BybitCategory {
    /// Return the Bybit market category string.
    fn category() -> &'static str;
}

/// Generic REST client for Bybit exchange variants.
///
/// The `Server` type parameter determines which Bybit server variant
/// (spot or perpetuals) this client connects to, via the
/// [`RestExchangeServer`] trait.
///
/// Includes a rate limiter configured for Bybit rate limits
/// (10 requests per second).
#[derive(Clone)]
pub struct BybitRestClient<Server> {
    pub client: Arc<RestClient<'static, PublicNoHeaders, BybitHttpParser>>,
    pub rate_limiter: Arc<BybitRateLimiter>,
    _server: PhantomData<Server>,
}

impl<Server> fmt::Debug for BybitRestClient<Server> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BybitRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"BybitRateLimiter { .. }")
            .finish()
    }
}

impl<Server> BybitRestClient<Server>
where
    Server: RestExchangeServer,
{
    /// Construct a new [`BybitRestClient`] using the base URL from
    /// [`Server::rest_base_url()`](RestExchangeServer::rest_base_url).
    ///
    /// Initialises a rate limiter with a quota of 10 requests per second,
    /// matching the Bybit rate limit.
    pub fn new() -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            PublicNoHeaders,
            BybitHttpParser,
        );

        let quota = Quota::per_second(NonZeroU32::new(10).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }
}

impl<Server> BybitRestClient<Server> {
    /// Construct a [`BybitRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time. Does not require the `Server` type to implement
    /// [`RestExchangeServer`] since the URL is provided directly.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, BybitHttpParser);
        let quota = Quota::per_second(NonZeroU32::new(10).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within Bybit
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }
}

/// Internal pagination state used by [`KlineFetcher::stream_klines`] to
/// track the cursor position when fetching consecutive batches.
struct PaginationState<Server> {
    client: BybitRestClient<Server>,
    market: String,
    interval: Interval,
    cursor: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
    done: bool,
}

impl<Server> KlineFetcher for BybitRestClient<Server>
where
    Server: RestExchangeServer + BybitCategory + Sync + 'static,
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
            Interval::W1,
            Interval::Month1,
        ]
    }

    /// Fetch a single batch of klines from the Bybit REST API.
    ///
    /// Builds a [`GetBybitKlines`](klines::GetBybitKlines) request from the provided
    /// [`KlineRequest`], waits for the rate limiter, executes the request with
    /// exponential-backoff retry, reverses the results (Bybit returns newest-first),
    /// and converts raw DTOs into [`Candle`]s.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "bybit",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building klines request");

            let get_klines_request = klines::GetBybitKlines {
                path: Server::klines_path(),
                params: klines::GetBybitKlinesParams {
                    category: Server::category().to_string(),
                    symbol: request.market,
                    interval: klines::bybit_interval(request.interval).to_string(),
                    start: request.start.map(|dt| dt.timestamp_millis()),
                    end: request.end.map(|dt| dt.timestamp_millis()),
                    limit: request.limit,
                },
            };

            this.wait_for_rate_limit().await;

            let response: klines::BybitKlinesResponse =
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

            // Extract raw klines from nested response and reverse
            // (Bybit returns newest-first, we want oldest-first)
            let mut raw_klines = response.result.list;
            raw_klines.reverse();

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
    /// advancing the `start` cursor past the last candle's `open_time` in each batch.
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
            cursor: request.start.unwrap_or_else(|| DateTime::UNIX_EPOCH),
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
                    // Advance cursor past the open_time of the last candle
                    // (Bybit klines use open_time as the primary timestamp)
                    if let Some(last) = batch.last() {
                        state.cursor = last.open_time + TimeDelta::milliseconds(1);

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
