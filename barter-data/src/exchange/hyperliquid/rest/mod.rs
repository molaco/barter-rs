use crate::{
    error::DataError,
    rest::{
        ExchangeRateLimiter, KlineFetcher, KlineRequest,
    },
    subscription::candle::{Candle, Interval},
};
use barter_integration::protocol::http::{
    HttpParser, public::PublicNoHeaders, rest::client::RestClient,
};
use chrono::Utc;
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, num::NonZeroU32, sync::Arc};
use tracing::{debug, warn};

/// Hyperliquid kline/candlestick REST request, raw DTO, and conversion to
/// [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Hyperliquid REST API error payload.
///
/// Hyperliquid returns errors as plain text or simple JSON. This captures the
/// common case where the API returns a string error message.
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct HyperliquidApiError {
    #[serde(default)]
    pub(crate) error: String,
}

/// HTTP response parser for Hyperliquid REST API responses.
#[derive(Debug)]
pub struct HyperliquidHttpParser;

impl HttpParser for HyperliquidHttpParser {
    type ApiError = HyperliquidApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::ExchangeApi {
            exchange: "hyperliquid".into(),
            code: status.to_string(),
            message: error.error,
        }
    }
}

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
#[non_exhaustive]
#[derive(Clone)]
pub struct HyperliquidRestClient {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, HyperliquidHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
}

impl fmt::Debug for HyperliquidRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HyperliquidRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
            .finish()
    }
}

impl Default for HyperliquidRestClient {
    fn default() -> Self {
        Self::new()
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

    /// Construct a new [`HyperliquidRestClient`] with a caller-provided rate limiter.
    ///
    /// This allows sharing a single [`ExchangeRateLimiter`] across multiple
    /// clients so they draw from the same token bucket.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(
            HYPERLIQUID_REST_BASE_URL.to_owned(),
            PublicNoHeaders,
            HyperliquidHttpParser,
        );
        Self {
            client: Arc::new(client),
            rate_limiter,
        }
    }

    /// Construct a [`HyperliquidRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, HyperliquidHttpParser);
        let quota = Quota::per_minute(NonZeroU32::new(600).unwrap()); // 600 is non-zero
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, PublicNoHeaders, HyperliquidHttpParser>> {
        &self.client
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// This method is for **standalone/direct usage** — call it before each
    /// REST API request when driving the client yourself (outside of a
    /// `barter-collector` pipeline). When using `barter-collector`, rate
    /// limiting is managed by the collector's orchestration layer.
    ///
    /// Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }
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
    /// request from the provided [`KlineRequest`], executes the request, and
    /// converts raw DTOs into [`Candle`]s. This is a single-attempt call;
    /// retry logic is handled by the collector.
    ///
    /// Hyperliquid returns data oldest-first, so no reversal is needed.
    #[tracing::instrument(skip(self), fields(exchange = "hyperliquid", market = %request.market, interval = %request.interval))]
    async fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> Result<Vec<Candle>, DataError> {
        debug!("building klines request");

        let interval_str = klines::hyperliquid_interval(request.interval)?;

        // Compute default time boundaries if not provided.
        // Hyperliquid requires both startTime and endTime.
        let start_ms = request.start.map(|dt| dt.timestamp_millis()).unwrap_or(0);
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

        let raw_klines: Vec<klines::HyperliquidKlineRaw> =
            match self.client.execute(get_klines_request).await.map(|(response, _metric)| response) {
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
            .map_err(DataError::DataParse)?;

        debug!(count = candles.len(), "fetched klines batch");

        Ok(candles)
    }
}
