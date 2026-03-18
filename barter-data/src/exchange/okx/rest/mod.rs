use crate::{
    error::DataError,
    rest::{
        ExchangeRateLimiter, KlineFetcher, KlineRequest, RestTrade, TradeFetcher, TradeRequest,
    },
    subscription::candle::{Candle, Interval},
};
use barter_integration::protocol::http::{
    HttpParser, public::PublicNoHeaders, rest::client::RestClient,
};
use chrono::DateTime;
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, future::Future, num::NonZeroU32, pin::Pin, sync::Arc};
use tracing::{debug, warn};

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
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct OkxApiError {
    pub(crate) code: String,
    pub(crate) msg: String,
}

/// HTTP response parser for OKX REST API responses.
#[derive(Debug)]
pub struct OkxHttpParser;

impl HttpParser for OkxHttpParser {
    type ApiError = OkxApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::ExchangeApi {
            exchange: "okx".into(),
            code: error.code,
            message: error.msg,
        }
    }
}

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
#[non_exhaustive]
#[derive(Clone)]
pub struct OkxRestClient {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, OkxHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
}

impl fmt::Debug for OkxRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkxRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
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

    /// Construct a new [`OkxRestClient`] with a caller-provided rate limiter.
    ///
    /// This allows sharing an [`ExchangeRateLimiter`] across multiple clients
    /// that hit the same exchange endpoint, ensuring the combined request rate
    /// stays within the exchange's limits.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(OKX_REST_BASE_URL.to_owned(), PublicNoHeaders, OkxHttpParser);

        Self {
            client: Arc::new(client),
            rate_limiter,
        }
    }

    /// Construct an [`OkxRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, OkxHttpParser);
        // 600 and 10 are non-zero
        let quota = Quota::per_minute(NonZeroU32::new(600).unwrap()).allow_burst(NonZeroU32::new(10).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, PublicNoHeaders, OkxHttpParser>> {
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

    /// Execute an OKX trades request with error checking.
    ///
    /// Returns the raw [`OkxRestTrade`](trades::OkxRestTrade) DTOs in the order
    /// returned by the API (newest-first). Callers are responsible for reversal,
    /// conversion, and filtering.
    ///
    /// This is the shared implementation used by [`TradeFetcher::fetch_trades`]
    /// to avoid duplicating HTTP execution logic. This is a single-attempt call;
    /// retry logic is handled by the collector.
    async fn fetch_trades_raw(
        &self,
        request: trades::GetOkxTrades,
    ) -> Result<Vec<trades::OkxRestTrade>, DataError> {
        let response: trades::OkxTradesResponse =
            match self.client.execute(request).await.map(|(response, _metric)| response) {
                Ok(resp) => resp,
                Err(error) => {
                    warn!(?error, "trades fetch failed");
                    return Err(error);
                }
            };

        // Check for OKX-level error (non-zero code)
        if response.code != "0" {
            warn!(code = %response.code, msg = %response.msg, "trades fetch returned error");
            return Err(DataError::ExchangeApi {
                exchange: "okx".into(),
                code: response.code,
                message: response.msg,
            });
        }

        Ok(response.data)
    }
}

/// Filter trades to the `[start, end]` time range (inclusive on both bounds).
fn filter_trades_by_time(
    trades: Vec<RestTrade>,
    start: Option<DateTime<chrono::Utc>>,
    end: Option<DateTime<chrono::Utc>>,
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
    /// provided [`KlineRequest`], executes the request, reverses the result
    /// (OKX returns newest-first), and converts raw DTOs into [`Candle`]s.
    /// This is a single-attempt call; retry logic is handled by the collector.
    #[tracing::instrument(skip(self), fields(exchange = "okx", market = %request.market, interval = %request.interval))]
    async fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> Result<Vec<Candle>, DataError> {
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

        let response: klines::OkxKlinesResponse =
            match self.client.execute(get_klines_request).await.map(|(response, _metric)| response) {
                Ok(resp) => resp,
                Err(error) => {
                    warn!(?error, "klines fetch failed");
                    return Err(error);
                }
            };

        // Check for OKX-level error (non-zero code)
        if response.code != "0" {
            warn!(code = %response.code, msg = %response.msg, "OKX returned error response");
            return Err(DataError::ExchangeApi {
                exchange: "okx".into(),
                code: response.code,
                message: response.msg,
            });
        }

        // OKX returns data newest-first; reverse to oldest-first
        let mut raw_klines = response.data;
        raw_klines.reverse();

        let candles = raw_klines
            .into_iter()
            .map(|raw| raw.try_into_candle(interval))
            .collect::<Result<Vec<_>, _>>()
            .map_err(DataError::DataParse)?;

        debug!(count = candles.len(), "fetched klines batch");

        Ok(candles)
    }
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
    #[tracing::instrument(skip(self), fields(exchange = "okx", market = %request.market))]
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        Box::pin(async move {
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
            let mut raw_trades = self.fetch_trades_raw(get_trades_request).await?;
            raw_trades.reverse();

            let rest_trades = raw_trades
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::DataParse)?;

            // Filter to [start, end] if specified
            let filtered = filter_trades_by_time(rest_trades, request_start, request_end);

            debug!(count = filtered.len(), "fetched trades batch");

            Ok(filtered)
        })
    }
}
