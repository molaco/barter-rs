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
use governor::Quota;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{fmt, future::Future, num::NonZeroU32, pin::Pin, sync::Arc};
use tracing::{debug, warn};

/// Kraken kline/OHLC REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;
/// Kraken trades REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

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
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct KrakenApiError {
    pub(crate) error: Vec<String>,
}

/// HTTP response parser for Kraken REST API responses.
#[derive(Debug)]
pub struct KrakenHttpParser;

impl HttpParser for KrakenHttpParser {
    type ApiError = KrakenApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::ExchangeApi {
            exchange: "kraken".into(),
            code: String::new(),
            message: error.error.join("; "),
        }
    }
}

/// REST client for the Kraken exchange.
///
/// Kraken is a single-variant exchange (no `ExchangeServer` pattern).
///
/// Includes a rate limiter configured for conservative Kraken limits
/// (~1 request per second = 60 req/min).
#[non_exhaustive]
#[derive(Clone)]
pub struct KrakenRestClient {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, KrakenHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
}

impl fmt::Debug for KrakenRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KrakenRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
            .finish()
    }
}

impl Default for KrakenRestClient {
    fn default() -> Self {
        Self::new()
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

        // 1 is non-zero
        let quota = Quota::per_second(NonZeroU32::new(1).unwrap()).allow_burst(NonZeroU32::new(1).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Construct a new [`KrakenRestClient`] using the default Kraken base URL
    /// and a pre-existing shared rate limiter.
    ///
    /// This allows multiple clients or components to share the same
    /// [`ExchangeRateLimiter`] so that aggregate request rates stay within
    /// Kraken's limits.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(
            KRAKEN_REST_BASE_URL.to_owned(),
            PublicNoHeaders,
            KrakenHttpParser,
        );

        Self {
            client: Arc::new(client),
            rate_limiter,
        }
    }

    /// Construct a [`KrakenRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, KrakenHttpParser);
        // 1 is non-zero
        let quota = Quota::per_second(NonZeroU32::new(1).unwrap()).allow_burst(NonZeroU32::new(1).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, PublicNoHeaders, KrakenHttpParser>> {
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

    /// Fetch a single batch of OHLC data and return both the candles and the
    /// `last` pagination cursor from the Kraken response.
    ///
    /// This internal method provides access to the raw cursor for use in
    /// pagination. This is a single-attempt call; retry logic is handled by
    /// the collector.
    async fn fetch_raw(
        &self,
        market: &str,
        interval: Interval,
        since: Option<i64>,
    ) -> Result<(Vec<Candle>, Option<i64>), DataError> {
        let interval_minutes = klines::kraken_interval(interval)?;

        let request = klines::GetKrakenOhlc {
            params: klines::GetKrakenOhlcParams {
                pair: market.replace('/', ""),
                interval: interval_minutes,
                since,
            },
        };

        let response: klines::KrakenOhlcResponse =
            match self.client.execute(request).await.map(|(response, _metric)| response) {
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
            return Err(DataError::ExchangeApi {
                exchange: "kraken".into(),
                code: String::new(),
                message: format!("Kraken API error: {}", msg),
            });
        }

        let (raw_klines, last) = response.parse_klines()?;

        let candles = raw_klines
            .into_iter()
            .map(Candle::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DataError::DataParse)?;

        debug!(count = candles.len(), ?last, "fetched Kraken OHLC batch");

        Ok((candles, last))
    }

    /// Fetch a single batch of trades and return both the trades and
    /// the `last` pagination cursor from the Kraken response.
    ///
    /// This internal method provides access to the raw cursor for use in
    /// pagination. This is a single-attempt call; retry logic is handled by
    /// the collector.
    async fn fetch_trades_raw(
        &self,
        market: &str,
        since: Option<String>,
        count: Option<u32>,
    ) -> Result<(Vec<RestTrade>, Option<String>), DataError> {
        let request = trades::GetKrakenTrades {
            params: trades::GetKrakenTradesParams {
                pair: market.replace('/', ""),
                since,
                count,
            },
        };

        let response: trades::KrakenTradesResponse =
            match self.client.execute(request).await.map(|(response, _metric)| response) {
                Ok(resp) => resp,
                Err(error) => {
                    warn!(?error, "Kraken trades fetch failed");
                    return Err(error);
                }
            };

        // Check for API-level errors in the response
        if !response.error.is_empty() {
            let msg = response.error.join("; ");
            warn!(errors = %msg, "Kraken API returned errors");
            return Err(DataError::ExchangeApi {
                exchange: "kraken".into(),
                code: String::new(),
                message: format!("Kraken API error: {}", msg),
            });
        }

        let (raw_trades, last) = response.parse_trades()?;

        let trades = raw_trades
            .into_iter()
            .map(RestTrade::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DataError::DataParse)?;

        debug!(count = trades.len(), ?last, "fetched Kraken trades batch");

        Ok((trades, last))
    }
}

impl KlineFetcher for KrakenRestClient {
    fn supported_intervals() -> &'static [Interval] {
        &[
            Interval::M1,
            Interval::M5,
            Interval::M15,
            Interval::M30,
            Interval::H1,
            Interval::H4,
            Interval::D1,
            Interval::W1,
        ]
    }

    /// Fetch a single batch of OHLC data from the Kraken REST API.
    ///
    /// Builds a [`GetKrakenOhlc`](klines::GetKrakenOhlc) request from the
    /// provided [`KlineRequest`], waits for the rate limiter, executes the
    /// request with exponential-backoff retry, and converts raw DTOs into
    /// [`Candle`]s.
    #[tracing::instrument(skip(self), fields(exchange = "kraken", market = %request.market, interval = %request.interval))]
    async fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> Result<Vec<Candle>, DataError> {
        debug!("building Kraken OHLC request");

        let since = request.start.map(|dt| dt.timestamp());

        let (candles, _last) = self
            .fetch_raw(&request.market, request.interval, since)
            .await?;

        Ok(candles)
    }
}

impl TradeFetcher for KrakenRestClient {
    fn wait_for_rate_limit(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async { self.rate_limiter.until_ready().await })
    }

    /// Fetch a single batch of trades from the Kraken REST API.
    ///
    /// Builds a [`GetKrakenTrades`](trades::GetKrakenTrades) request from the
    /// provided [`TradeRequest`], waits for the rate limiter, executes the
    /// request with exponential-backoff retry, and converts raw DTOs into
    /// [`RestTrade`]s.
    ///
    /// If `request.start` is provided, it is converted to a nanosecond nonce
    /// string for the Kraken `since` parameter. Results are filtered to the
    /// `[start, end]` range if specified.
    #[tracing::instrument(skip(self), fields(exchange = "kraken", market = %request.market))]
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        Box::pin(async move {
            debug!("building Kraken trades request");

            let since = match request.start {
                Some(dt) => {
                    let nanos = dt.timestamp_nanos_opt().ok_or_else(|| {
                        DataError::DataParse(format!("timestamp out of nanosecond range: {}", dt))
                    })?;
                    Some(nanos.to_string())
                }
                None => None,
            };

            let (trades, _last) = self
                .fetch_trades_raw(&request.market, since, request.limit)
                .await?;

            // Filter to [start, end] range if specified
            let filtered: Vec<RestTrade> = trades
                .into_iter()
                .filter(|t| {
                    if let Some(start) = request.start
                        && t.time < start
                    {
                        return false;
                    }
                    if let Some(end) = request.end
                        && t.time > end
                    {
                        return false;
                    }
                    true
                })
                .collect();

            Ok(filtered)
        })
    }
}
