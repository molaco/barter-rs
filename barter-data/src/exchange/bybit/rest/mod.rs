use crate::{
    error::DataError,
    exchange::RestExchangeServer,
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
use std::{fmt, future::Future, marker::PhantomData, num::NonZeroU32, pin::Pin, sync::Arc};
use tracing::{debug, warn};

/// Bybit kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Bybit recent trades REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

/// Bybit REST API error payload.
///
/// Returned by the Bybit API when a request fails, e.g.:
/// ```json
/// { "retCode": 10001, "retMsg": "Invalid symbol." }
/// ```
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct BybitApiError {
    #[serde(rename = "retCode")]
    pub(crate) ret_code: i64,
    #[serde(rename = "retMsg")]
    pub(crate) ret_msg: String,
}

/// HTTP response parser for Bybit REST API responses.
#[derive(Debug)]
pub struct BybitHttpParser;

impl HttpParser for BybitHttpParser {
    type ApiError = BybitApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::ExchangeApi {
            exchange: "bybit".into(),
            code: error.ret_code.to_string(),
            message: error.ret_msg,
        }
    }
}

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
#[non_exhaustive]
#[derive(Clone)]
pub struct BybitRestClient<Server> {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, BybitHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
    _server: PhantomData<Server>,
}

impl<Server> fmt::Debug for BybitRestClient<Server> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BybitRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
            .finish()
    }
}

impl<Server> Default for BybitRestClient<Server>
where
    Server: RestExchangeServer,
{
    fn default() -> Self {
        Self::new()
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

        let quota = Quota::per_second(NonZeroU32::new(10).unwrap()); // 10 is non-zero
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Construct a new [`BybitRestClient`] with a caller-provided rate limiter.
    ///
    /// This allows sharing a single [`ExchangeRateLimiter`] across multiple
    /// clients so they draw from the same token bucket.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            PublicNoHeaders,
            BybitHttpParser,
        );
        Self {
            client: Arc::new(client),
            rate_limiter,
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
        let quota = Quota::per_second(NonZeroU32::new(10).unwrap()); // 10 is non-zero
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, PublicNoHeaders, BybitHttpParser>> {
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
    /// [`KlineRequest`], executes the request, reverses the results (Bybit returns
    /// newest-first), and converts raw DTOs into [`Candle`]s. This is a single-attempt
    /// call; retry logic is handled by the collector.
    #[tracing::instrument(skip(self), fields(exchange = "bybit", market = %request.market, interval = %request.interval))]
    async fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> Result<Vec<Candle>, DataError> {
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

        let response: klines::BybitKlinesResponse =
            match self.client.execute(get_klines_request).await.map(|(response, _metric)| response) {
                Ok(resp) => resp,
                Err(error) => {
                    warn!(?error, "klines fetch failed");
                    return Err(error);
                }
            };

        // Check for Bybit API-level error (non-zero retCode)
        if response.ret_code != 0 {
            warn!(
                ret_code = response.ret_code,
                ret_msg = %response.ret_msg,
                "klines fetch returned error"
            );
            return Err(DataError::ExchangeApi {
                exchange: "bybit".into(),
                code: response.ret_code.to_string(),
                message: response.ret_msg,
            });
        }

        // Extract raw klines from nested response and reverse
        // (Bybit returns newest-first, we want oldest-first)
        let mut raw_klines = response.result.list;
        raw_klines.reverse();

        let candles = raw_klines
            .into_iter()
            .map(Candle::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DataError::DataParse)?;

        debug!(count = candles.len(), "fetched klines batch");

        Ok(candles)
    }
}

impl<Server> TradeFetcher for BybitRestClient<Server>
where
    Server: RestExchangeServer + BybitCategory + Sync + 'static,
{
    fn wait_for_rate_limit(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async { self.rate_limiter.until_ready().await })
    }

    /// Fetch a single batch of recent trades from the Bybit REST API.
    ///
    /// Builds a [`GetBybitTrades`](trades::GetBybitTrades) request from the provided
    /// [`TradeRequest`], executes the request, checks the `retCode` for API-level
    /// errors, reverses the results (Bybit returns newest-first), and converts raw
    /// DTOs into [`RestTrade`]s. This is a single-attempt call; retry logic is handled
    /// by the collector.
    #[tracing::instrument(skip(self), fields(exchange = "bybit", market = %request.market))]
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        Box::pin(async move {
            debug!("building trades request");

            if request.start.is_some() || request.end.is_some() {
                warn!(
                    "Bybit recent-trade endpoint does not support time filtering; \
                     start/end parameters will be ignored"
                );
            }

            let get_trades_request = trades::GetBybitTrades {
                path: Server::trades_path(),
                params: trades::GetBybitTradesParams {
                    category: Server::category().to_string(),
                    symbol: request.market,
                    limit: request.limit,
                },
            };

            let response: trades::BybitTradesResponse =
                match self.client.execute(get_trades_request).await.map(|(response, _metric)| response) {
                    Ok(resp) => resp,
                    Err(error) => {
                        warn!(?error, "trades fetch failed");
                        return Err(error);
                    }
                };

            // Check for API-level errors
            if response.ret_code != 0 {
                warn!(
                    ret_code = response.ret_code,
                    ret_msg = %response.ret_msg,
                    "trades fetch returned error"
                );
                return Err(DataError::ExchangeApi {
                    exchange: "bybit".into(),
                    code: response.ret_code.to_string(),
                    message: response.ret_msg,
                });
            }

            // Extract raw trades from nested response and reverse
            // (Bybit returns newest-first, we want oldest-first)
            let mut raw_trades = response.result.list;
            raw_trades.reverse();

            let rest_trades = raw_trades
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::DataParse)?;

            debug!(count = rest_trades.len(), "fetched trades batch");

            Ok(rest_trades)
        })
    }
}
