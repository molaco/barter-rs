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

/// Binance kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Binance aggregate trade REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

/// Binance REST API error payload.
///
/// Returned by the Binance API when a request fails, e.g.:
/// ```json
/// { "code": -1121, "msg": "Invalid symbol." }
/// ```
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct BinanceApiError {
    pub(crate) code: i64,
    pub(crate) msg: String,
}

/// HTTP response parser for Binance REST API responses.
#[derive(Debug)]
pub struct BinanceHttpParser;

impl HttpParser for BinanceHttpParser {
    type ApiError = BinanceApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::ExchangeApi {
            exchange: "binance".into(),
            code: error.code.to_string(),
            message: error.msg,
        }
    }
}

/// Generic REST client for Binance exchange variants.
///
/// The `Server` type parameter determines which Binance server variant
/// (spot or futures) this client connects to, via the
/// [`RestExchangeServer`] trait.
///
/// Includes a rate limiter configured for Binance weight-based limits
/// (6000 weight per minute, ~5 weight per klines request = 1200 req/min).
#[non_exhaustive]
#[derive(Clone)]
pub struct BinanceRestClient<Server> {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, BinanceHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
    _server: PhantomData<Server>,
}

impl<Server> fmt::Debug for BinanceRestClient<Server> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
            .finish()
    }
}

impl<Server> Default for BinanceRestClient<Server>
where
    Server: RestExchangeServer,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<Server> BinanceRestClient<Server>
where
    Server: RestExchangeServer,
{
    /// Construct a new [`BinanceRestClient`] using the base URL from
    /// [`Server::rest_base_url()`](RestExchangeServer::rest_base_url).
    ///
    /// Initialises a rate limiter with a quota of 1200 requests per minute,
    /// modelling the Binance 6000 weight/min limit at ~5 weight per request.
    pub fn new() -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            PublicNoHeaders,
            BinanceHttpParser,
        );

        // 1200 and 20 are non-zero
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap()).allow_burst(NonZeroU32::new(20).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Construct a new [`BinanceRestClient`] with a shared rate limiter.
    ///
    /// Uses the base URL from [`Server::rest_base_url()`](RestExchangeServer::rest_base_url)
    /// but accepts an externally-owned [`ExchangeRateLimiter`] instead of creating one.
    /// This is useful when multiple clients should share the same rate-limit budget.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            PublicNoHeaders,
            BinanceHttpParser,
        );

        Self {
            client: Arc::new(client),
            rate_limiter,
            _server: PhantomData,
        }
    }
}

impl<Server> BinanceRestClient<Server> {
    /// Construct a [`BinanceRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time. Does not require the `Server` type to implement
    /// [`RestExchangeServer`] since the URL is provided directly.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, BinanceHttpParser);
        // 1200 and 20 are non-zero
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap()).allow_burst(NonZeroU32::new(20).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, PublicNoHeaders, BinanceHttpParser>> {
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

impl<Server> KlineFetcher for BinanceRestClient<Server>
where
    Server: RestExchangeServer + Sync + 'static,
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
            Interval::D3,
            Interval::W1,
            Interval::Month1,
        ]
    }

    /// Fetch a single batch of klines from the Binance REST API.
    ///
    /// Builds a [`GetKlines`](klines::GetKlines) request from the provided
    /// [`KlineRequest`], executes the request, and converts raw DTOs into
    /// [`Candle`]s. This is a single-attempt call; retry logic is handled by
    /// the collector.
    #[tracing::instrument(skip(self), fields(exchange = "binance", market = %request.market, interval = %request.interval))]
    async fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> Result<Vec<Candle>, DataError> {
        debug!("building klines request");

        let get_klines_request = klines::GetKlines {
            path: Server::klines_path(),
            params: klines::GetKlinesParams {
                symbol: request.market,
                interval: klines::binance_interval(request.interval).to_string(),
                start_time: request.start.map(|dt| dt.timestamp_millis()),
                end_time: request.end.map(|dt| dt.timestamp_millis()),
                limit: request.limit,
            },
        };

        let raw_klines: Vec<klines::BinanceKlineRaw> =
            match self.client.execute(get_klines_request).await.map(|(response, _metric)| response) {
                Ok(klines) => klines,
                Err(error) => {
                    warn!(?error, "klines fetch failed");
                    return Err(error);
                }
            };

        let candles = raw_klines
            .into_iter()
            .map(Candle::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(DataError::DataParse)?;

        debug!(count = candles.len(), "fetched klines batch");

        Ok(candles)
    }
}

impl<Server> TradeFetcher for BinanceRestClient<Server>
where
    Server: RestExchangeServer + Sync + 'static,
{
    fn wait_for_rate_limit(&self) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async { self.rate_limiter.until_ready().await })
    }

    /// Fetch a single batch of aggregate trades from the Binance REST API.
    ///
    /// Builds a [`GetAggTrades`](trades::GetAggTrades) request from the provided
    /// [`TradeRequest`], executes the request, and converts raw DTOs into
    /// [`RestTrade`]s. This is a single-attempt call; retry logic is handled by
    /// the collector.
    #[tracing::instrument(skip(self), fields(exchange = "binance", market = %request.market))]
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        Box::pin(async move {
            debug!("building trades request");

            let get_trades_request = trades::GetAggTrades {
                path: Server::trades_path(),
                params: trades::GetAggTradesParams {
                    symbol: request.market,
                    start_time: request.start.map(|dt| dt.timestamp_millis()),
                    end_time: request.end.map(|dt| dt.timestamp_millis()),
                    from_id: None,
                    limit: request.limit,
                },
            };

            let raw_trades: Vec<trades::BinanceAggTrade> =
                match self.client.execute(get_trades_request).await.map(|(response, _metric)| response) {
                    Ok(trades) => trades,
                    Err(error) => {
                        warn!(?error, "trades fetch failed");
                        return Err(error);
                    }
                };

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
