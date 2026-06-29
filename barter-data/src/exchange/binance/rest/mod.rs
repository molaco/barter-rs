use crate::{
    error::DataError,
    exchange::RestExchangeServer,
    rest::{
        ExchangeRateLimiter, KlineFetcher, KlineRequest, RestTrade, TradeFetcher, TradeRequest,
    },
    subscription::candle::{Candle, Interval},
};
use barter_integration::{
    error::SocketError,
    protocol::http::{BuildStrategy, HttpParser, rest::client::RestClient},
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

/// [`BuildStrategy`] for Binance REST requests that optionally injects the
/// `X-MBX-APIKEY` header.
///
/// Most Binance market-data endpoints are public and require no key, but some
/// (notably `/historicalTrades`) are gated behind a valid API key. When
/// `api_key` is `None`, no header is added and every request stays public —
/// preserving the prior [`PublicNoHeaders`](barter_integration::protocol::http::public::PublicNoHeaders)
/// behaviour.
#[derive(Clone, Default)]
pub struct BinanceAuthHeaders {
    api_key: Option<String>,
}

impl fmt::Debug for BinanceAuthHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the secret; only whether one is configured.
        f.debug_struct("BinanceAuthHeaders")
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl BuildStrategy for BinanceAuthHeaders {
    fn build<Request>(
        &self,
        _request: Request,
        builder: reqwest::RequestBuilder,
    ) -> Result<reqwest::Request, SocketError> {
        let builder = match &self.api_key {
            Some(k) => builder.header("X-MBX-APIKEY", k),
            None => builder,
        };
        builder.build().map_err(SocketError::from)
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
    pub(crate) client: Arc<RestClient<'static, BinanceAuthHeaders, BinanceHttpParser>>,
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
            BinanceAuthHeaders::default(),
            BinanceHttpParser,
        );

        // 1200 and 20 are non-zero
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap())
            .allow_burst(NonZeroU32::new(20).unwrap());
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
            BinanceAuthHeaders::default(),
            BinanceHttpParser,
        );

        Self {
            client: Arc::new(client),
            rate_limiter,
            _server: PhantomData,
        }
    }

    /// Construct a new [`BinanceRestClient`] with an optional API key.
    ///
    /// Behaves like [`new`](Self::new) (creating its own rate limiter) but
    /// attaches `api_key` so that key-gated endpoints such as
    /// `/historicalTrades` can be reached. A `None` key keeps every request
    /// public.
    pub fn with_api_key(api_key: Option<String>) -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            BinanceAuthHeaders { api_key },
            BinanceHttpParser,
        );

        // 1200 and 20 are non-zero
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap())
            .allow_burst(NonZeroU32::new(20).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);

        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Construct a new [`BinanceRestClient`] with a shared rate limiter and an
    /// optional API key.
    ///
    /// Combines [`with_rate_limiter`](Self::with_rate_limiter) and
    /// [`with_api_key`](Self::with_api_key): the rate-limit budget is shared
    /// via the supplied [`ExchangeRateLimiter`], while `api_key` (when `Some`)
    /// is sent on every request via the `X-MBX-APIKEY` header.
    pub fn with_rate_limiter_and_key(
        rate_limiter: Arc<ExchangeRateLimiter>,
        api_key: Option<String>,
    ) -> Self {
        let client = RestClient::new(
            Server::rest_base_url().to_owned(),
            BinanceAuthHeaders { api_key },
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
        let client = RestClient::new(base_url, BinanceAuthHeaders::default(), BinanceHttpParser);
        // 1200 and 20 are non-zero
        let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap())
            .allow_burst(NonZeroU32::new(20).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
            _server: PhantomData,
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, BinanceAuthHeaders, BinanceHttpParser>> {
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
    async fn fetch_klines(&self, request: KlineRequest) -> Result<Vec<Candle>, DataError> {
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

        let raw_klines: Vec<klines::BinanceKlineRaw> = match self
            .client
            .execute(get_klines_request)
            .await
            .map(|(response, _metric)| response)
        {
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

    /// Fetch a single batch of trades from the Binance REST API.
    ///
    /// Two modes, selected by [`TradeRequest::initial_cursor`]:
    ///
    /// - **`Some(from_id)`** — fetches *individual* (raw) trades via
    ///   [`GetHistoricalTrades`](trades::GetHistoricalTrades) (`/historicalTrades`,
    ///   paged by `fromId`). These match the live `@trade` stream 1:1 and are the
    ///   correct source for gap-filling. `from_id` is the decimal raw trade id to
    ///   start at (seed it with [`resolve_start_trade_id`](Self::resolve_start_trade_id)).
    /// - **`None`** — fetches *aggregate* trades via
    ///   [`GetAggTrades`](trades::GetAggTrades) using the time window
    ///   (`start`/`end`), preserving the original behaviour.
    ///
    /// Either way this is a single-attempt call returning oldest-first trades;
    /// retry logic is handled by the collector.
    #[tracing::instrument(skip(self), fields(exchange = "binance", market = %request.market))]
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        Box::pin(async move {
            // Raw individual-trades path — opt-in via `initial_cursor` carrying a
            // decimal raw `fromId`. Returns trades matching the live `@trade`
            // stream rather than aggregated trades.
            if let Some(cursor) = request.initial_cursor {
                debug!(%cursor, "building raw historical trades request");

                let from_id = cursor.parse::<u64>().map_err(|e| {
                    DataError::DataParse(format!(
                        "invalid raw trade fromId cursor '{cursor}': {e}"
                    ))
                })?;

                let get_raw_request = trades::GetHistoricalTrades {
                    path: Server::historical_trades_path(),
                    params: trades::GetHistoricalTradesParams {
                        symbol: request.market,
                        from_id: Some(from_id),
                        limit: request.limit,
                    },
                };

                let raw_trades: Vec<trades::BinanceRawTrade> = match self
                    .client
                    .execute(get_raw_request)
                    .await
                    .map(|(response, _metric)| response)
                {
                    Ok(trades) => trades,
                    Err(error) => {
                        warn!(?error, "raw historical trades fetch failed");
                        return Err(error);
                    }
                };

                let rest_trades = raw_trades
                    .into_iter()
                    .map(RestTrade::try_from)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(DataError::DataParse)?;

                debug!(count = rest_trades.len(), "fetched raw trades batch");

                return Ok(rest_trades);
            }

            debug!("building aggregate trades request");

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

            let raw_trades: Vec<trades::BinanceAggTrade> = match self
                .client
                .execute(get_trades_request)
                .await
                .map(|(response, _metric)| response)
            {
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

    /// Resolve the raw trade id to seed forward `fromId` pagination from
    /// `start_ms`.
    ///
    /// Issues a single public `aggTrades` probe (`limit=1`, `startTime=start_ms`)
    /// and returns the aggregate's first (raw) trade id — i.e. the id of the
    /// earliest individual trade at/after `start_ms`. Feed this back into
    /// [`fetch_trades`](Self::fetch_trades) via
    /// [`TradeRequest::initial_cursor`] to page individual trades. Returns
    /// `Ok(None)` when no trade exists at/after `start_ms`.
    #[tracing::instrument(skip(self), fields(exchange = "binance", market = %market))]
    fn resolve_start_trade_id(
        &self,
        market: &str,
        start_ms: i64,
    ) -> Pin<Box<dyn Future<Output = Result<Option<u64>, DataError>> + Send + '_>> {
        let market = market.to_owned();
        Box::pin(async move {
            debug!(start_ms, "resolving start trade id via aggTrades probe");

            let probe = trades::GetAggTrades {
                path: Server::trades_path(),
                params: trades::GetAggTradesParams {
                    symbol: market,
                    start_time: Some(start_ms),
                    end_time: None,
                    from_id: None,
                    limit: Some(1),
                },
            };

            let raw: Vec<trades::BinanceAggTrade> = self
                .client
                .execute(probe)
                .await
                .map(|(response, _metric)| response)?;

            Ok(raw.first().map(|t| t.first_trade_id))
        })
    }
}
