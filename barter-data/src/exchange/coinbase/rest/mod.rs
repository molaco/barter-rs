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
use tracing::{Instrument, debug, warn};

/// Coinbase kline/candlestick REST request, raw DTO, and conversion to [`Candle`](crate::subscription::candle::Candle).
pub mod klines;

/// Coinbase trade REST request, raw DTO, and conversion to [`RestTrade`](crate::rest::RestTrade).
pub mod trades;

/// Coinbase REST API error payload.
///
/// Returned by the Coinbase API when a request fails, e.g.:
/// ```json
/// { "error": "NOT_FOUND", "message": "product not found" }
/// ```
#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct CoinbaseApiError {
    pub(crate) error: String,
    pub(crate) message: String,
}

/// HTTP response parser for Coinbase REST API responses.
#[derive(Debug)]
pub struct CoinbaseHttpParser;

impl HttpParser for CoinbaseHttpParser {
    type ApiError = CoinbaseApiError;
    type OutputError = DataError;

    fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
        DataError::ExchangeApi {
            exchange: "coinbase".into(),
            code: error.error,
            message: error.message,
        }
    }
}

/// Base URL for the Coinbase REST API.
const COINBASE_REST_BASE_URL: &str = "https://api.coinbase.com";

/// REST client for the Coinbase exchange.
///
/// Unlike the Binance client this is not generic over a server variant
/// because Coinbase uses a single REST API endpoint for all markets.
///
/// Includes a rate limiter configured for 10 requests per second.
#[non_exhaustive]
#[derive(Clone)]
pub struct CoinbaseRestClient {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, CoinbaseHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
}

impl fmt::Debug for CoinbaseRestClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoinbaseRestClient")
            .field("client", &self.client)
            .field("rate_limiter", &"ExchangeRateLimiter { .. }")
            .finish()
    }
}

impl Default for CoinbaseRestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl CoinbaseRestClient {
    /// Construct a new [`CoinbaseRestClient`] using the default Coinbase
    /// REST API base URL.
    ///
    /// Initialises a rate limiter with a quota of 10 requests per second.
    pub fn new() -> Self {
        Self::with_base_url(COINBASE_REST_BASE_URL.to_owned())
    }

    /// Construct a new [`CoinbaseRestClient`] using the default Coinbase
    /// REST API base URL and a caller-provided rate limiter.
    ///
    /// This is useful when you want to share a single rate limiter across
    /// multiple client instances or customise the quota.
    pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
        let client = RestClient::new(
            COINBASE_REST_BASE_URL.to_owned(),
            PublicNoHeaders,
            CoinbaseHttpParser,
        );
        Self {
            client: Arc::new(client),
            rate_limiter,
        }
    }

    /// Construct a [`CoinbaseRestClient`] with a custom base URL.
    ///
    /// Useful for testing with a mock server where the URL is not known at
    /// compile time.
    pub fn with_base_url(base_url: String) -> Self {
        let client = RestClient::new(base_url, PublicNoHeaders, CoinbaseHttpParser);
        // 10 is non-zero
        let quota = Quota::per_second(NonZeroU32::new(10).unwrap()).allow_burst(NonZeroU32::new(10).unwrap());
        let rate_limiter = governor::RateLimiter::direct(quota);
        Self {
            client: Arc::new(client),
            rate_limiter: Arc::new(rate_limiter),
        }
    }

    /// Return a reference to the inner HTTP client.
    pub fn http_client(&self) -> &Arc<RestClient<'static, PublicNoHeaders, CoinbaseHttpParser>> {
        &self.client
    }

    /// Wait until the rate limiter permits the next request.
    ///
    /// Call this before each REST API request to stay within Coinbase
    /// rate limits. Blocks asynchronously until a permit is available.
    pub async fn wait_for_rate_limit(&self) {
        debug!("waiting for rate limit permit");
        self.rate_limiter.until_ready().await;
    }
}

impl KlineFetcher for CoinbaseRestClient {
    fn supported_intervals() -> &'static [Interval] {
        &[
            Interval::M1,
            Interval::M5,
            Interval::M15,
            Interval::M30,
            Interval::H1,
            Interval::H2,
            Interval::H6,
            Interval::D1,
        ]
    }

    /// Fetch a single batch of klines from the Coinbase REST API.
    ///
    /// Builds a [`GetCoinbaseKlines`](klines::GetCoinbaseKlines) request from the provided
    /// [`KlineRequest`], executes the request, and converts raw DTOs into [`Candle`]s.
    /// This is a single-attempt call; retry logic is handled by the collector.
    ///
    /// The Coinbase API returns candles newest-first, so the result is
    /// reversed to oldest-first before returning.
    fn fetch_klines(
        &self,
        request: KlineRequest,
    ) -> impl std::future::Future<Output = Result<Vec<Candle>, DataError>> + Send {
        let this = self.clone();
        let span = tracing::info_span!(
            "fetch_klines",
            exchange = "coinbase",
            market = %request.market,
            interval = %request.interval,
        );
        async move {
            debug!("building klines request");

            let granularity = klines::coinbase_interval(request.interval)?;

            let path = format!(
                "/api/v3/brokerage/market/products/{}/candles",
                request.market
            );

            let get_klines_request = klines::GetCoinbaseKlines {
                path,
                params: klines::GetCoinbaseKlinesParams {
                    start: request.start.map(|dt| dt.timestamp()),
                    end: request.end.map(|dt| dt.timestamp()),
                    granularity: granularity.to_string(),
                },
            };

            let response: klines::CoinbaseKlinesResponse =
                match this.client.execute(get_klines_request).await.map(|(response, _metric)| response) {
                    Ok(resp) => resp,
                    Err(error) => {
                        warn!(?error, "klines fetch failed");
                        return Err(error);
                    }
                };

            let interval = request.interval;
            let mut candles = response
                .candles
                .into_iter()
                .map(|raw| raw.into_candle(interval))
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::DataParse)?;

            // Coinbase returns newest-first; reverse to oldest-first
            candles.reverse();

            debug!(count = candles.len(), "fetched klines batch");

            Ok(candles)
        }
        .instrument(span)
    }
}

impl TradeFetcher for CoinbaseRestClient {
    /// Fetch a single batch of trades from the Coinbase REST API.
    ///
    /// Builds a [`GetCoinbaseTrades`](trades::GetCoinbaseTrades) request from the provided
    /// [`TradeRequest`], executes the request, reverses the results (Coinbase returns
    /// newest-first), and converts raw DTOs into [`RestTrade`]s. This is a single-attempt
    /// call; retry logic is handled by the collector.
    fn fetch_trades(
        &self,
        request: TradeRequest,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>> {
        let this = self.clone();
        let start = request.start;
        let end = request.end;
        let span = tracing::info_span!(
            "fetch_trades",
            exchange = "coinbase",
            market = %request.market,
        );
        Box::pin(async move {
            debug!("building trades request");

            let path = format!(
                "{}/{}/ticker",
                trades::COINBASE_TICKER_PATH_PREFIX,
                request.market
            );

            let get_trades_request = trades::GetCoinbaseTrades {
                path,
                params: trades::GetCoinbaseTradesParams {
                    start: request.start.map(|dt| dt.timestamp()),
                    end: request.end.map(|dt| dt.timestamp()),
                    limit: request.limit,
                },
            };

            let response: trades::CoinbaseTradesResponse =
                match this.client.execute(get_trades_request).await.map(|(response, _metric)| response) {
                    Ok(resp) => resp,
                    Err(error) => {
                        warn!(?error, "trades fetch failed");
                        return Err(error);
                    }
                };

            // Coinbase returns newest-first; reverse to oldest-first
            let mut raw_trades = response.trades;
            raw_trades.reverse();

            let trades: Vec<RestTrade> = raw_trades
                .into_iter()
                .map(RestTrade::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(DataError::DataParse)?;

            // Filter to [start, end] range if specified
            let trades: Vec<RestTrade> = trades
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
                .collect();

            debug!(count = trades.len(), "fetched trades batch");

            Ok(trades)
        }
        .instrument(span))
    }
}
