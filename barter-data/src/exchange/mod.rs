use self::subscription::ExchangeSub;
use crate::{
    SnapshotFetcher,
    instrument::{InstrumentData, MarketInput},
    subscriber::{Subscriber, validator::SubscriptionValidator},
    subscription::{Map, SubKind, SubscriptionKind},
    transformer::ExchangeTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, Validator,
    error::SocketError,
    protocol::{
        StreamParser,
        websocket::{WsError, WsMessage},
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{fmt::Debug, time::Duration};
use url::Url;

/// `BinanceSpot` & `BinanceFuturesUsd` [`Connector`] and [`StreamSelector`] implementations.
pub mod binance;

/// `Bitfinex` [`Connector`] and [`StreamSelector`] implementations.
pub mod bitfinex;

/// `Bitmex [`Connector`] and [`StreamSelector`] implementations.
pub mod bitmex;

/// `Bybit` ['Connector'] and ['StreamSelector'] implementation
pub mod bybit;

/// `Coinbase` [`Connector`] and [`StreamSelector`] implementations.
pub mod coinbase;

/// `GateioSpot`, `GateioFuturesUsd` & `GateioFuturesBtc` [`Connector`] and [`StreamSelector`]
/// implementations.
pub mod gateio;

/// `Hyperliquid` [`Connector`] and [`StreamSelector`] implementations.
pub mod hyperliquid;

/// `Kraken` [`Connector`] and [`StreamSelector`] implementations.
pub mod kraken;

/// `Okx` [`Connector`] and [`StreamSelector`] implementations.
pub mod okx;

/// Defines the generic [`ExchangeSub`] containing a market and channel combination used by an
/// exchange [`Connector`] to build [`WsMessage`] subscription payloads.
pub mod subscription;

/// Default [`Duration`] the [`Connector::SubValidator`] will wait to receive all success responses to actioned
/// `Subscription` requests.
pub const DEFAULT_SUBSCRIPTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Defines the associated types for streaming market data for a specific
/// exchange `Subscription` [`SubscriptionKind`].
///
/// ### Notes
/// Must be implemented by an exchange [`Connector`] if it supports a specific
/// [`SubscriptionKind`].
pub trait StreamSelector<Instrument, Kind>
where
    Self: Connector,
    Instrument: InstrumentData,
    Kind: SubscriptionKind,
{
    type SnapFetcher: SnapshotFetcher<Self, Kind>;
    type Transformer: ExchangeTransformer<Self, Instrument::Key, Kind> + Send;
    type Parser: StreamParser<
            <Self::Transformer as Transformer>::Input,
            Message = WsMessage,
            Error = WsError,
        > + Send;
}

/// Primary exchange abstraction. Defines how to translate Barter types into exchange specific
/// types, as well as connecting, subscribing, and interacting with the exchange server.
///
/// ### Notes
/// This must be implemented for a new exchange integration!
pub trait Connector
where
    Self: Clone + Default + Debug + for<'de> Deserialize<'de> + Serialize + Sized,
{
    /// Unique identifier for the exchange server being connected with.
    const ID: ExchangeId;

    /// Type that defines how to translate a Barter `Subscription` into an exchange specific
    /// channel to be subscribed to.
    ///
    /// ### Examples
    /// - [`BinanceChannel("@depth@100ms")`](binance::channel::BinanceChannel)
    /// - [`KrakenChannel("trade")`](kraken::channel::KrakenChannel)
    type Channel: AsRef<str> + Clone + Send;

    /// Type that defines how to translate a Barter
    /// `Subscription` into an exchange specific market that
    /// can be subscribed to.
    ///
    /// ### Examples
    /// - [`BinanceMarket("btcusdt")`](binance::market::BinanceMarket)
    /// - [`KrakenMarket("BTC/USDT")`](kraken::market::KrakenMarket)
    type Market: AsRef<str> + Clone + Send;

    /// [`Subscriber`] type that establishes a connection with the exchange server, and actions
    /// `Subscription`s over the socket.
    type Subscriber: Subscriber;

    /// [`SubscriptionValidator`] type that listens to responses from the exchange server and
    /// validates if the actioned `Subscription`s were
    /// successful.
    type SubValidator: SubscriptionValidator;

    /// Deserialisable type that the [`Self::SubValidator`] expects to receive from the exchange server in
    /// response to the `Subscription` [`Self::requests`]
    /// sent over the [`WebSocket`](barter_integration::protocol::websocket::WebSocket). Implements
    /// [`Validator`] in order to determine if [`Self`]
    /// communicates a successful `Subscription` outcome.
    type SubResponse: Validator + Debug + DeserializeOwned;

    /// Base [`Url`] of the exchange server being connected with.
    fn url() -> Result<Url, SocketError>;

    /// Defines [`PingInterval`] of custom application-level
    /// [`WebSocket`](barter_integration::protocol::websocket::WebSocket) pings for the exchange
    /// server being connected with.
    ///
    /// Defaults to `None`, meaning that no custom pings are sent.
    fn ping_interval() -> Option<PingInterval> {
        None
    }

    /// Defines how to translate a collection of [`ExchangeSub`]s into the [`WsMessage`]
    /// subscription payloads sent to the exchange server.
    fn requests(exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>]) -> Vec<WsMessage>;

    /// Defines how to translate a collection of [`ExchangeSub`]s into the [`WsMessage`]
    /// unsubscription payloads sent to the exchange server.
    ///
    /// Default implementation returns an empty vec (no unsubscribe support).
    fn unsubscribe_requests(
        _exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>],
    ) -> Vec<WsMessage> {
        vec![]
    }

    /// Number of `Subscription` responses expected from the
    /// exchange server in responses to the requests send. Used to validate all
    /// `Subscription`s were accepted.
    fn expected_responses<InstrumentKey>(map: &Map<InstrumentKey>) -> usize {
        map.0.len()
    }

    /// Expected [`Duration`] the [`SubscriptionValidator`] will wait to receive all success
    /// responses to actioned `Subscription` requests.
    fn subscription_timeout() -> Duration {
        DEFAULT_SUBSCRIPTION_TIMEOUT
    }

    /// Resolve a [`MarketInput`] and [`SubKind`] into the exchange-specific [`Self::Market`].
    fn resolve_market(input: MarketInput<'_>, sub_kind: &SubKind) -> Self::Market;

    /// Optional WebSocket send rate limit as `(max_requests, per_duration)`.
    ///
    /// When `Some`, the subscriber will throttle `ws.send()` calls to stay
    /// within the exchange's documented limits and avoid being banned.
    ///
    /// Default: `None` (no rate limit).
    fn send_rate_limit() -> Option<(u32, Duration)> {
        None
    }
}

/// Split subscription requests into chunks that fit within `max_frame_bytes`.
///
/// Each item is serialized to JSON to estimate its wire size. Items are accumulated into
/// the current chunk until adding the next item would exceed the limit, at which point a
/// new chunk is started. Items that individually exceed the limit are placed in their own
/// single-element chunk.
pub fn chunk_requests_by_frame_size<T: serde::Serialize>(
    requests: Vec<T>,
    max_frame_bytes: usize,
) -> Vec<Vec<T>> {
    let mut chunks: Vec<Vec<T>> = Vec::new();
    let mut current_chunk: Vec<T> = Vec::new();
    let mut current_size: usize = 0;

    for item in requests {
        let item_size = serde_json::to_string(&item).map(|s| s.len()).unwrap_or(0);

        if !current_chunk.is_empty() && current_size + item_size > max_frame_bytes {
            chunks.push(std::mem::take(&mut current_chunk));
            current_size = 0;
        }

        current_size += item_size;
        current_chunk.push(item);
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    chunks
}

/// Used when an exchange has servers different
/// [`InstrumentKind`](barter_instrument::instrument::kind::InstrumentKind) market data on distinct servers,
/// allowing all the [`Connector`] logic to be identical apart from what this trait provides.
///
/// ### Examples
/// - [`BinanceServerSpot`](binance::spot::BinanceServerSpot)
/// - [`BinanceServerFuturesUsd`](binance::futures::BinanceServerFuturesUsd)
pub trait ExchangeServer: Default + Debug + Clone + Send {
    const ID: ExchangeId;
    fn websocket_url() -> &'static str;
}

/// Used when an exchange has different REST API base URLs for different
/// [`InstrumentKind`](barter_instrument::instrument::kind::InstrumentKind) markets.
pub trait RestExchangeServer: Default + Debug + Clone + Send {
    const ID: ExchangeId;

    /// Base URL for REST API (e.g., "https://api.binance.com")
    fn rest_base_url() -> &'static str;

    /// Path to the klines endpoint (e.g., "/api/v3/klines")
    fn klines_path() -> &'static str;

    /// Path to the trades endpoint (e.g., "/api/v3/aggTrades").
    ///
    /// Note: Some exchanges (e.g., Coinbase) require dynamic paths that
    /// include the symbol in the URL. In those cases, the `TradeFetcher`
    /// implementation constructs the full path directly rather than using
    /// this method.
    fn trades_path() -> &'static str;

    /// Path to the individual (raw) trades endpoint
    /// (e.g., "/api/v3/historicalTrades").
    ///
    /// Used to fetch *individual* trades that match the live `@trade` stream
    /// over a past window via `fromId` pagination, as opposed to aggregated
    /// trades from [`trades_path`](Self::trades_path). The default empty
    /// string indicates the exchange does not support raw historical-trade
    /// fetching.
    fn historical_trades_path() -> &'static str {
        ""
    }

    /// Maximum time window allowed per trades request.
    /// Returns `None` if there is no limit (default).
    /// Binance Futures limits `endTime - startTime` to < 1 hour.
    fn max_trades_time_window() -> Option<chrono::TimeDelta> {
        None
    }
}

/// Defines the frequency and construction function for custom
/// [`WebSocket`](barter_integration::protocol::websocket::WebSocket) pings - used for exchanges
/// that require additional application-level pings.
#[derive(Debug)]
pub struct PingInterval {
    pub interval: tokio::time::Interval,
    pub ping: fn() -> WsMessage,
}
