//!
//! ### Notes
//! #### SubscripionId
//! - Successful Bitfinex subscription responses contain a numeric `CHANNEL_ID` that must be used to
//!   identify future messages relating to that subscription (not persistent across connections).
//! - To identify the initial subscription response containing the `CHANNEL_ID`, the "channel" &
//!   "market" identifiers can be used for the `SubscriptionId(channel|market)`
//!   (eg/ SubscriptionId("trades|tBTCUSD")).
//! - Once the subscription has been validated and the `CHANNEL_ID` determined, each `SubscriptionId`
//!   in the `SubscriptionIds` `HashMap` is mutated to become `SubscriptionId(CHANNEL_ID)`.
//!   eg/ SubscriptionId("trades|tBTCUSD") -> SubscriptionId(69)
//!
//! #### Connection Limits
//! - The user is allowed up to 20 connections per minute on the public API.
//! - Each connection can be used to connect up to 25 different channels.
//!
//! #### Trade Variants
//! - Bitfinex trades subscriptions results in receiving tag="te" & tag="tu" trades.
//! - Both appear to be identical payloads, but "te" arriving marginally faster.
//! - Therefore, tag="tu" trades are filtered out and considered only as additional Heartbeats.

use self::{
    channel::BitfinexChannel, market::BitfinexMarket, message::BitfinexMessage,
    subscription::BitfinexPlatformEvent, validator::BitfinexWebSocketSubValidator,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::WebSocketSubscriber,
    subscription::trade::PublicTrades,
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use barter_macro::{DeExchange, SerExchange};
use derive_more::Display;
use serde_json::json;
use url::Url;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`BitfinexMessage`] type for [`Bitfinex`].
pub mod message;

/// [`Subscription`](crate::subscription::Subscription) response types and response
/// [`Validator`](barter_integration::Validator) for [`Bitfinex`].
pub mod subscription;

/// Public trade types for [`Bitfinex`].
pub mod trade;

/// Custom `SubscriptionValidator` implementation for [`Bitfinex`].
pub mod validator;

use crate::{error::DataError, subscription::candle::Interval};

/// Convert a normalised [`Interval`] to the Bitfinex API interval string.
///
/// Bitfinex supports: 1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 1W, 14D, 1M.
pub fn bitfinex_interval(interval: Interval) -> Result<&'static str, DataError> {
    match interval {
        Interval::M1 => Ok("1m"),
        Interval::M5 => Ok("5m"),
        Interval::M15 => Ok("15m"),
        Interval::M30 => Ok("30m"),
        Interval::H1 => Ok("1h"),
        Interval::H6 => Ok("6h"),
        Interval::H12 => Ok("12h"),
        Interval::D1 => Ok("1D"),
        Interval::W1 => Ok("1W"),
        Interval::Month1 => Ok("1M"),
        unsupported => Err(DataError::Socket(format!(
            "Bitfinex does not support interval: {unsupported}"
        ))),
    }
}

/// [`Bitfinex`] server base url.
///
/// See docs: <https://docs.bitfinex.com/docs/ws-general>
pub const BASE_URL_BITFINEX: &str = "wss://api-pub.bitfinex.com/ws/2";

/// Convenient type alias for a Bitfinex [`ExchangeWsStream`] using [`WebSocketSerdeParser`](barter_integration::protocol::websocket::WebSocketSerdeParser).
pub type BitfinexWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Bitfinex`] exchange.
///
/// See docs: <https://docs.bitfinex.com/docs/ws-general>
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
)]
pub struct Bitfinex;

impl Connector for Bitfinex {
    const ID: ExchangeId = ExchangeId::Bitfinex;
    type Channel = BitfinexChannel;
    type Market = BitfinexMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = BitfinexWebSocketSubValidator;
    type SubResponse = BitfinexPlatformEvent;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_BITFINEX).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                WsMessage::text(
                    json!({
                        "event": "subscribe",
                        "channel": channel.as_ref(),
                        "symbol": market.as_ref(),
                    })
                    .to_string(),
                )
            })
            .collect()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Bitfinex
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = BitfinexWsStream<
        StatelessTransformer<Self, Instrument::Key, PublicTrades, BitfinexMessage>,
    >;
}
