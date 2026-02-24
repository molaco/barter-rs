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
    candle::BitfinexCandleMessage, channel::BitfinexChannel, market::BitfinexMarket,
    message::BitfinexMessage, subscription::BitfinexPlatformEvent,
    validator::BitfinexWebSocketSubValidator,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::WebSocketSubscriber,
    subscription::{candle::Candles, trade::PublicTrades},
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

/// Public candle/kline types for [`Bitfinex`].
pub mod candle;

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
                let channel_str = channel.as_ref();
                let payload = if channel_str == "candles" {
                    json!({
                        "event": "subscribe",
                        "channel": channel_str,
                        "key": market.as_ref(),
                    })
                } else {
                    json!({
                        "event": "subscribe",
                        "channel": channel_str,
                        "symbol": market.as_ref(),
                    })
                };
                WsMessage::text(payload.to_string())
            })
            .collect()
    }

    fn unsubscribe_requests(
        exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>,
    ) -> Vec<WsMessage> {
        // TODO: Bitfinex unsubscribe officially requires a server-assigned `chanId`:
        //   {"event": "unsubscribe", "chanId": N}
        // Since we don't have chanId at the Connector level, we mirror the subscribe
        // format with "unsubscribe" using channel/symbol identifiers. Bitfinex may accept
        // this format. If not, proper chanId-based unsubscribe will need to be handled
        // in the transformer layer.
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                let channel_str = channel.as_ref();
                let payload = if channel_str == "candles" {
                    json!({
                        "event": "unsubscribe",
                        "channel": channel_str,
                        "key": market.as_ref(),
                    })
                } else {
                    json!({
                        "event": "unsubscribe",
                        "channel": channel_str,
                        "symbol": market.as_ref(),
                    })
                };
                WsMessage::text(payload.to_string())
            })
            .collect()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Bitfinex
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, PublicTrades, BitfinexMessage>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Bitfinex
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, Candles, BitfinexCandleMessage>;
    type Parser = WebSocketSerdeParser;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::subscription::ExchangeSub;
    use crate::subscription::candle::Interval;
    use smol_str::SmolStr;

    #[test]
    fn test_bitfinex_interval_supported() {
        assert_eq!(bitfinex_interval(Interval::M1).unwrap(), "1m");
        assert_eq!(bitfinex_interval(Interval::M5).unwrap(), "5m");
        assert_eq!(bitfinex_interval(Interval::M15).unwrap(), "15m");
        assert_eq!(bitfinex_interval(Interval::M30).unwrap(), "30m");
        assert_eq!(bitfinex_interval(Interval::H1).unwrap(), "1h");
        assert_eq!(bitfinex_interval(Interval::H6).unwrap(), "6h");
        assert_eq!(bitfinex_interval(Interval::H12).unwrap(), "12h");
        assert_eq!(bitfinex_interval(Interval::D1).unwrap(), "1D");
        assert_eq!(bitfinex_interval(Interval::W1).unwrap(), "1W");
        assert_eq!(bitfinex_interval(Interval::Month1).unwrap(), "1M");
    }

    #[test]
    fn test_bitfinex_interval_unsupported() {
        assert!(bitfinex_interval(Interval::M3).is_err());
        assert!(bitfinex_interval(Interval::H2).is_err());
        assert!(bitfinex_interval(Interval::H4).is_err());
        assert!(bitfinex_interval(Interval::D3).is_err());
    }

    #[test]
    fn test_unsubscribe_requests_trades() {
        let subs = vec![ExchangeSub {
            channel: BitfinexChannel(SmolStr::new("trades")),
            market: BitfinexMarket(SmolStr::new("tBTCUSD")),
        }];

        let messages = Bitfinex::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["event"], "unsubscribe");
        assert_eq!(payload["channel"], "trades");
        assert_eq!(payload["symbol"], "tBTCUSD");
    }

    #[test]
    fn test_unsubscribe_requests_candles() {
        let subs = vec![ExchangeSub {
            channel: BitfinexChannel(SmolStr::new("candles")),
            market: BitfinexMarket(SmolStr::new("trade:1m:tBTCUSD")),
        }];

        let messages = Bitfinex::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["event"], "unsubscribe");
        assert_eq!(payload["channel"], "candles");
        assert_eq!(payload["key"], "trade:1m:tBTCUSD");
        // candles use "key" not "symbol"
        assert!(payload.get("symbol").is_none());
    }

    #[test]
    fn test_unsubscribe_requests_multiple() {
        let subs = vec![
            ExchangeSub {
                channel: BitfinexChannel(SmolStr::new("trades")),
                market: BitfinexMarket(SmolStr::new("tBTCUSD")),
            },
            ExchangeSub {
                channel: BitfinexChannel(SmolStr::new("candles")),
                market: BitfinexMarket(SmolStr::new("trade:1m:tETHUSD")),
            },
        ];

        let messages = Bitfinex::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 2);

        let payload0: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload0["event"], "unsubscribe");
        assert_eq!(payload0["channel"], "trades");
        assert_eq!(payload0["symbol"], "tBTCUSD");

        let payload1: serde_json::Value =
            serde_json::from_str(&messages[1].to_string()).unwrap();
        assert_eq!(payload1["event"], "unsubscribe");
        assert_eq!(payload1["channel"], "candles");
        assert_eq!(payload1["key"], "trade:1m:tETHUSD");
    }
}
