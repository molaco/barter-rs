use self::{
    book::l1::KrakenOrderBookL1, candle::KrakenKline, channel::KrakenChannel,
    market::KrakenMarket, message::KrakenMessage, subscription::KrakenSubResponse,
    trade::KrakenTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{book::OrderBooksL1, candle::Candles, trade::PublicTrades},
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

/// OrderBook types for [`Kraken`].
pub mod book;

/// WebSocket candle/OHLC types for [`Kraken`].
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`]  specific market used for generating [`Connector::requests`].
pub mod market;

/// [`KrakenMessage`] type for [`Kraken`].
pub mod message;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration) for [`Kraken`].
pub mod subscription;

/// Generic REST client for Kraken exchange.
#[cfg(feature = "rest")]
pub mod rest;

/// Public trade types for [`Kraken`].
pub mod trade;

use crate::{error::DataError, subscription::candle::Interval};

/// Convert a normalised [`Interval`] to the Kraken API integer-minutes interval.
///
/// Kraken supports: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600.
/// Returns a [`DataError`] for unsupported intervals (3m, 2h, 6h, 12h, 3d, 1M).
pub fn kraken_interval(interval: Interval) -> Result<u32, DataError> {
    match interval {
        Interval::M1 => Ok(1),
        Interval::M5 => Ok(5),
        Interval::M15 => Ok(15),
        Interval::M30 => Ok(30),
        Interval::H1 => Ok(60),
        Interval::H4 => Ok(240),
        Interval::D1 => Ok(1440),
        Interval::W1 => Ok(10080),
        Interval::M3 => Err(DataError::Socket(
            "Kraken does not support 3m interval".to_string(),
        )),
        Interval::H2 => Err(DataError::Socket(
            "Kraken does not support 2h interval".to_string(),
        )),
        Interval::H6 => Err(DataError::Socket(
            "Kraken does not support 6h interval".to_string(),
        )),
        Interval::H12 => Err(DataError::Socket(
            "Kraken does not support 12h interval".to_string(),
        )),
        Interval::D3 => Err(DataError::Socket(
            "Kraken does not support 3d interval".to_string(),
        )),
        Interval::Month1 => Err(DataError::Socket(
            "Kraken does not support 1M interval".to_string(),
        )),
    }
}

/// [`Kraken`] server base url.
///
/// See docs: <https://docs.kraken.com/websockets/#overview>
pub const BASE_URL_KRAKEN: &str = "wss://ws.kraken.com/";

/// Convenient type alias for a Kraken [`ExchangeWsStream`] using [`WebSocketSerdeParser`](barter_integration::protocol::websocket::WebSocketSerdeParser).
pub type KrakenWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Kraken`] exchange.
///
/// See docs: <https://docs.kraken.com/websockets/#overview>
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
pub struct Kraken;

impl Connector for Kraken {
    const ID: ExchangeId = ExchangeId::Kraken;
    type Channel = KrakenChannel;
    type Market = KrakenMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = KrakenSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_KRAKEN).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                let channel_str = channel.as_ref();

                // Kraken OHLC channels are encoded as "ohlc-<interval>" (eg/ "ohlc-1").
                // The subscription request requires "name": "ohlc" with a separate
                // "interval" field.
                let subscription = if let Some(interval_str) = channel_str.strip_prefix("ohlc-") {
                    let interval: u32 = interval_str.parse().unwrap_or(1);
                    json!({
                        "name": "ohlc",
                        "interval": interval
                    })
                } else {
                    json!({
                        "name": channel_str
                    })
                };

                WsMessage::text(
                    json!({
                        "event": "subscribe",
                        "pair": [market.as_ref()],
                        "subscription": subscription
                    })
                    .to_string(),
                )
            })
            .collect()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Kraken
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        KrakenWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, KrakenTrades>>;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Kraken
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        KrakenWsStream<StatelessTransformer<Self, Instrument::Key, Candles, KrakenKline>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for Kraken
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = KrakenWsStream<
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, KrakenOrderBookL1>,
    >;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;

    #[test]
    fn test_kraken_interval_supported() {
        assert_eq!(kraken_interval(Interval::M1).unwrap(), 1);
        assert_eq!(kraken_interval(Interval::M5).unwrap(), 5);
        assert_eq!(kraken_interval(Interval::M15).unwrap(), 15);
        assert_eq!(kraken_interval(Interval::M30).unwrap(), 30);
        assert_eq!(kraken_interval(Interval::H1).unwrap(), 60);
        assert_eq!(kraken_interval(Interval::H4).unwrap(), 240);
        assert_eq!(kraken_interval(Interval::D1).unwrap(), 1440);
        assert_eq!(kraken_interval(Interval::W1).unwrap(), 10080);
    }

    #[test]
    fn test_kraken_interval_unsupported() {
        assert!(kraken_interval(Interval::M3).is_err());
        assert!(kraken_interval(Interval::H2).is_err());
        assert!(kraken_interval(Interval::H6).is_err());
        assert!(kraken_interval(Interval::H12).is_err());
        assert!(kraken_interval(Interval::D3).is_err());
        assert!(kraken_interval(Interval::Month1).is_err());
    }
}
