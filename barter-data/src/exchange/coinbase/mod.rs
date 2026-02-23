use self::{
    candle::CoinbaseKline, channel::CoinbaseChannel, market::CoinbaseMarket,
    subscription::CoinbaseSubResponse, trade::CoinbaseTrade,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
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

/// WebSocket candle types for [`Coinbase`].
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`Coinbase`].
pub mod subscription;

/// Generic REST client for Coinbase exchange.
#[cfg(feature = "rest")]
pub mod rest;

/// Public trade types for [`Coinbase`].
pub mod trade;

use crate::{error::DataError, subscription::candle::Interval};

/// Convert a normalised [`Interval`] to the Coinbase REST API granularity string.
///
/// Returns an error for intervals not supported by the Coinbase API
/// (3m, 4h, 12h, 3d, 1w, 1M).
pub fn coinbase_interval(interval: Interval) -> Result<&'static str, DataError> {
    match interval {
        Interval::M1 => Ok("ONE_MINUTE"),
        Interval::M5 => Ok("FIVE_MINUTES"),
        Interval::M15 => Ok("FIFTEEN_MINUTES"),
        Interval::M30 => Ok("THIRTY_MINUTES"),
        Interval::H1 => Ok("ONE_HOUR"),
        Interval::H2 => Ok("TWO_HOURS"),
        Interval::H6 => Ok("SIX_HOURS"),
        Interval::D1 => Ok("ONE_DAY"),
        unsupported => Err(DataError::Socket(format!(
            "Coinbase does not support interval: {unsupported}"
        ))),
    }
}

/// [`Coinbase`] server base url.
///
/// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-overview>
pub const BASE_URL_COINBASE: &str = "wss://ws-feed.exchange.coinbase.com";

/// Convenient type alias for a Coinbase [`ExchangeWsStream`] using [`WebSocketSerdeParser`](barter_integration::protocol::websocket::WebSocketSerdeParser).
pub type CoinbaseWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Coinbase`] exchange.
///
/// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-overview>
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
pub struct Coinbase;

impl Connector for Coinbase {
    const ID: ExchangeId = ExchangeId::Coinbase;
    type Channel = CoinbaseChannel;
    type Market = CoinbaseMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = CoinbaseSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_COINBASE).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                WsMessage::text(
                    json!({
                        "type": "subscribe",
                        "product_ids": [market.as_ref()],
                        "channels": [channel.as_ref()],
                    })
                    .to_string(),
                )
            })
            .collect()
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Coinbase
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        CoinbaseWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, CoinbaseTrade>>;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Coinbase
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream =
        CoinbaseWsStream<StatelessTransformer<Self, Instrument::Key, Candles, CoinbaseKline>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;

    #[test]
    fn test_coinbase_interval_supported() {
        assert_eq!(coinbase_interval(Interval::M1).unwrap(), "ONE_MINUTE");
        assert_eq!(coinbase_interval(Interval::M5).unwrap(), "FIVE_MINUTES");
        assert_eq!(coinbase_interval(Interval::M15).unwrap(), "FIFTEEN_MINUTES");
        assert_eq!(coinbase_interval(Interval::M30).unwrap(), "THIRTY_MINUTES");
        assert_eq!(coinbase_interval(Interval::H1).unwrap(), "ONE_HOUR");
        assert_eq!(coinbase_interval(Interval::H2).unwrap(), "TWO_HOURS");
        assert_eq!(coinbase_interval(Interval::H6).unwrap(), "SIX_HOURS");
        assert_eq!(coinbase_interval(Interval::D1).unwrap(), "ONE_DAY");
    }

    #[test]
    fn test_coinbase_interval_unsupported() {
        assert!(coinbase_interval(Interval::M3).is_err());
        assert!(coinbase_interval(Interval::H4).is_err());
        assert!(coinbase_interval(Interval::H12).is_err());
        assert!(coinbase_interval(Interval::D3).is_err());
        assert!(coinbase_interval(Interval::W1).is_err());
        assert!(coinbase_interval(Interval::Month1).is_err());
    }
}
