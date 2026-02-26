use self::{
    candle::CoinbaseKline, channel::CoinbaseChannel,
    market::{CoinbaseMarket, coinbase_market},
    subscription::CoinbaseSubResponse, trade::CoinbaseTrade,
};
use crate::{
    NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::{InstrumentData, MarketInput},
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{SubKind, candle::Candles, trade::PublicTrades},
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

    fn requests(exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>]) -> Vec<WsMessage> {
        exchange_subs
            .iter()
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

    fn unsubscribe_requests(
        exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>],
    ) -> Vec<WsMessage> {
        exchange_subs
            .iter()
            .map(|ExchangeSub { channel, market }| {
                WsMessage::text(
                    json!({
                        "type": "unsubscribe",
                        "product_ids": [market.as_ref()],
                        "channels": [channel.as_ref()],
                    })
                    .to_string(),
                )
            })
            .collect()
    }

    fn resolve_market(input: MarketInput<'_>, _sub_kind: &SubKind) -> Self::Market {
        match input {
            MarketInput::Components { base, quote, .. } => coinbase_market(base, quote),
            MarketInput::ExchangeName(name) => CoinbaseMarket(name.name().clone()),
        }
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Coinbase
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, PublicTrades, CoinbaseTrade>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Coinbase
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, Candles, CoinbaseKline>;
    type Parser = WebSocketSerdeParser;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{exchange::subscription::ExchangeSub, subscription::candle::Interval};
    use smol_str::SmolStr;

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

    #[test]
    fn test_unsubscribe_requests_trades() {
        let subs = vec![ExchangeSub {
            channel: CoinbaseChannel(SmolStr::new("matches")),
            market: CoinbaseMarket(SmolStr::new("BTC-USD")),
        }];

        let messages = Coinbase::unsubscribe_requests(&subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["type"], "unsubscribe");
        assert_eq!(payload["product_ids"][0], "BTC-USD");
        assert_eq!(payload["channels"][0], "matches");
    }

    #[test]
    fn test_unsubscribe_requests_multiple() {
        let subs = vec![
            ExchangeSub {
                channel: CoinbaseChannel(SmolStr::new("matches")),
                market: CoinbaseMarket(SmolStr::new("BTC-USD")),
            },
            ExchangeSub {
                channel: CoinbaseChannel(SmolStr::new("candles")),
                market: CoinbaseMarket(SmolStr::new("ETH-USD")),
            },
        ];

        let messages = Coinbase::unsubscribe_requests(&subs);
        assert_eq!(messages.len(), 2);

        let payload0: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload0["type"], "unsubscribe");
        assert_eq!(payload0["product_ids"][0], "BTC-USD");
        assert_eq!(payload0["channels"][0], "matches");

        let payload1: serde_json::Value = serde_json::from_str(&messages[1].to_string()).unwrap();
        assert_eq!(payload1["type"], "unsubscribe");
        assert_eq!(payload1["product_ids"][0], "ETH-USD");
        assert_eq!(payload1["channels"][0], "candles");
    }
}
