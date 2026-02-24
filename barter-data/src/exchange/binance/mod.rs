use self::{
    book::l1::BinanceOrderBookL1, candle::BinanceKline, channel::BinanceChannel,
    market::BinanceMarket, subscription::BinanceSubResponse, trade::BinanceTrade,
};
use crate::{
    NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{Map, book::OrderBooksL1, candle::Candles, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use std::{fmt::Debug, marker::PhantomData};
use url::Url;

/// OrderBook types common to both [`BinanceSpot`](spot::BinanceSpot) and
/// [`BinanceFuturesUsd`](futures::BinanceFuturesUsd).
pub mod book;

/// WebSocket candle/kline types common to both [`BinanceSpot`](spot::BinanceSpot) and
/// [`BinanceFuturesUsd`](futures::BinanceFuturesUsd).
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`BinanceFuturesUsd`](futures::BinanceFuturesUsd).
pub mod futures;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// Generic REST client for Binance exchange variants.
#[cfg(feature = "rest")]
pub mod rest;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`BinanceSpot`](spot::BinanceSpot).
pub mod spot;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) common to both [`BinanceSpot`](spot::BinanceSpot)
/// and [`BinanceFuturesUsd`](futures::BinanceFuturesUsd).
pub mod subscription;

/// Public trade types common to both [`BinanceSpot`](spot::BinanceSpot) and
/// [`BinanceFuturesUsd`](futures::BinanceFuturesUsd).
pub mod trade;

use crate::subscription::candle::Interval;

/// Convert a normalised [`Interval`] to the Binance API interval string.
pub fn binance_interval(interval: Interval) -> &'static str {
    match interval {
        Interval::M1 => "1m",
        Interval::M3 => "3m",
        Interval::M5 => "5m",
        Interval::M15 => "15m",
        Interval::M30 => "30m",
        Interval::H1 => "1h",
        Interval::H2 => "2h",
        Interval::H4 => "4h",
        Interval::H6 => "6h",
        Interval::H12 => "12h",
        Interval::D1 => "1d",
        Interval::D3 => "3d",
        Interval::W1 => "1w",
        Interval::Month1 => "1M",
    }
}

/// Generic [`Binance<Server>`](Binance) exchange.
///
/// ### Notes
/// A `Server` [`ExchangeServer`] implementations exists for
/// [`BinanceSpot`](spot::BinanceSpot) and [`BinanceFuturesUsd`](futures::BinanceFuturesUsd).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Binance<Server> {
    server: PhantomData<Server>,
}

impl<Server> Connector for Binance<Server>
where
    Server: ExchangeServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = BinanceChannel;
    type Market = BinanceMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = BinanceSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(Server::websocket_url()).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let stream_names = exchange_subs
            .into_iter()
            .map(|sub| {
                // Note:
                // Market must be lowercase when subscribing, but lowercase in general since
                // Binance sends message with uppercase MARKET (eg/ BTCUSDT).
                format!(
                    "{}{}",
                    sub.market.as_ref().to_lowercase(),
                    sub.channel.as_ref()
                )
            })
            .collect::<Vec<String>>();

        vec![WsMessage::text(
            serde_json::json!({
                "method": "SUBSCRIBE",
                "params": stream_names,
                "id": 1
            })
            .to_string(),
        )]
    }

    fn unsubscribe_requests(
        exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>,
    ) -> Vec<WsMessage> {
        let stream_names = exchange_subs
            .into_iter()
            .map(|sub| {
                format!(
                    "{}{}",
                    sub.market.as_ref().to_lowercase(),
                    sub.channel.as_ref()
                )
            })
            .collect::<Vec<String>>();

        vec![WsMessage::text(
            serde_json::json!({
                "method": "UNSUBSCRIBE",
                "params": stream_names,
                "id": 1
            })
            .to_string(),
        )]
    }

    fn expected_responses<InstrumentKey>(_: &Map<InstrumentKey>) -> usize {
        1
    }
}

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Binance<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, PublicTrades, BinanceTrade>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, Candles> for Binance<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, Candles, BinanceKline>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, OrderBooksL1> for Binance<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, BinanceOrderBookL1>;
    type Parser = WebSocketSerdeParser;
}

impl<'de, Server> serde::Deserialize<'de> for Binance<Server>
where
    Server: ExchangeServer,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = <String as serde::Deserialize>::deserialize(deserializer)?;

        if input.as_str() == Self::ID.as_str() {
            Ok(Self::default())
        } else {
            Err(serde::de::Error::invalid_value(
                serde::de::Unexpected::Str(input.as_str()),
                &Self::ID.as_str(),
            ))
        }
    }
}

impl<Server> serde::Serialize for Binance<Server>
where
    Server: ExchangeServer,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serializer.serialize_str(Self::ID.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;
    use smol_str::SmolStr;

    #[test]
    fn test_binance_interval_mapping() {
        assert_eq!(binance_interval(Interval::M1), "1m");
        assert_eq!(binance_interval(Interval::M3), "3m");
        assert_eq!(binance_interval(Interval::M5), "5m");
        assert_eq!(binance_interval(Interval::M15), "15m");
        assert_eq!(binance_interval(Interval::M30), "30m");
        assert_eq!(binance_interval(Interval::H1), "1h");
        assert_eq!(binance_interval(Interval::H2), "2h");
        assert_eq!(binance_interval(Interval::H4), "4h");
        assert_eq!(binance_interval(Interval::H6), "6h");
        assert_eq!(binance_interval(Interval::H12), "12h");
        assert_eq!(binance_interval(Interval::D1), "1d");
        assert_eq!(binance_interval(Interval::D3), "3d");
        assert_eq!(binance_interval(Interval::W1), "1w");
        assert_eq!(binance_interval(Interval::Month1), "1M");
    }

    #[test]
    fn test_unsubscribe_requests() {
        use spot::BinanceSpot;

        let exchange_subs = vec![
            ExchangeSub {
                channel: BinanceChannel(SmolStr::new_static("@trade")),
                market: BinanceMarket(SmolStr::new_static("BTCUSDT")),
            },
            ExchangeSub {
                channel: BinanceChannel(SmolStr::new_static("@kline_1m")),
                market: BinanceMarket(SmolStr::new_static("ETHUSDT")),
            },
        ];

        let messages = BinanceSpot::unsubscribe_requests(exchange_subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();

        assert_eq!(payload["method"], "UNSUBSCRIBE");
        assert_eq!(payload["id"], 1);

        let params = payload["params"].as_array().unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], "btcusdt@trade");
        assert_eq!(params[1], "ethusdt@kline_1m");
    }
}
