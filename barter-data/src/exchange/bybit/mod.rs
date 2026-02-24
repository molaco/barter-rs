use crate::{
    NoInitialSnapshots,
    exchange::{
        Connector, ExchangeServer, PingInterval, StreamSelector,
        bybit::{channel::BybitChannel, market::BybitMarket, subscription::BybitResponse},
        subscription::ExchangeSub,
    },
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{
        Map,
        book::{OrderBooksL1, OrderBooksL2},
        candle::Candles,
        trade::PublicTrades,
    },
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use book::{BybitOrderBookMessage, l2::BybitOrderBooksL2Transformer};
use candle::BybitKline;
use serde::de::{Error, Unexpected};
use std::{fmt::Debug, marker::PhantomData, time::Duration};
use tokio::time;
use trade::BybitTrade;
use url::Url;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`BybitFuturesUsd`](futures::BybitPerpetualsUsd).
pub mod futures;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// Generic [`BybitPayload<T>`](message::BybitPayload) type common to
/// [`BybitSpot`](spot::BybitSpot)
pub mod message;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`BybitSpot`](spot::BybitSpot).
pub mod spot;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) common to both [`BybitSpot`](spot::BybitSpot)
/// and [`BybitFuturesUsd`](futures::BybitPerpetualsUsd).
pub mod subscription;

/// Public trade types common to both [`BybitSpot`](spot::BybitSpot) and
/// [`BybitFuturesUsd`](futures::BybitPerpetualsUsd).
pub mod trade;

/// Orderbook types common to both [`BybitSpot`](spot::BybitSpot) and
/// [`BybitFuturesUsd`](futures::BybitPerpetualsUsd).
pub mod book;

/// WebSocket candle/kline types common to both [`BybitSpot`](spot::BybitSpot) and
/// [`BybitPerpetualsUsd`](futures::BybitPerpetualsUsd).
pub mod candle;

use crate::subscription::candle::Interval;

/// Convert a normalised [`Interval`] to the Bybit API interval string.
///
/// Bybit interval format: "1" (1m), "3", "5", "15", "30", "60" (1h),
/// "120" (2h), "240" (4h), "360" (6h), "720" (12h), "D", "W", "M".
///
/// Note: Bybit does not have a 3-day interval. [`Interval::D3`] falls back to "D".
pub fn bybit_interval(interval: Interval) -> &'static str {
    match interval {
        Interval::M1 => "1",
        Interval::M3 => "3",
        Interval::M5 => "5",
        Interval::M15 => "15",
        Interval::M30 => "30",
        Interval::H1 => "60",
        Interval::H2 => "120",
        Interval::H4 => "240",
        Interval::H6 => "360",
        Interval::H12 => "720",
        Interval::D1 => "D",
        Interval::D3 => "D",
        Interval::W1 => "W",
        Interval::Month1 => "M",
    }
}

/// REST API client and kline fetching for Bybit.
#[cfg(feature = "rest")]
pub mod rest;

/// Generic [`Bybit<Server>`](Bybit) exchange.
///
/// ### Notes
/// A `Server` [`ExchangeServer`] implementations exists for
/// [`BybitSpot`](spot::BybitSpot) and [`BybitFuturesUsd`](futures::BybitPerpetualsUsd).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Bybit<Server> {
    server: PhantomData<Server>,
}

impl<Server> Connector for Bybit<Server>
where
    Server: ExchangeServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = BybitChannel;
    type Market = BybitMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = BybitResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(Server::websocket_url()).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: time::interval(Duration::from_millis(5_000)),
            ping: || {
                WsMessage::text(
                    serde_json::json!({
                        "op": "ping",
                    })
                    .to_string(),
                )
            },
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let stream_names = exchange_subs
            .into_iter()
            .map(|sub| format!("{}.{}", sub.channel.as_ref(), sub.market.as_ref(),))
            .collect::<Vec<String>>();

        vec![WsMessage::text(
            serde_json::json!({
                "op": "subscribe",
                "args": stream_names
            })
            .to_string(),
        )]
    }

    fn unsubscribe_requests(
        exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>,
    ) -> Vec<WsMessage> {
        let stream_names = exchange_subs
            .into_iter()
            .map(|sub| format!("{}.{}", sub.channel.as_ref(), sub.market.as_ref()))
            .collect::<Vec<String>>();

        vec![WsMessage::text(
            serde_json::json!({
                "op": "unsubscribe",
                "args": stream_names
            })
            .to_string(),
        )]
    }

    fn expected_responses<InstrumentKey>(_: &Map<InstrumentKey>) -> usize {
        1
    }
}

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Bybit<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, PublicTrades, BybitTrade>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, Candles> for Bybit<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, Candles, BybitKline>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, OrderBooksL1> for Bybit<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, OrderBooksL1, BybitOrderBookMessage>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, OrderBooksL2> for Bybit<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = BybitOrderBooksL2Transformer<Instrument::Key>;
    type Parser = WebSocketSerdeParser;
}

impl<'de, Server> serde::Deserialize<'de> for Bybit<Server>
where
    Server: ExchangeServer,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = <&str as serde::Deserialize>::deserialize(deserializer)?;

        if input == Self::ID.as_str() {
            Ok(Self::default())
        } else {
            Err(Error::invalid_value(
                Unexpected::Str(input),
                &Self::ID.as_str(),
            ))
        }
    }
}

impl<Server> serde::Serialize for Bybit<Server>
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
    fn test_bybit_interval_mapping() {
        assert_eq!(bybit_interval(Interval::M1), "1");
        assert_eq!(bybit_interval(Interval::M3), "3");
        assert_eq!(bybit_interval(Interval::M5), "5");
        assert_eq!(bybit_interval(Interval::M15), "15");
        assert_eq!(bybit_interval(Interval::M30), "30");
        assert_eq!(bybit_interval(Interval::H1), "60");
        assert_eq!(bybit_interval(Interval::H2), "120");
        assert_eq!(bybit_interval(Interval::H4), "240");
        assert_eq!(bybit_interval(Interval::H6), "360");
        assert_eq!(bybit_interval(Interval::H12), "720");
        assert_eq!(bybit_interval(Interval::D1), "D");
        assert_eq!(bybit_interval(Interval::D3), "D");
        assert_eq!(bybit_interval(Interval::W1), "W");
        assert_eq!(bybit_interval(Interval::Month1), "M");
    }

    #[test]
    fn test_unsubscribe_requests() {
        use spot::BybitSpot;

        let exchange_subs = vec![
            ExchangeSub {
                channel: BybitChannel(SmolStr::new_static("publicTrade")),
                market: BybitMarket(SmolStr::new_static("BTCUSDT")),
            },
            ExchangeSub {
                channel: BybitChannel(SmolStr::new_static("kline.1")),
                market: BybitMarket(SmolStr::new_static("ETHUSDT")),
            },
        ];

        let messages = BybitSpot::unsubscribe_requests(exchange_subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();

        assert_eq!(payload["op"], "unsubscribe");

        let args = payload["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "publicTrade.BTCUSDT");
        assert_eq!(args[1], "kline.1.ETHUSDT");
    }
}
