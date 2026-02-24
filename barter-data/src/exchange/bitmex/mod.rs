use crate::{
    NoInitialSnapshots,
    exchange::{
        Connector, StreamSelector,
        bitmex::{
            candle::BitmexKline,
            channel::BitmexChannel, market::BitmexMarket, subscription::BitmexSubResponse,
            trade::BitmexTrade,
        },
        subscription::ExchangeSub,
    },
    instrument::InstrumentData,
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{Map, candle::Candles, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use derive_more::Display;
use serde::de::{Error, Unexpected};
use std::fmt::Debug;
use url::Url;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// Generic [`BitmexMessage<T>`](message::BitmexMessage)
pub mod message;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`Bitmex`].
pub mod subscription;

/// Public trade types for [`Bitmex`].
pub mod trade;

/// Public candle/kline types for [`Bitmex`].
pub mod candle;

use crate::{error::DataError, subscription::candle::Interval};

/// Convert a normalised [`Interval`] to the BitMEX API interval string.
///
/// BitMEX only supports 4 intervals: 1m, 5m, 1h, 1d.
pub fn bitmex_interval(interval: Interval) -> Result<&'static str, DataError> {
    match interval {
        Interval::M1 => Ok("1m"),
        Interval::M5 => Ok("5m"),
        Interval::H1 => Ok("1h"),
        Interval::D1 => Ok("1d"),
        unsupported => Err(DataError::Socket(format!(
            "BitMEX does not support interval: {unsupported}"
        ))),
    }
}

/// [`Bitmex`] server base url.
///
/// See docs: <https://www.bitmex.com/app/wsAPI>
pub const BASE_URL_BITMEX: &str = "wss://ws.bitmex.com/realtime";

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, Display)]
pub struct Bitmex;

impl Connector for Bitmex {
    const ID: ExchangeId = ExchangeId::Bitmex;
    type Channel = BitmexChannel;
    type Market = BitmexMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = BitmexSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_BITMEX).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let stream_names = exchange_subs
            .into_iter()
            .map(|sub| format!("{}:{}", sub.channel.as_ref(), sub.market.as_ref(),))
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
            .map(|sub| format!("{}:{}", sub.channel.as_ref(), sub.market.as_ref(),))
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

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Bitmex
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, PublicTrades, BitmexTrade>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Bitmex
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, Candles, BitmexKline>;
    type Parser = WebSocketSerdeParser;
}

impl<'de> serde::Deserialize<'de> for Bitmex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let input = <&str as serde::Deserialize>::deserialize(deserializer)?;
        if input == Self::ID.as_str() {
            Ok(Self)
        } else {
            Err(Error::invalid_value(
                Unexpected::Str(input),
                &Self::ID.as_str(),
            ))
        }
    }
}

impl serde::Serialize for Bitmex {
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
    use crate::exchange::subscription::ExchangeSub;
    use crate::subscription::candle::Interval;
    use smol_str::SmolStr;

    #[test]
    fn test_bitmex_interval_supported() {
        assert_eq!(bitmex_interval(Interval::M1).unwrap(), "1m");
        assert_eq!(bitmex_interval(Interval::M5).unwrap(), "5m");
        assert_eq!(bitmex_interval(Interval::H1).unwrap(), "1h");
        assert_eq!(bitmex_interval(Interval::D1).unwrap(), "1d");
    }

    #[test]
    fn test_bitmex_interval_unsupported() {
        assert!(bitmex_interval(Interval::M3).is_err());
        assert!(bitmex_interval(Interval::M15).is_err());
        assert!(bitmex_interval(Interval::M30).is_err());
        assert!(bitmex_interval(Interval::H2).is_err());
        assert!(bitmex_interval(Interval::H4).is_err());
        assert!(bitmex_interval(Interval::H6).is_err());
        assert!(bitmex_interval(Interval::H12).is_err());
        assert!(bitmex_interval(Interval::D3).is_err());
        assert!(bitmex_interval(Interval::W1).is_err());
        assert!(bitmex_interval(Interval::Month1).is_err());
    }

    #[test]
    fn test_unsubscribe_requests_single() {
        let subs = vec![ExchangeSub {
            channel: BitmexChannel(SmolStr::new("trade")),
            market: BitmexMarket(SmolStr::new("XBTUSD")),
        }];

        let messages = Bitmex::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["op"], "unsubscribe");
        assert_eq!(payload["args"][0], "trade:XBTUSD");
    }

    #[test]
    fn test_unsubscribe_requests_multiple_batched() {
        let subs = vec![
            ExchangeSub {
                channel: BitmexChannel(SmolStr::new("trade")),
                market: BitmexMarket(SmolStr::new("XBTUSD")),
            },
            ExchangeSub {
                channel: BitmexChannel(SmolStr::new("tradeBin1m")),
                market: BitmexMarket(SmolStr::new("ETHUSD")),
            },
        ];

        let messages = Bitmex::unsubscribe_requests(subs);
        // Bitmex batches all unsubscribes into a single message
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["op"], "unsubscribe");

        let args = payload["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], "trade:XBTUSD");
        assert_eq!(args[1], "tradeBin1m:ETHUSD");
    }
}
