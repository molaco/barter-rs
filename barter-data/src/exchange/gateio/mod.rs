use self::{channel::GateioChannel, market::GateioMarket, subscription::GateioSubResponse};
use crate::{
    exchange::{Connector, ExchangeServer, subscription::ExchangeSub},
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{error::SocketError, protocol::websocket::WsMessage};
use serde_json::json;
use std::{fmt::Debug, marker::PhantomData};
use url::Url;

/// Public candle/kline types for [`Gateio`].
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// [`ExchangeServer`] and [`StreamSelector`](super::StreamSelector) implementations for
/// [`GateioSpot`](spot::GateioSpot).
pub mod spot;

/// [`ExchangeServer`] and [`StreamSelector`](super::StreamSelector) implementations for
/// [`GateioFutureUsd`](future::GateioFuturesUsd) and
/// [`GateioFutureBtc`](future::GateioFuturesBtc).
pub mod future;

/// [`ExchangeServer`] and [`StreamSelector`](super::StreamSelector) implementations for
/// [`GateioPerpetualUsdt`](perpetual::GateioPerpetualsUsd) and
/// [`GateioPerpetualBtc`](perpetual::GateioPerpetualsBtc).
pub mod perpetual;

/// [`ExchangeServer`] and [`StreamSelector`](super::StreamSelector) implementations for
/// [`GateioOptions`](option::GateioOptions)
pub mod option;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// Generic [`GateioMessage<T>`](message::GateioMessage) type common to
/// [`GateioSpot`](spot::GateioSpot), [`GateioPerpetualUsdt`](perpetual::GateioPerpetualsUsd)
/// and [`GateioPerpetualBtc`](perpetual::GateioPerpetualsBtc).
pub mod message;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration) common to [`GateioSpot`](spot::GateioSpot),
/// [`GateioPerpetualUsdt`](perpetual::GateioPerpetualsUsd) and
/// [`GateioPerpetualBtc`](perpetual::GateioPerpetualsBtc).
pub mod subscription;

use crate::{error::DataError, subscription::candle::Interval};

/// Convert a normalised [`Interval`] to the GateIO API interval string.
///
/// GateIO supports: 10s, 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 7d, 30d.
pub fn gateio_interval(interval: Interval) -> Result<&'static str, DataError> {
    match interval {
        Interval::M1 => Ok("1m"),
        Interval::M5 => Ok("5m"),
        Interval::M15 => Ok("15m"),
        Interval::M30 => Ok("30m"),
        Interval::H1 => Ok("1h"),
        Interval::H4 => Ok("4h"),
        Interval::D1 => Ok("1d"),
        Interval::W1 => Ok("7d"),
        Interval::Month1 => Ok("30d"),
        unsupported => Err(DataError::Socket(format!(
            "GateIO does not support interval: {unsupported}"
        ))),
    }
}

/// Generic [`Gateio<Server>`](Gateio) exchange.
///
/// ### Notes
/// A `Server` [`ExchangeServer`] implementations exists for
/// [`GateioSpot`](spot::GateioSpot), [`GateioPerpetualUsdt`](perpetual::GateioPerpetualsUsd) and
/// [`GateioPerpetualBtc`](perpetual::GateioPerpetualsBtc).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Gateio<Server> {
    server: PhantomData<Server>,
}

impl<Server> Connector for Gateio<Server>
where
    Server: ExchangeServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = GateioChannel;
    type Market = GateioMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = GateioSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(Server::websocket_url()).map_err(SocketError::UrlParse)
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                WsMessage::text(
                    json!({
                        "time": chrono::Utc::now().timestamp_millis(),
                        "channel": channel.as_ref(),
                        "event": "subscribe",
                        "payload": [market.as_ref()]
                    })
                    .to_string(),
                )
            })
            .collect()
    }

    fn unsubscribe_requests(
        exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>,
    ) -> Vec<WsMessage> {
        exchange_subs
            .into_iter()
            .map(|ExchangeSub { channel, market }| {
                WsMessage::text(
                    json!({
                        "time": chrono::Utc::now().timestamp_millis(),
                        "channel": channel.as_ref(),
                        "event": "unsubscribe",
                        "payload": [market.as_ref()]
                    })
                    .to_string(),
                )
            })
            .collect()
    }
}

impl<'de, Server> serde::Deserialize<'de> for Gateio<Server>
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

impl<Server> serde::Serialize for Gateio<Server>
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
    use crate::{exchange::subscription::ExchangeSub, subscription::candle::Interval};
    use smol_str::SmolStr;

    #[test]
    fn test_gateio_interval_supported() {
        assert_eq!(gateio_interval(Interval::M1).unwrap(), "1m");
        assert_eq!(gateio_interval(Interval::M5).unwrap(), "5m");
        assert_eq!(gateio_interval(Interval::M15).unwrap(), "15m");
        assert_eq!(gateio_interval(Interval::M30).unwrap(), "30m");
        assert_eq!(gateio_interval(Interval::H1).unwrap(), "1h");
        assert_eq!(gateio_interval(Interval::H4).unwrap(), "4h");
        assert_eq!(gateio_interval(Interval::D1).unwrap(), "1d");
        assert_eq!(gateio_interval(Interval::W1).unwrap(), "7d");
        assert_eq!(gateio_interval(Interval::Month1).unwrap(), "30d");
    }

    #[test]
    fn test_gateio_interval_unsupported() {
        assert!(gateio_interval(Interval::M3).is_err());
        assert!(gateio_interval(Interval::H2).is_err());
        assert!(gateio_interval(Interval::H6).is_err());
        assert!(gateio_interval(Interval::H12).is_err());
        assert!(gateio_interval(Interval::D3).is_err());
    }

    #[test]
    fn test_unsubscribe_requests_spot_trades() {
        use spot::GateioSpot;

        let subs = vec![ExchangeSub {
            channel: GateioChannel(SmolStr::new("spot.trades")),
            market: GateioMarket(SmolStr::new("BTC_USDT")),
        }];

        let messages = GateioSpot::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["event"], "unsubscribe");
        assert_eq!(payload["channel"], "spot.trades");
        assert_eq!(payload["payload"][0], "BTC_USDT");
        assert!(payload["time"].is_number());
    }

    #[test]
    fn test_unsubscribe_requests_perpetual_trades() {
        use perpetual::GateioPerpetualsUsd;

        let subs = vec![ExchangeSub {
            channel: GateioChannel(SmolStr::new("futures.trades")),
            market: GateioMarket(SmolStr::new("BTC_USDT")),
        }];

        let messages = GateioPerpetualsUsd::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["event"], "unsubscribe");
        assert_eq!(payload["channel"], "futures.trades");
        assert_eq!(payload["payload"][0], "BTC_USDT");
        assert!(payload["time"].is_number());
    }

    #[test]
    fn test_unsubscribe_requests_futures_usd() {
        use future::GateioFuturesUsd;

        let subs = vec![ExchangeSub {
            channel: GateioChannel(SmolStr::new("futures.trades")),
            market: GateioMarket(SmolStr::new("BTC_USDT")),
        }];

        let messages = GateioFuturesUsd::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["event"], "unsubscribe");
        assert_eq!(payload["channel"], "futures.trades");
        assert_eq!(payload["payload"][0], "BTC_USDT");
    }

    #[test]
    fn test_unsubscribe_requests_options() {
        use option::GateioOptions;

        let subs = vec![ExchangeSub {
            channel: GateioChannel(SmolStr::new("options.trades")),
            market: GateioMarket(SmolStr::new("BTC_USDT")),
        }];

        let messages = GateioOptions::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload["event"], "unsubscribe");
        assert_eq!(payload["channel"], "options.trades");
        assert_eq!(payload["payload"][0], "BTC_USDT");
    }

    #[test]
    fn test_unsubscribe_requests_multiple() {
        use spot::GateioSpot;

        let subs = vec![
            ExchangeSub {
                channel: GateioChannel(SmolStr::new("spot.trades")),
                market: GateioMarket(SmolStr::new("BTC_USDT")),
            },
            ExchangeSub {
                channel: GateioChannel(SmolStr::new("spot.candlesticks_1m")),
                market: GateioMarket(SmolStr::new("ETH_USDT")),
            },
        ];

        let messages = GateioSpot::unsubscribe_requests(subs);
        assert_eq!(messages.len(), 2);

        let payload0: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();
        assert_eq!(payload0["event"], "unsubscribe");
        assert_eq!(payload0["channel"], "spot.trades");
        assert_eq!(payload0["payload"][0], "BTC_USDT");

        let payload1: serde_json::Value = serde_json::from_str(&messages[1].to_string()).unwrap();
        assert_eq!(payload1["event"], "unsubscribe");
        assert_eq!(payload1["channel"], "spot.candlesticks_1m");
        assert_eq!(payload1["payload"][0], "ETH_USDT");
    }
}
