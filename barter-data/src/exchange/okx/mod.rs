use self::{
    candle::OkxKline,
    channel::OkxChannel,
    market::{OkxMarket, okx_market},
    subscription::OkxSubResponse,
    trade::OkxTrades,
};
use crate::{
    NoInitialSnapshots,
    exchange::{Connector, ExchangeServer, ExchangeSub, PingInterval, StreamSelector},
    instrument::{InstrumentData, MarketInput},
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{SubKind, book::OrderBooksL2, candle::Candles, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocketSerdeParser, WsMessage},
};
use serde::de::{Error, Unexpected};
use serde_json::json;
use std::{fmt::Debug, marker::PhantomData, time::Duration};
use url::Url;

/// WebSocket candle/kline types for [`Okx`].
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`OkxPerpetualsUsd`](perpetual::OkxPerpetualsUsd).
pub mod perpetual;

/// [`ExchangeServer`] and [`StreamSelector`] implementations for
/// [`OkxSpot`](spot::OkxSpot).
pub mod spot;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`Okx`].
pub mod subscription;

/// REST client for fetching historical kline/candlestick data from OKX.
#[cfg(feature = "rest")]
pub mod rest;

/// Bulk archive download client for OKX.
#[cfg(feature = "bulk")]
pub mod bulk;

/// L2 OrderBook types for [`Okx`].
pub mod book;

/// Public trade types for [`Okx`].
pub mod trade;

use crate::subscription::candle::Interval;

/// Convert a normalised [`Interval`] to the OKX API interval string.
///
/// OKX uses uppercase letters for hours (`H`) and days (`D`), unlike most
/// other exchanges.
pub fn okx_interval(interval: Interval) -> &'static str {
    match interval {
        Interval::M1 => "1m",
        Interval::M3 => "3m",
        Interval::M5 => "5m",
        Interval::M15 => "15m",
        Interval::M30 => "30m",
        Interval::H1 => "1H",
        Interval::H2 => "2H",
        Interval::H4 => "4H",
        Interval::H6 => "6H",
        Interval::H12 => "12H",
        Interval::D1 => "1D",
        Interval::D3 => "3D",
        Interval::W1 => "1W",
        Interval::Month1 => "1M",
    }
}

/// [`Okx`] server [`PingInterval`] duration.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-connect>
pub const PING_INTERVAL_OKX: Duration = Duration::from_secs(29);

/// Generic [`Okx<Server>`](Okx) exchange.
///
/// ### Notes
/// A `Server` [`ExchangeServer`] implementation exists for
/// [`OkxSpot`](spot::OkxSpot) and [`OkxPerpetualsUsd`](perpetual::OkxPerpetualsUsd).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct Okx<Server> {
    server: PhantomData<Server>,
}

impl<Server> Connector for Okx<Server>
where
    Server: ExchangeServer,
{
    const ID: ExchangeId = Server::ID;
    type Channel = OkxChannel;
    type Market = OkxMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = OkxSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(Server::websocket_url()).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(PING_INTERVAL_OKX),
            ping: || WsMessage::text("ping"),
        })
    }

    fn requests(exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>]) -> Vec<WsMessage> {
        use crate::exchange::chunk_requests_by_frame_size;

        /// OKX WebSocket frame size limit in bytes.
        const OKX_MAX_FRAME_BYTES: usize = 4096;

        let chunks = chunk_requests_by_frame_size(exchange_subs.to_vec(), OKX_MAX_FRAME_BYTES);

        chunks
            .into_iter()
            .map(|chunk| {
                WsMessage::text(
                    json!({
                        "op": "subscribe",
                        "args": chunk,
                    })
                    .to_string(),
                )
            })
            .collect()
    }

    fn unsubscribe_requests(
        exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>],
    ) -> Vec<WsMessage> {
        vec![WsMessage::text(
            json!({
                "op": "unsubscribe",
                "args": exchange_subs,
            })
            .to_string(),
        )]
    }

    fn resolve_market(input: MarketInput<'_>, _sub_kind: &SubKind) -> Self::Market {
        match input {
            MarketInput::Components {
                base,
                quote,
                instrument_kind,
            } => okx_market(base, quote, instrument_kind),
            MarketInput::ExchangeName(name) => OkxMarket(name.name().clone()),
        }
    }

    fn send_rate_limit() -> Option<(u32, std::time::Duration)> {
        // OKX: 2 messages per second
        Some((2, std::time::Duration::from_secs(1)))
    }
}

impl<Instrument, Server> StreamSelector<Instrument, PublicTrades> for Okx<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, PublicTrades, OkxTrades>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, OrderBooksL2> for Okx<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = book::snapshot::OkxOrderBooksL2SnapshotFetcher;
    type Transformer = book::l2::OkxOrderBooksL2Transformer<Instrument::Key>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument, Server> StreamSelector<Instrument, Candles> for Okx<Server>
where
    Instrument: InstrumentData,
    Server: ExchangeServer + Debug + Send + Sync,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, Candles, OkxKline>;
    type Parser = WebSocketSerdeParser;
}

impl<'de, Server> serde::Deserialize<'de> for Okx<Server>
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

impl<Server> serde::Serialize for Okx<Server>
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
    fn test_okx_interval_mapping() {
        assert_eq!(okx_interval(Interval::M1), "1m");
        assert_eq!(okx_interval(Interval::M3), "3m");
        assert_eq!(okx_interval(Interval::M5), "5m");
        assert_eq!(okx_interval(Interval::M15), "15m");
        assert_eq!(okx_interval(Interval::M30), "30m");
        assert_eq!(okx_interval(Interval::H1), "1H");
        assert_eq!(okx_interval(Interval::H2), "2H");
        assert_eq!(okx_interval(Interval::H4), "4H");
        assert_eq!(okx_interval(Interval::H6), "6H");
        assert_eq!(okx_interval(Interval::H12), "12H");
        assert_eq!(okx_interval(Interval::D1), "1D");
        assert_eq!(okx_interval(Interval::D3), "3D");
        assert_eq!(okx_interval(Interval::W1), "1W");
        assert_eq!(okx_interval(Interval::Month1), "1M");
    }

    #[test]
    fn test_unsubscribe_requests() {
        use spot::OkxSpot;

        let exchange_subs = vec![
            ExchangeSub {
                channel: OkxChannel(SmolStr::new_static("trades")),
                market: OkxMarket(SmolStr::new_static("BTC-USDT")),
            },
            ExchangeSub {
                channel: OkxChannel(SmolStr::new_static("candle1m")),
                market: OkxMarket(SmolStr::new_static("ETH-USDT")),
            },
        ];

        let messages = OkxSpot::unsubscribe_requests(&exchange_subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();

        assert_eq!(payload["op"], "unsubscribe");

        let args = payload["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0]["channel"], "trades");
        assert_eq!(args[0]["instId"], "BTC-USDT");
        assert_eq!(args[1]["channel"], "candle1m");
        assert_eq!(args[1]["instId"], "ETH-USDT");
    }
}
