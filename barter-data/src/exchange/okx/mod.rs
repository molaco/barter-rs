use self::{
    candle::OkxKline, channel::OkxChannel, market::OkxMarket, subscription::OkxSubResponse,
    trade::OkxTrades,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
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
use std::time::Duration;
use url::Url;

/// WebSocket candle/kline types for [`Okx`].
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`Okx`].
pub mod subscription;

/// REST client for fetching historical kline/candlestick data from OKX.
#[cfg(feature = "rest")]
pub mod rest;

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

/// [`Okx`] server base url.
///
/// See docs: <https://www.okx.com/docs-v5/en/#overview-api-resources-and-support>
pub const BASE_URL_OKX: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// [`Okx`] server [`PingInterval`] duration.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-connect>
pub const PING_INTERVAL_OKX: Duration = Duration::from_secs(29);

/// Convenient type alias for an Okx [`ExchangeWsStream`] using [`WebSocketSerdeParser`](barter_integration::protocol::websocket::WebSocketSerdeParser).
pub type OkxWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Okx`] exchange.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api>
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
pub struct Okx;

impl Connector for Okx {
    const ID: ExchangeId = ExchangeId::Okx;
    type Channel = OkxChannel;
    type Market = OkxMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = OkxSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_OKX).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(PING_INTERVAL_OKX),
            ping: || WsMessage::text("ping"),
        })
    }

    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        vec![WsMessage::text(
            json!({
                "op": "subscribe",
                "args": &exchange_subs,
            })
            .to_string(),
        )]
    }

    fn unsubscribe_requests(
        exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>,
    ) -> Vec<WsMessage> {
        vec![WsMessage::text(
            json!({
                "op": "unsubscribe",
                "args": &exchange_subs,
            })
            .to_string(),
        )]
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Okx
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = OkxWsStream<StatelessTransformer<Self, Instrument::Key, PublicTrades, OkxTrades>>;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Okx
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = OkxWsStream<StatelessTransformer<Self, Instrument::Key, Candles, OkxKline>>;
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

        let messages = Okx::unsubscribe_requests(exchange_subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(&messages[0].to_string()).unwrap();

        assert_eq!(payload["op"], "unsubscribe");

        let args = payload["args"].as_array().unwrap();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0]["channel"], "trades");
        assert_eq!(args[0]["instId"], "BTC-USDT");
        assert_eq!(args[1]["channel"], "candle1m");
        assert_eq!(args[1]["instId"], "ETH-USDT");
    }
}
