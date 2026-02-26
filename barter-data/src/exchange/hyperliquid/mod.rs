use self::{
    candle::HyperliquidKline, channel::HyperliquidChannel,
    market::{HyperliquidMarket, hyperliquid_market},
    subscription::HyperliquidSubResponse, trade::HyperliquidTrades,
};
use crate::{
    NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::{InstrumentData, MarketInput},
    subscriber::{WebSocketSubscriber, validator::WebSocketSubValidator},
    subscription::{Map, SubKind, candle::Candles, trade::PublicTrades},
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

/// WebSocket candle/kline types for [`Hyperliquid`].
pub mod candle;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Defines the type that translates a Barter [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](barter_integration::Validator) for [`Hyperliquid`].
pub mod subscription;

/// REST client for fetching historical klines from [`Hyperliquid`].
#[cfg(feature = "rest")]
pub mod rest;

/// Public trade types for [`Hyperliquid`].
pub mod trade;

use crate::{error::DataError, subscription::candle::Interval};

/// Convert a normalised [`Interval`] to the Hyperliquid API interval string.
///
/// Hyperliquid supports all standard intervals.
pub fn hyperliquid_interval(interval: Interval) -> Result<&'static str, DataError> {
    match interval {
        Interval::M1 => Ok("1m"),
        Interval::M3 => Ok("3m"),
        Interval::M5 => Ok("5m"),
        Interval::M15 => Ok("15m"),
        Interval::M30 => Ok("30m"),
        Interval::H1 => Ok("1h"),
        Interval::H2 => Ok("2h"),
        Interval::H4 => Ok("4h"),
        Interval::H6 => Ok("6h"),
        Interval::H12 => Ok("12h"),
        Interval::D1 => Ok("1d"),
        Interval::D3 => Ok("3d"),
        Interval::W1 => Ok("1w"),
        Interval::Month1 => Ok("1M"),
    }
}

/// [`Hyperliquid`] server base url.
///
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
pub const BASE_URL_HYPERLIQUID: &str = "wss://api.hyperliquid.xyz/ws";

/// [`Hyperliquid`] server [`PingInterval`] duration.
pub const PING_INTERVAL_HYPERLIQUID: Duration = Duration::from_secs(50);

/// [`Hyperliquid`] exchange.
///
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
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
pub struct Hyperliquid;

impl Connector for Hyperliquid {
    const ID: ExchangeId = ExchangeId::Hyperliquid;
    type Channel = HyperliquidChannel;
    type Market = HyperliquidMarket;
    type Subscriber = WebSocketSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = HyperliquidSubResponse;

    fn url() -> Result<Url, SocketError> {
        Url::parse(BASE_URL_HYPERLIQUID).map_err(SocketError::UrlParse)
    }

    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(PING_INTERVAL_HYPERLIQUID),
            ping: || {
                WsMessage::text(
                    json!({
                        "method": "ping",
                    })
                    .to_string(),
                )
            },
        })
    }

    fn requests(exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>]) -> Vec<WsMessage> {
        exchange_subs
            .iter()
            .map(|sub| {
                let subscription =
                    serde_json::to_value(&sub).expect("failed to serialize ExchangeSub");

                let mut payload = serde_json::Map::new();
                payload.insert("method".to_string(), json!("subscribe"));
                payload.insert("subscription".to_string(), subscription);

                WsMessage::text(serde_json::Value::Object(payload).to_string())
            })
            .collect()
    }

    fn unsubscribe_requests(
        exchange_subs: &[ExchangeSub<Self::Channel, Self::Market>],
    ) -> Vec<WsMessage> {
        exchange_subs
            .iter()
            .map(|sub| {
                let subscription =
                    serde_json::to_value(&sub).expect("failed to serialize ExchangeSub");

                let mut payload = serde_json::Map::new();
                payload.insert("method".to_string(), json!("unsubscribe"));
                payload.insert("subscription".to_string(), subscription);

                WsMessage::text(serde_json::Value::Object(payload).to_string())
            })
            .collect()
    }

    fn expected_responses<InstrumentKey>(map: &Map<InstrumentKey>) -> usize {
        map.0.len()
    }

    fn resolve_market(input: MarketInput<'_>, _sub_kind: &SubKind) -> Self::Market {
        match input {
            MarketInput::Components {
                base,
                quote,
                instrument_kind,
            } => hyperliquid_market(base, quote, instrument_kind),
            MarketInput::ExchangeName(name) => HyperliquidMarket(name.name().clone()),
        }
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Hyperliquid
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, PublicTrades, HyperliquidTrades>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Candles> for Hyperliquid
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, Candles, HyperliquidKline>;
    type Parser = WebSocketSerdeParser;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;
    use smol_str::SmolStr;

    #[test]
    fn test_hyperliquid_interval_supported() {
        assert_eq!(hyperliquid_interval(Interval::M1).unwrap(), "1m");
        assert_eq!(hyperliquid_interval(Interval::M3).unwrap(), "3m");
        assert_eq!(hyperliquid_interval(Interval::M5).unwrap(), "5m");
        assert_eq!(hyperliquid_interval(Interval::M15).unwrap(), "15m");
        assert_eq!(hyperliquid_interval(Interval::M30).unwrap(), "30m");
        assert_eq!(hyperliquid_interval(Interval::H1).unwrap(), "1h");
        assert_eq!(hyperliquid_interval(Interval::H2).unwrap(), "2h");
        assert_eq!(hyperliquid_interval(Interval::H4).unwrap(), "4h");
        assert_eq!(hyperliquid_interval(Interval::H6).unwrap(), "6h");
        assert_eq!(hyperliquid_interval(Interval::H12).unwrap(), "12h");
        assert_eq!(hyperliquid_interval(Interval::D1).unwrap(), "1d");
        assert_eq!(hyperliquid_interval(Interval::D3).unwrap(), "3d");
        assert_eq!(hyperliquid_interval(Interval::W1).unwrap(), "1w");
        assert_eq!(hyperliquid_interval(Interval::Month1).unwrap(), "1M");
    }

    #[test]
    fn test_unsubscribe_requests_trades() {
        let exchange_subs = vec![ExchangeSub {
            channel: HyperliquidChannel(SmolStr::new_static("trades")),
            market: HyperliquidMarket(SmolStr::new_static("BTC")),
        }];

        let messages = Hyperliquid::unsubscribe_requests(&exchange_subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();

        assert_eq!(payload["method"], "unsubscribe");
        assert_eq!(payload["subscription"]["type"], "trades");
        assert_eq!(payload["subscription"]["coin"], "BTC");
    }

    #[test]
    fn test_unsubscribe_requests_candles() {
        use smol_str::format_smolstr;

        let exchange_subs = vec![ExchangeSub {
            channel: HyperliquidChannel(format_smolstr!("candle.1m")),
            market: HyperliquidMarket(SmolStr::new_static("ETH")),
        }];

        let messages = Hyperliquid::unsubscribe_requests(&exchange_subs);
        assert_eq!(messages.len(), 1);

        let payload: serde_json::Value = serde_json::from_str(&messages[0].to_string()).unwrap();

        assert_eq!(payload["method"], "unsubscribe");
        assert_eq!(payload["subscription"]["type"], "candle");
        assert_eq!(payload["subscription"]["coin"], "ETH");
        assert_eq!(payload["subscription"]["interval"], "1m");
    }

    #[test]
    fn test_unsubscribe_requests_multiple() {
        let exchange_subs = vec![
            ExchangeSub {
                channel: HyperliquidChannel(SmolStr::new_static("trades")),
                market: HyperliquidMarket(SmolStr::new_static("BTC")),
            },
            ExchangeSub {
                channel: HyperliquidChannel(SmolStr::new_static("trades")),
                market: HyperliquidMarket(SmolStr::new_static("ETH")),
            },
        ];

        let messages = Hyperliquid::unsubscribe_requests(&exchange_subs);
        // Hyperliquid sends one message per subscription
        assert_eq!(messages.len(), 2);

        for msg in &messages {
            let payload: serde_json::Value = serde_json::from_str(&msg.to_string()).unwrap();
            assert_eq!(payload["method"], "unsubscribe");
        }
    }
}
