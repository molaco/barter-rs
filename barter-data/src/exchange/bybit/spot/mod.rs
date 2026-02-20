use super::{Bybit, ExchangeServer};
#[cfg(feature = "rest")]
use crate::exchange::RestExchangeServer;
use barter_instrument::exchange::ExchangeId;
use std::fmt::Display;

/// [`BybitSpot`] WebSocket server base url.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/ws/connect>
pub const WEBSOCKET_BASE_URL_BYBIT_SPOT: &str = "wss://stream.bybit.com/v5/public/spot";

/// [`Bybit`] spot exchange.
pub type BybitSpot = Bybit<BybitServerSpot>;

/// [`Bybit`] spot [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BybitServerSpot;

impl ExchangeServer for BybitServerSpot {
    const ID: ExchangeId = ExchangeId::BybitSpot;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BYBIT_SPOT
    }
}

#[cfg(feature = "rest")]
impl RestExchangeServer for BybitServerSpot {
    const ID: ExchangeId = ExchangeId::BybitSpot;

    fn rest_base_url() -> &'static str {
        "https://api.bybit.com"
    }

    fn klines_path() -> &'static str {
        "/v5/market/kline"
    }
}

#[cfg(feature = "rest")]
impl super::rest::BybitCategory for BybitServerSpot {
    fn category() -> &'static str {
        "spot"
    }
}

impl Display for BybitSpot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BybitSpot")
    }
}
