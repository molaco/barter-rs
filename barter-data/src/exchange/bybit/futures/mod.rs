use super::{Bybit, ExchangeServer};
#[cfg(feature = "rest")]
use crate::exchange::RestExchangeServer;
use barter_instrument::exchange::ExchangeId;
use std::fmt::Display;

/// [`BybitPerpetualsUsd`] WebSocket server base url.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/ws/connect>
pub const WEBSOCKET_BASE_URL_BYBIT_PERPETUALS_USD: &str = "wss://stream.bybit.com/v5/public/linear";

/// [`Bybit`] perpetual exchange.
pub type BybitPerpetualsUsd = Bybit<BybitServerPerpetualsUsd>;

/// [`Bybit`] perpetual [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BybitServerPerpetualsUsd;

impl ExchangeServer for BybitServerPerpetualsUsd {
    const ID: ExchangeId = ExchangeId::BybitPerpetualsUsd;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BYBIT_PERPETUALS_USD
    }
}

#[cfg(feature = "rest")]
impl RestExchangeServer for BybitServerPerpetualsUsd {
    const ID: ExchangeId = ExchangeId::BybitPerpetualsUsd;

    fn rest_base_url() -> &'static str {
        "https://api.bybit.com"
    }

    fn klines_path() -> &'static str {
        "/v5/market/kline"
    }
}

#[cfg(feature = "rest")]
impl super::rest::BybitCategory for BybitServerPerpetualsUsd {
    fn category() -> &'static str {
        "linear"
    }
}

impl Display for BybitPerpetualsUsd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BybitPerpetualsUsd")
    }
}
