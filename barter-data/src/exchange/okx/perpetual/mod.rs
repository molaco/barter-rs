use super::Okx;
use crate::exchange::ExchangeServer;
use barter_instrument::exchange::ExchangeId;
use std::fmt::Display;

/// [`OkxPerpetualsUsd`] WebSocket server base url.
///
/// See docs: <https://www.okx.com/docs-v5/en/#overview-api-resources-and-support>
pub const WEBSOCKET_BASE_URL_OKX_PERPETUALS_USD: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// [`Okx`] perpetuals exchange.
pub type OkxPerpetualsUsd = Okx<OkxServerPerpetualsUsd>;

/// [`Okx`] perpetuals [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct OkxServerPerpetualsUsd;

impl ExchangeServer for OkxServerPerpetualsUsd {
    const ID: ExchangeId = ExchangeId::OkxPerpetualsUsd;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_OKX_PERPETUALS_USD
    }
}

impl Display for OkxPerpetualsUsd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OkxPerpetualsUsd")
    }
}
