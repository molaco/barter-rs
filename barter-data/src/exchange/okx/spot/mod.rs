use super::Okx;
use crate::exchange::ExchangeServer;
use barter_instrument::exchange::ExchangeId;
use std::fmt::Display;

/// [`OkxSpot`] WebSocket server base url.
///
/// See docs: <https://www.okx.com/docs-v5/en/#overview-api-resources-and-support>
pub const WEBSOCKET_BASE_URL_OKX_SPOT: &str = "wss://ws.okx.com:8443/ws/v5/public";

/// [`Okx`] spot exchange.
pub type OkxSpot = Okx<OkxServerSpot>;

/// [`Okx`] spot [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct OkxServerSpot;

impl ExchangeServer for OkxServerSpot {
    const ID: ExchangeId = ExchangeId::OkxSpot;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_OKX_SPOT
    }
}

impl Display for OkxSpot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OkxSpot")
    }
}
