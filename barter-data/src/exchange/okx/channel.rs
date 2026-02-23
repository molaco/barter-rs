use super::{Okx, okx_interval};
use crate::{
    Identifier,
    subscription::{Subscription, candle::Candles, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Okx`] channel to be subscribed to.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct OkxChannel(pub String);

impl OkxChannel {
    /// [`Okx`] real-time trades channel.
    ///
    /// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel>
    pub fn trades() -> Self { Self("trades".into()) }
}

impl<Instrument> Identifier<OkxChannel> for Subscription<Okx, Instrument, PublicTrades> {
    fn id(&self) -> OkxChannel {
        OkxChannel::trades()
    }
}

impl<Instrument> Identifier<OkxChannel> for Subscription<Okx, Instrument, Candles> {
    fn id(&self) -> OkxChannel {
        OkxChannel(format!("candle{}", okx_interval(self.kind.0)))
    }
}

impl AsRef<str> for OkxChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
