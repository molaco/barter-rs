use super::Coinbase;
use crate::{
    Identifier,
    subscription::{Subscription, candle::Candles, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Coinbase`] channel to be subscribed to.
///
/// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-overview#subscribe>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct CoinbaseChannel(pub String);

impl CoinbaseChannel {
    /// [`Coinbase`] real-time trades channel.
    ///
    /// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-channels#match>
    pub fn trades() -> Self { Self("matches".into()) }

    /// [`Coinbase`] real-time candles channel.
    ///
    /// See docs: <https://docs.cloud.coinbase.com/advanced-trade/docs/ws-channels#candles-channel>
    pub fn candles() -> Self { Self("candles".into()) }
}

impl<Instrument> Identifier<CoinbaseChannel> for Subscription<Coinbase, Instrument, PublicTrades> {
    fn id(&self) -> CoinbaseChannel {
        CoinbaseChannel::trades()
    }
}

impl<Instrument> Identifier<CoinbaseChannel> for Subscription<Coinbase, Instrument, Candles> {
    fn id(&self) -> CoinbaseChannel {
        CoinbaseChannel::candles()
    }
}

impl AsRef<str> for CoinbaseChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
