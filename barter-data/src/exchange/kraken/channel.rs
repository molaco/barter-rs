use super::{Kraken, kraken_interval};
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL1, candle::Candles, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kraken`] channel to be subscribed to.
///
/// See docs: <https://docs.kraken.com/websockets/#message-subscribe>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct KrakenChannel(pub String);

impl KrakenChannel {
    /// [`Kraken`] real-time trades channel name.
    ///
    /// See docs: <https://docs.kraken.com/websockets/#message-subscribe>
    pub fn trades() -> Self { Self("trade".into()) }

    /// [`Kraken`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs: <https://docs.kraken.com/websockets/#message-subscribe>
    pub fn order_book_l1() -> Self { Self("spread".into()) }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, PublicTrades> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::trades()
    }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, OrderBooksL1> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::order_book_l1()
    }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, Candles> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel(format!(
            "ohlc-{}",
            kraken_interval(self.kind.0).unwrap_or(1)
        ))
    }
}

impl AsRef<str> for KrakenChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
