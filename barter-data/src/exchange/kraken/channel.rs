use super::{Kraken, kraken_interval};
use crate::{
    Identifier,
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        candle::Candles,
        trade::PublicTrades,
    },
};
use serde::Serialize;
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kraken`] v2 channel to be subscribed to.
///
/// See docs: <https://docs.kraken.com/api/docs/websocket-v2/>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct KrakenChannel(pub(crate) SmolStr);

impl KrakenChannel {
    /// [`Kraken`] v2 real-time trades channel.
    ///
    /// See docs: <https://docs.kraken.com/api/docs/websocket-v2/trade/>
    pub const TRADES: Self = Self(SmolStr::new_static("trade"));

    /// [`Kraken`] v2 ticker channel (best bid/ask, replaces v1 "spread").
    ///
    /// See docs: <https://docs.kraken.com/api/docs/websocket-v2/ticker/>
    pub const TICKER: Self = Self(SmolStr::new_static("ticker"));

    /// [`Kraken`] v2 L2 orderbook channel.
    ///
    /// See docs: <https://docs.kraken.com/api/docs/websocket-v2/book/>
    pub const ORDER_BOOK_L2: Self = Self(SmolStr::new_static("book"));
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, PublicTrades> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::TRADES
    }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, OrderBooksL1> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::TICKER
    }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, OrderBooksL2> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel::ORDER_BOOK_L2
    }
}

impl<Instrument> Identifier<KrakenChannel> for Subscription<Kraken, Instrument, Candles> {
    fn id(&self) -> KrakenChannel {
        KrakenChannel(format_smolstr!(
            "ohlc-{}",
            kraken_interval(self.kind.0)
                .expect("caller must validate interval is supported by Kraken")
        ))
    }
}

impl AsRef<str> for KrakenChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trades_channel() {
        assert_eq!(KrakenChannel::TRADES.as_ref(), "trade");
    }

    #[test]
    fn test_ticker_channel() {
        assert_eq!(KrakenChannel::TICKER.as_ref(), "ticker");
    }

    #[test]
    fn test_book_channel() {
        assert_eq!(KrakenChannel::ORDER_BOOK_L2.as_ref(), "book");
    }
}
