use super::Coinbase;
use crate::{
    Identifier,
    subscription::{Subscription, book::OrderBooksL2, candle::Candles, trade::PublicTrades},
};
use serde::Serialize;
use smol_str::SmolStr;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Coinbase`] channel to be subscribed to.
///
/// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-overview#subscribe>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct CoinbaseChannel(pub(crate) SmolStr);

impl CoinbaseChannel {
    /// [`Coinbase`] real-time trades channel.
    ///
    /// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-channels#match>
    pub const TRADES: Self = Self(SmolStr::new_static("matches"));

    /// [`Coinbase`] real-time candles channel.
    ///
    /// See docs: <https://docs.cloud.coinbase.com/advanced-trade/docs/ws-channels#candles-channel>
    pub const CANDLES: Self = Self(SmolStr::new_static("candles"));

    /// [`Coinbase`] L2 orderbook channel.
    ///
    /// See docs: <https://docs.cloud.coinbase.com/exchange/docs/websocket-channels#level2-channel>
    pub const ORDER_BOOK_L2: Self = Self(SmolStr::new_static("level2"));
}

impl<Instrument> Identifier<CoinbaseChannel> for Subscription<Coinbase, Instrument, PublicTrades> {
    fn id(&self) -> CoinbaseChannel {
        CoinbaseChannel::TRADES
    }
}

impl<Instrument> Identifier<CoinbaseChannel> for Subscription<Coinbase, Instrument, Candles> {
    fn id(&self) -> CoinbaseChannel {
        CoinbaseChannel::CANDLES
    }
}

impl<Instrument> Identifier<CoinbaseChannel> for Subscription<Coinbase, Instrument, OrderBooksL2> {
    fn id(&self) -> CoinbaseChannel {
        CoinbaseChannel::ORDER_BOOK_L2
    }
}

impl AsRef<str> for CoinbaseChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use barter_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    fn candles_channel(interval: Interval) -> CoinbaseChannel {
        let sub: Subscription<Coinbase, MarketDataInstrument, Candles> = Subscription::new(
            Coinbase,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_candles_channel_m1() {
        // Coinbase always produces "candles" regardless of interval
        assert_eq!(candles_channel(Interval::M1).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_m15() {
        assert_eq!(candles_channel(Interval::M15).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_m30() {
        assert_eq!(candles_channel(Interval::M30).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_h2() {
        assert_eq!(candles_channel(Interval::H2).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_h6() {
        assert_eq!(candles_channel(Interval::H6).as_ref(), "candles");
    }
}
