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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use barter_instrument::instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind};

    fn candles_channel(interval: Interval) -> KrakenChannel {
        let sub: Subscription<Kraken, MarketDataInstrument, Candles> = Subscription::new(
            Kraken,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        Identifier::<KrakenChannel>::id(&sub)
    }

    #[test]
    fn test_candles_channel_m1() {
        assert_eq!(candles_channel(Interval::M1).as_ref(), "ohlc-1");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "ohlc-5");
    }

    #[test]
    fn test_candles_channel_m15() {
        assert_eq!(candles_channel(Interval::M15).as_ref(), "ohlc-15");
    }

    #[test]
    fn test_candles_channel_m30() {
        assert_eq!(candles_channel(Interval::M30).as_ref(), "ohlc-30");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "ohlc-60");
    }

    #[test]
    fn test_candles_channel_h4() {
        assert_eq!(candles_channel(Interval::H4).as_ref(), "ohlc-240");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "ohlc-1440");
    }

    #[test]
    fn test_candles_channel_w1() {
        assert_eq!(candles_channel(Interval::W1).as_ref(), "ohlc-10080");
    }

    #[test]
    fn test_candles_channel_unsupported_falls_back_to_1() {
        // Kraken uses unwrap_or(1) for unsupported intervals
        assert_eq!(candles_channel(Interval::M3).as_ref(), "ohlc-1");
        assert_eq!(candles_channel(Interval::H2).as_ref(), "ohlc-1");
        assert_eq!(candles_channel(Interval::H6).as_ref(), "ohlc-1");
        assert_eq!(candles_channel(Interval::H12).as_ref(), "ohlc-1");
        assert_eq!(candles_channel(Interval::D3).as_ref(), "ohlc-1");
        assert_eq!(candles_channel(Interval::Month1).as_ref(), "ohlc-1");
    }
}
