use super::Bitfinex;
use crate::{
    Identifier,
    subscription::{Subscription, candle::Candles, trade::PublicTrades},
};
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Bitfinex`] channel to be subscribed to.
///
/// See docs: <https://docs.bitfinex.com/docs/ws-public>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BitfinexChannel(pub String);

impl BitfinexChannel {
    /// [`Bitfinex`] real-time trades channel.
    ///
    /// See docs: <https://docs.bitfinex.com/reference/ws-public-trades>
    pub fn trades() -> Self { Self("trades".into()) }

    /// [`Bitfinex`] real-time candles channel.
    ///
    /// See docs: <https://docs.bitfinex.com/reference/ws-public-candles>
    pub fn candles() -> Self { Self("candles".into()) }
}

impl<Instrument> Identifier<BitfinexChannel> for Subscription<Bitfinex, Instrument, PublicTrades> {
    fn id(&self) -> BitfinexChannel {
        BitfinexChannel::trades()
    }
}

impl<Instrument> Identifier<BitfinexChannel> for Subscription<Bitfinex, Instrument, Candles> {
    fn id(&self) -> BitfinexChannel {
        BitfinexChannel::candles()
    }
}

impl AsRef<str> for BitfinexChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use barter_instrument::instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind};

    fn candles_channel(interval: Interval) -> BitfinexChannel {
        let sub: Subscription<Bitfinex, MarketDataInstrument, Candles> = Subscription::new(
            Bitfinex,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_candles_channel_m1() {
        // Bitfinex always produces "candles" regardless of interval
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
    fn test_candles_channel_h6() {
        assert_eq!(candles_channel(Interval::H6).as_ref(), "candles");
    }

    #[test]
    fn test_candles_channel_w1() {
        assert_eq!(candles_channel(Interval::W1).as_ref(), "candles");
    }
}
