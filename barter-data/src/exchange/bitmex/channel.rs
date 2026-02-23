use crate::{
    Identifier,
    exchange::bitmex::Bitmex,
    subscription::{Subscription, candle::Candles, trade::PublicTrades},
};
use super::bitmex_interval;
use serde::Serialize;
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a [`Bitmex`]
/// channel to be subscribed to.
///
/// See docs: <https://www.bitmex.com/app/wsAPI>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BitmexChannel(pub SmolStr);

impl BitmexChannel {
    /// [`Bitmex`] real-time trades channel name.
    ///
    /// See docs: <https://www.bitmex.com/app/wsAPI>
    pub const TRADES: Self = Self(SmolStr::new_static("trade"));
}

impl<Instrument> Identifier<BitmexChannel> for Subscription<Bitmex, Instrument, PublicTrades> {
    fn id(&self) -> BitmexChannel {
        BitmexChannel::TRADES
    }
}

impl<Instrument> Identifier<BitmexChannel> for Subscription<Bitmex, Instrument, Candles> {
    fn id(&self) -> BitmexChannel {
        BitmexChannel(format_smolstr!("tradeBin{}", bitmex_interval(self.kind.0).expect("validated")))
    }
}

impl AsRef<str> for BitmexChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use barter_instrument::instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind};

    fn candles_channel(interval: Interval) -> BitmexChannel {
        let sub: Subscription<Bitmex, MarketDataInstrument, Candles> = Subscription::new(
            Bitmex,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        Identifier::<BitmexChannel>::id(&sub)
    }

    #[test]
    fn test_candles_channel_m1() {
        assert_eq!(candles_channel(Interval::M1).as_ref(), "tradeBin1m");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "tradeBin5m");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "tradeBin1h");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "tradeBin1d");
    }
}
