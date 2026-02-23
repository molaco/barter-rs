use super::{Hyperliquid, hyperliquid_interval};
use crate::{
    Identifier,
    subscription::{Subscription, candle::{Candles, Interval}, trade::PublicTrades},
};
use serde::Serialize;
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Hyperliquid`] channel to be subscribed to.
///
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct HyperliquidChannel(pub SmolStr);

impl HyperliquidChannel {
    /// [`Hyperliquid`] real-time trades channel.
    ///
    /// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
    pub const TRADES: Self = Self(SmolStr::new_static("trades"));

    /// [`Hyperliquid`] real-time candles channel.
    ///
    /// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
    pub fn candles(interval: Interval) -> Self {
        Self(format_smolstr!(
            "candle.{}",
            hyperliquid_interval(interval).expect("validated")
        ))
    }
}

impl<Instrument> Identifier<HyperliquidChannel>
    for Subscription<Hyperliquid, Instrument, PublicTrades>
{
    fn id(&self) -> HyperliquidChannel {
        HyperliquidChannel::TRADES
    }
}

impl<Instrument> Identifier<HyperliquidChannel> for Subscription<Hyperliquid, Instrument, Candles> {
    fn id(&self) -> HyperliquidChannel {
        HyperliquidChannel::candles(self.kind.0)
    }
}

impl AsRef<str> for HyperliquidChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::Interval;
    use barter_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    fn candles_channel(interval: Interval) -> HyperliquidChannel {
        let sub: Subscription<Hyperliquid, MarketDataInstrument, Candles> = Subscription::new(
            Hyperliquid,
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_trades_channel() {
        assert_eq!(HyperliquidChannel::TRADES.as_ref(), "trades");
    }

    #[test]
    fn test_candles_channel_m1() {
        assert_eq!(candles_channel(Interval::M1).as_ref(), "candle.1m");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "candle.5m");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "candle.1h");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "candle.1d");
    }

    #[test]
    fn test_candles_channel_m15() {
        assert_eq!(candles_channel(Interval::M15).as_ref(), "candle.15m");
    }

    #[test]
    fn test_candles_channel_m30() {
        assert_eq!(candles_channel(Interval::M30).as_ref(), "candle.30m");
    }

    #[test]
    fn test_candles_channel_h2() {
        assert_eq!(candles_channel(Interval::H2).as_ref(), "candle.2h");
    }

    #[test]
    fn test_candles_channel_h6() {
        assert_eq!(candles_channel(Interval::H6).as_ref(), "candle.6h");
    }
}
