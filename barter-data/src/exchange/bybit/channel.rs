use super::bybit_interval;
use crate::{
    Identifier,
    exchange::bybit::Bybit,
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        candle::Candles,
        trade::PublicTrades,
    },
};
use serde::Serialize;
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a [`Bybit`]
/// channel to be subscribed to.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/ws/connect>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BybitChannel(pub SmolStr);

impl BybitChannel {
    /// [`Bybit`] real-time trades channel name.
    ///
    /// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/trade>
    pub const TRADES: Self = Self(SmolStr::new_static("publicTrade"));

    /// [`Bybit`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>
    pub const ORDER_BOOK_L1: Self = Self(SmolStr::new_static("orderbook.1"));

    /// [`Bybit`] OrderBook Level2 channel name (20ms delta updates).
    ///
    /// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>
    pub const ORDER_BOOK_L2: Self = Self(SmolStr::new_static("orderbook.50"));
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> BybitChannel {
        BybitChannel::TRADES
    }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> BybitChannel {
        BybitChannel::ORDER_BOOK_L1
    }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, OrderBooksL2>
{
    fn id(&self) -> BybitChannel {
        BybitChannel::ORDER_BOOK_L2
    }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, Candles>
{
    fn id(&self) -> BybitChannel {
        BybitChannel(format_smolstr!("kline.{}", bybit_interval(self.kind.0)))
    }
}

impl AsRef<str> for BybitChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use crate::exchange::bybit::spot::BybitSpot;
    use barter_instrument::instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind};

    fn candles_channel(interval: Interval) -> BybitChannel {
        let sub: Subscription<BybitSpot, MarketDataInstrument, Candles> = Subscription::new(
            BybitSpot::default(),
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_candles_channel_m1() {
        assert_eq!(candles_channel(Interval::M1).as_ref(), "kline.1");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "kline.5");
    }

    #[test]
    fn test_candles_channel_m15() {
        assert_eq!(candles_channel(Interval::M15).as_ref(), "kline.15");
    }

    #[test]
    fn test_candles_channel_m30() {
        assert_eq!(candles_channel(Interval::M30).as_ref(), "kline.30");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "kline.60");
    }

    #[test]
    fn test_candles_channel_h4() {
        assert_eq!(candles_channel(Interval::H4).as_ref(), "kline.240");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "kline.D");
    }

    #[test]
    fn test_candles_channel_w1() {
        assert_eq!(candles_channel(Interval::W1).as_ref(), "kline.W");
    }

    #[test]
    fn test_candles_channel_month1() {
        assert_eq!(candles_channel(Interval::Month1).as_ref(), "kline.M");
    }
}
