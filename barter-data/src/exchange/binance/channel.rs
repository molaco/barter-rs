use super::{Binance, binance_interval, futures::BinanceFuturesUsd};
use crate::{
    Identifier,
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        candle::Candles,
        liquidation::Liquidations,
        trade::PublicTrades,
    },
};
use serde::Serialize;
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a [`Binance`]
/// channel to be subscribed to.
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BinanceChannel(pub SmolStr);

impl BinanceChannel {
    /// [`Binance`] real-time trades channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#trade-streams>
    ///
    /// Note:
    /// For [`BinanceFuturesUsd`] this real-time
    /// stream is undocumented.
    ///
    /// See discord: <https://discord.com/channels/910237311332151317/923160222711812126/975712874582388757>
    pub const TRADES: Self = Self(SmolStr::new_static("@trade"));

    /// [`Binance`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs:<https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams>
    /// See docs:<https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams>
    pub const ORDER_BOOK_L1: Self = Self(SmolStr::new_static("@bookTicker"));

    /// [`Binance`] OrderBook Level2 channel name (100ms delta updates).
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams>
    pub const ORDER_BOOK_L2: Self = Self(SmolStr::new_static("@depth@100ms"));

    /// [`BinanceFuturesUsd`] liquidation orders channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams>
    pub const LIQUIDATIONS: Self = Self(SmolStr::new_static("@forceOrder"));
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::TRADES
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ORDER_BOOK_L1
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL2>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::ORDER_BOOK_L2
    }
}

impl<Instrument> Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsd, Instrument, Liquidations>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::LIQUIDATIONS
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, Candles>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel(format_smolstr!("@kline_{}", binance_interval(self.kind.0)))
    }
}

impl AsRef<str> for BinanceChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use crate::exchange::binance::spot::BinanceSpot;
    use barter_instrument::instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind};

    fn candles_channel(interval: Interval) -> BinanceChannel {
        let sub: Subscription<BinanceSpot, MarketDataInstrument, Candles> = Subscription::new(
            BinanceSpot::default(),
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_candles_channel_m1() {
        assert_eq!(candles_channel(Interval::M1).as_ref(), "@kline_1m");
    }

    #[test]
    fn test_candles_channel_m3() {
        assert_eq!(candles_channel(Interval::M3).as_ref(), "@kline_3m");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "@kline_5m");
    }

    #[test]
    fn test_candles_channel_m15() {
        assert_eq!(candles_channel(Interval::M15).as_ref(), "@kline_15m");
    }

    #[test]
    fn test_candles_channel_m30() {
        assert_eq!(candles_channel(Interval::M30).as_ref(), "@kline_30m");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "@kline_1h");
    }

    #[test]
    fn test_candles_channel_h4() {
        assert_eq!(candles_channel(Interval::H4).as_ref(), "@kline_4h");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "@kline_1d");
    }

    #[test]
    fn test_candles_channel_w1() {
        assert_eq!(candles_channel(Interval::W1).as_ref(), "@kline_1w");
    }

    #[test]
    fn test_candles_channel_month1() {
        assert_eq!(candles_channel(Interval::Month1).as_ref(), "@kline_1M");
    }
}
