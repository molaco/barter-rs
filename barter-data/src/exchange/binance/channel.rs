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

/// Type that defines how to translate a Barter [`Subscription`] into a [`Binance`]
/// channel to be subscribed to.
///
/// See docs: <https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams>
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BinanceChannel(pub String);

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
    pub fn trades() -> Self { Self("@trade".into()) }

    /// [`Binance`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs:<https://binance-docs.github.io/apidocs/spot/en/#individual-symbol-book-ticker-streams>
    /// See docs:<https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams>
    pub fn order_book_l1() -> Self { Self("@bookTicker".into()) }

    /// [`Binance`] OrderBook Level2 channel name (100ms delta updates).
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream>
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams>
    pub fn order_book_l2() -> Self { Self("@depth@100ms".into()) }

    /// [`BinanceFuturesUsd`] liquidation orders channel name.
    ///
    /// See docs: <https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams>
    pub fn liquidations() -> Self { Self("@forceOrder".into()) }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::trades()
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::order_book_l1()
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, OrderBooksL2>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::order_book_l2()
    }
}

impl<Instrument> Identifier<BinanceChannel>
    for Subscription<BinanceFuturesUsd, Instrument, Liquidations>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel::liquidations()
    }
}

impl<Server, Instrument> Identifier<BinanceChannel>
    for Subscription<Binance<Server>, Instrument, Candles>
{
    fn id(&self) -> BinanceChannel {
        BinanceChannel(format!("@kline_{}", binance_interval(self.kind.0)))
    }
}

impl AsRef<str> for BinanceChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
