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

/// Type that defines how to translate a Barter [`Subscription`] into a [`Bybit`]
/// channel to be subscribed to.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/ws/connect>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct BybitChannel(pub String);

impl BybitChannel {
    /// [`Bybit`] real-time trades channel name.
    ///
    /// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/trade>
    pub fn trades() -> Self { Self("publicTrade".into()) }

    /// [`Bybit`] real-time OrderBook Level1 (top of books) channel name.
    ///
    /// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>
    pub fn order_book_l1() -> Self { Self("orderbook.1".into()) }

    /// [`Bybit`] OrderBook Level2 channel name (20ms delta updates).
    ///
    /// See docs: <https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook>
    pub fn order_book_l2() -> Self { Self("orderbook.50".into()) }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, PublicTrades>
{
    fn id(&self) -> BybitChannel {
        BybitChannel::trades()
    }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, OrderBooksL1>
{
    fn id(&self) -> BybitChannel {
        BybitChannel::order_book_l1()
    }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, OrderBooksL2>
{
    fn id(&self) -> BybitChannel {
        BybitChannel::order_book_l2()
    }
}

impl<Server, Instrument> Identifier<BybitChannel>
    for Subscription<Bybit<Server>, Instrument, Candles>
{
    fn id(&self) -> BybitChannel {
        BybitChannel(format!("kline.{}", bybit_interval(self.kind.0)))
    }
}

impl AsRef<str> for BybitChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
