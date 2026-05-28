use super::{Okx, okx_interval};
use crate::{
    Identifier,
    exchange::ExchangeServer,
    subscription::{Subscription, book::OrderBooksL2, candle::Candles, trade::PublicTrades},
};
use serde::Serialize;
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Okx`] channel to be subscribed to.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct OkxChannel(pub(crate) SmolStr);

impl OkxChannel {
    /// [`Okx`] real-time trades channel.
    ///
    /// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel-trades-channel>
    pub const TRADES: Self = Self(SmolStr::new_static("trades"));

    /// [`Okx`] L2 orderbook channel (50-level snapshot + diffs).
    ///
    /// Uses `books` channel which sends an initial snapshot then incremental
    /// updates with `seqId`/`prevSeqId` sequencing and CRC32 checksum.
    ///
    /// See docs: <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel>
    pub const ORDER_BOOK_L2: Self = Self(SmolStr::new_static("books"));
}

impl<Instrument, Server> Identifier<OkxChannel>
    for Subscription<Okx<Server>, Instrument, PublicTrades>
where
    Server: ExchangeServer,
{
    fn id(&self) -> OkxChannel {
        OkxChannel::TRADES
    }
}

impl<Instrument, Server> Identifier<OkxChannel>
    for Subscription<Okx<Server>, Instrument, OrderBooksL2>
where
    Server: ExchangeServer,
{
    fn id(&self) -> OkxChannel {
        OkxChannel::ORDER_BOOK_L2
    }
}

impl<Instrument, Server> Identifier<OkxChannel> for Subscription<Okx<Server>, Instrument, Candles>
where
    Server: ExchangeServer,
{
    fn id(&self) -> OkxChannel {
        OkxChannel(format_smolstr!("candle{}", okx_interval(self.kind.0)))
    }
}

impl AsRef<str> for OkxChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exchange::okx::spot::OkxSpot,
        subscription::candle::{Candles, Interval},
    };
    use barter_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    fn candles_channel(interval: Interval) -> OkxChannel {
        let sub: Subscription<OkxSpot, MarketDataInstrument, Candles> = Subscription::new(
            OkxSpot::default(),
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_candles_channel_m1() {
        assert_eq!(candles_channel(Interval::M1).as_ref(), "candle1m");
    }

    #[test]
    fn test_candles_channel_m5() {
        assert_eq!(candles_channel(Interval::M5).as_ref(), "candle5m");
    }

    #[test]
    fn test_candles_channel_m15() {
        assert_eq!(candles_channel(Interval::M15).as_ref(), "candle15m");
    }

    #[test]
    fn test_candles_channel_h1() {
        assert_eq!(candles_channel(Interval::H1).as_ref(), "candle1H");
    }

    #[test]
    fn test_candles_channel_h4() {
        assert_eq!(candles_channel(Interval::H4).as_ref(), "candle4H");
    }

    #[test]
    fn test_candles_channel_d1() {
        assert_eq!(candles_channel(Interval::D1).as_ref(), "candle1D");
    }

    #[test]
    fn test_candles_channel_w1() {
        assert_eq!(candles_channel(Interval::W1).as_ref(), "candle1W");
    }

    #[test]
    fn test_candles_channel_month1() {
        assert_eq!(candles_channel(Interval::Month1).as_ref(), "candle1M");
    }

    #[test]
    fn test_l2_book_channel() {
        assert_eq!(OkxChannel::ORDER_BOOK_L2.as_ref(), "books");
    }

    #[test]
    fn test_l2_book_subscription_identifier() {
        let sub: Subscription<OkxSpot, MarketDataInstrument, OrderBooksL2> = Subscription::new(
            OkxSpot::default(),
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            OrderBooksL2,
        );
        assert_eq!(sub.id(), OkxChannel::ORDER_BOOK_L2);
    }
}
