use self::liquidation::BinanceLiquidation;
use super::{Binance, ExchangeServer};
#[cfg(feature = "rest")]
use crate::exchange::RestExchangeServer;
use crate::{
    NoInitialSnapshots,
    exchange::{
        StreamSelector,
        binance::futures::l2::{
            BinanceFuturesUsdOrderBooksL2SnapshotFetcher, BinanceFuturesUsdOrderBooksL2Transformer,
        },
    },
    instrument::InstrumentData,
    subscription::{book::OrderBooksL2, liquidation::Liquidations},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::protocol::websocket::WebSocketSerdeParser;
use std::fmt::{Display, Formatter};

/// Level 2 OrderBook types.
pub mod l2;

/// Liquidation types.
pub mod liquidation;

/// [`BinanceFuturesUsd`] WebSocket server base url.
///
/// See docs: <https://binance-docs.github.io/apidocs/futures/en/#websocket-market-streams>
pub const WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD: &str = "wss://fstream.binance.com/ws";

/// [`Binance`] perpetual usd exchange.
pub type BinanceFuturesUsd = Binance<BinanceServerFuturesUsd>;

/// [`Binance`] perpetual usd [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct BinanceServerFuturesUsd;

impl ExchangeServer for BinanceServerFuturesUsd {
    const ID: ExchangeId = ExchangeId::BinanceFuturesUsd;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_BINANCE_FUTURES_USD
    }
}

#[cfg(feature = "rest")]
impl RestExchangeServer for BinanceServerFuturesUsd {
    const ID: ExchangeId = ExchangeId::BinanceFuturesUsd;

    fn rest_base_url() -> &'static str {
        "https://fapi.binance.com"
    }

    fn klines_path() -> &'static str {
        "/fapi/v1/klines"
    }
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = BinanceFuturesUsdOrderBooksL2SnapshotFetcher;
    type Transformer = BinanceFuturesUsdOrderBooksL2Transformer<Instrument::Key>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Liquidations> for BinanceFuturesUsd
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, Liquidations, BinanceLiquidation>;
    type Parser = WebSocketSerdeParser;
}

impl Display for BinanceFuturesUsd {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BinanceFuturesUsd")
    }
}
