use crate::{
    NoInitialSnapshots,
    exchange::{
        ExchangeServer, StreamSelector,
        gateio::{Gateio, candle::GateioKline, perpetual::trade::GateioFuturesTrades},
    },
    instrument::InstrumentData,
    subscription::{candle::Candles, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::protocol::websocket::WebSocketSerdeParser;
use std::fmt::Display;

/// [`GateioOptions`] WebSocket server base url.
///
/// See docs: <https://www.gate.io/docs/developers/futures/ws/en/>
pub const WEBSOCKET_BASE_URL_GATEIO_OPTIONS_USD: &str = "wss://op-ws.gateio.live/v4/ws";

/// [`Gateio`] options exchange.
pub type GateioOptions = Gateio<GateioServerOptions>;

/// [`Gateio`] options [`ExchangeServer`].
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default)]
pub struct GateioServerOptions;

impl ExchangeServer for GateioServerOptions {
    const ID: ExchangeId = ExchangeId::GateioOptions;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_GATEIO_OPTIONS_USD
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for GateioOptions
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, PublicTrades, GateioFuturesTrades>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Candles> for GateioOptions
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer = StatelessTransformer<Self, Instrument::Key, Candles, GateioKline>;
    type Parser = WebSocketSerdeParser;
}

impl Display for GateioOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GateioOptions")
    }
}
