use self::trade::GateioSpotTrade;
use super::{Gateio, candle::GateioKline};
use crate::{
    NoInitialSnapshots,
    exchange::{ExchangeServer, StreamSelector},
    instrument::InstrumentData,
    subscription::{candle::Candles, trade::PublicTrades},
    transformer::stateless::StatelessTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::protocol::websocket::WebSocketSerdeParser;
use barter_macro::{DeExchange, SerExchange};
use std::fmt::Display;

/// Public trades types.
pub mod trade;

/// [`GateioSpot`] WebSocket server base url.
///
/// See docs: <https://www.gate.io/docs/developers/apiv4/ws/en/>
pub const WEBSOCKET_BASE_URL_GATEIO_SPOT: &str = "wss://api.gateio.ws/ws/v4/";

/// [`Gateio`] spot exchange.
pub type GateioSpot = Gateio<GateioServerSpot>;

/// [`Gateio`] spot [`ExchangeServer`].
#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Default, DeExchange, SerExchange,
)]
pub struct GateioServerSpot;

impl ExchangeServer for GateioServerSpot {
    const ID: ExchangeId = ExchangeId::GateioSpot;

    fn websocket_url() -> &'static str {
        WEBSOCKET_BASE_URL_GATEIO_SPOT
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for GateioSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, PublicTrades, GateioSpotTrade>;
    type Parser = WebSocketSerdeParser;
}

impl<Instrument> StreamSelector<Instrument, Candles> for GateioSpot
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Transformer =
        StatelessTransformer<Self, Instrument::Key, Candles, GateioKline>;
    type Parser = WebSocketSerdeParser;
}

impl Display for GateioSpot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GateioSpot")
    }
}
