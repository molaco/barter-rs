use crate::{
    Identifier,
    instrument::InstrumentData,
    subscription::{Subscription, trade::PublicTrades},
};
use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use serde::Serialize;

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Gateio`](super::Gateio) channel to be subscribed to.
///
/// See docs: <https://www.okx.com/docs-v5/en/#websocket-api-public-channel>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct GateioChannel(pub String);

impl GateioChannel {
    /// Gateio [`MarketDataInstrumentKind::Spot`] real-time trades channel.
    ///
    /// See docs: <https://www.gate.io/docs/developers/apiv4/ws/en/#public-trades-channel>
    pub fn spot_trades() -> Self { Self("spot.trades".into()) }

    /// Gateio [`MarketDataInstrumentKind::Future`] & [`MarketDataInstrumentKind::Perpetual`] real-time trades channel.
    ///
    /// See docs: <https://www.gate.io/docs/developers/futures/ws/en/#trades-subscription>
    /// See docs: <https://www.gate.io/docs/developers/delivery/ws/en/#trades-subscription>
    pub fn future_trades() -> Self { Self("futures.trades".into()) }

    /// Gateio [`MarketDataInstrumentKind::Option`] real-time trades channel.
    ///
    /// See docs: <https://www.gate.io/docs/developers/options/ws/en/#public-contract-trades-channel>
    pub fn option_trades() -> Self { Self("options.trades".into()) }
}

impl<GateioExchange, Instrument> Identifier<GateioChannel>
    for Subscription<GateioExchange, Instrument, PublicTrades>
where
    Instrument: InstrumentData,
{
    fn id(&self) -> GateioChannel {
        match self.instrument.kind() {
            MarketDataInstrumentKind::Spot => GateioChannel::spot_trades(),
            MarketDataInstrumentKind::Future { .. } | MarketDataInstrumentKind::Perpetual => {
                GateioChannel::future_trades()
            }
            MarketDataInstrumentKind::Option { .. } => GateioChannel::option_trades(),
        }
    }
}

impl AsRef<str> for GateioChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}
