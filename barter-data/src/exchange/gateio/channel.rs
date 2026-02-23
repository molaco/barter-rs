use super::gateio_interval;
use crate::{
    Identifier,
    instrument::InstrumentData,
    subscription::{
        Subscription,
        candle::Candles,
        trade::PublicTrades,
    },
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

impl<GateioExchange, Instrument> Identifier<GateioChannel>
    for Subscription<GateioExchange, Instrument, Candles>
where
    Instrument: InstrumentData,
{
    fn id(&self) -> GateioChannel {
        let interval = gateio_interval(self.kind.0).expect("validated");
        match self.instrument.kind() {
            MarketDataInstrumentKind::Spot => {
                GateioChannel(format!("spot.candlesticks_{}", interval))
            }
            MarketDataInstrumentKind::Future { .. } | MarketDataInstrumentKind::Perpetual => {
                GateioChannel(format!("futures.candlesticks_{}", interval))
            }
            MarketDataInstrumentKind::Option { .. } => {
                GateioChannel(format!("options.candlesticks_{}", interval))
            }
        }
    }
}

impl AsRef<str> for GateioChannel {
    fn as_ref(&self) -> &str {
        self.0.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::candle::{Candles, Interval};
    use crate::exchange::gateio::spot::GateioSpot;
    use barter_instrument::instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind};

    fn candles_channel_spot(interval: Interval) -> GateioChannel {
        let sub: Subscription<GateioSpot, MarketDataInstrument, Candles> = Subscription::new(
            GateioSpot::default(),
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
            Candles(interval),
        );
        sub.id()
    }

    #[test]
    fn test_candles_channel_spot_m1() {
        assert_eq!(candles_channel_spot(Interval::M1).as_ref(), "spot.candlesticks_1m");
    }

    #[test]
    fn test_candles_channel_spot_m5() {
        assert_eq!(candles_channel_spot(Interval::M5).as_ref(), "spot.candlesticks_5m");
    }

    #[test]
    fn test_candles_channel_spot_m15() {
        assert_eq!(candles_channel_spot(Interval::M15).as_ref(), "spot.candlesticks_15m");
    }

    #[test]
    fn test_candles_channel_spot_m30() {
        assert_eq!(candles_channel_spot(Interval::M30).as_ref(), "spot.candlesticks_30m");
    }

    #[test]
    fn test_candles_channel_spot_h1() {
        assert_eq!(candles_channel_spot(Interval::H1).as_ref(), "spot.candlesticks_1h");
    }

    #[test]
    fn test_candles_channel_spot_h4() {
        assert_eq!(candles_channel_spot(Interval::H4).as_ref(), "spot.candlesticks_4h");
    }

    #[test]
    fn test_candles_channel_spot_d1() {
        assert_eq!(candles_channel_spot(Interval::D1).as_ref(), "spot.candlesticks_1d");
    }

    #[test]
    fn test_candles_channel_spot_w1() {
        assert_eq!(candles_channel_spot(Interval::W1).as_ref(), "spot.candlesticks_7d");
    }

    #[test]
    fn test_candles_channel_spot_month1() {
        assert_eq!(candles_channel_spot(Interval::Month1).as_ref(), "spot.candlesticks_30d");
    }
}
