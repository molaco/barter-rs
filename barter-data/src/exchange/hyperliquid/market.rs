use super::Hyperliquid;
use crate::{Identifier, instrument::MarketInstrumentData, subscription::Subscription};
use barter_instrument::{
    Keyed,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Hyperliquid`] market that can be subscribed to.
///
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HyperliquidMarket(pub SmolStr);

impl<Kind> Identifier<HyperliquidMarket> for Subscription<Hyperliquid, MarketDataInstrument, Kind> {
    fn id(&self) -> HyperliquidMarket {
        hyperliquid_market(&self.instrument)
    }
}

impl<InstrumentKey, Kind> Identifier<HyperliquidMarket>
    for Subscription<Hyperliquid, Keyed<InstrumentKey, MarketDataInstrument>, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        hyperliquid_market(&self.instrument.value)
    }
}

impl<InstrumentKey, Kind> Identifier<HyperliquidMarket>
    for Subscription<Hyperliquid, MarketInstrumentData<InstrumentKey>, Kind>
{
    fn id(&self) -> HyperliquidMarket {
        HyperliquidMarket(self.instrument.name_exchange.name().clone())
    }
}

impl AsRef<str> for HyperliquidMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Translate a Barter [`MarketDataInstrument`] into a [`HyperliquidMarket`].
///
/// Hyperliquid uses:
/// - Perpetuals: uppercase base only (e.g., "BTC")
/// - Spot: "BASE/QUOTE" uppercase (e.g., "BTC/USDC")
fn hyperliquid_market(instrument: &MarketDataInstrument) -> HyperliquidMarket {
    let MarketDataInstrument { base, quote, kind } = instrument;

    match kind {
        MarketDataInstrumentKind::Perpetual => {
            HyperliquidMarket(base.as_ref().to_uppercase_smolstr())
        }
        MarketDataInstrumentKind::Spot => HyperliquidMarket(format_smolstr!(
            "{}/{}",
            base.as_ref().to_uppercase_smolstr(),
            quote.as_ref().to_uppercase_smolstr()
        )),
        other => panic!("Hyperliquid does not support {other} instruments"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::instrument::market_data::kind::MarketDataInstrumentKind;

    #[test]
    fn test_hyperliquid_market_perpetual() {
        let instrument =
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Perpetual));
        let market = hyperliquid_market(&instrument);
        assert_eq!(market.as_ref(), "BTC");
    }

    #[test]
    fn test_hyperliquid_market_spot() {
        let instrument =
            MarketDataInstrument::from(("btc", "usdc", MarketDataInstrumentKind::Spot));
        let market = hyperliquid_market(&instrument);
        assert_eq!(market.as_ref(), "BTC/USDC");
    }

    #[test]
    #[should_panic(expected = "Hyperliquid does not support")]
    fn test_hyperliquid_market_unsupported() {
        use barter_instrument::instrument::market_data::kind::MarketDataFutureContract;
        use chrono::Utc;

        let instrument = MarketDataInstrument::from((
            "btc",
            "usdt",
            MarketDataInstrumentKind::Future(MarketDataFutureContract {
                expiry: Utc::now(),
            }),
        ));
        hyperliquid_market(&instrument);
    }
}
