use crate::error::DataError;
use barter_instrument::{
    asset::name::AssetNameInternal, instrument::market_data::kind::MarketDataInstrumentKind,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, StrExt, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Hyperliquid`] market that can be subscribed to.
///
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HyperliquidMarket(pub(crate) SmolStr);

impl AsRef<str> for HyperliquidMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Translate Barter instrument components into a [`HyperliquidMarket`].
///
/// Hyperliquid uses:
/// - Perpetuals: uppercase base only (e.g., "BTC")
/// - Spot: "BASE/QUOTE" uppercase (e.g., "BTC/USDC")
pub(in crate::exchange::hyperliquid) fn hyperliquid_market(
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
    kind: &MarketDataInstrumentKind,
) -> Result<HyperliquidMarket, DataError> {
    match kind {
        MarketDataInstrumentKind::Perpetual => {
            Ok(HyperliquidMarket(base.as_ref().to_uppercase_smolstr()))
        }
        MarketDataInstrumentKind::Spot => Ok(HyperliquidMarket(format_smolstr!(
            "{}/{}",
            base.as_ref().to_uppercase_smolstr(),
            quote.as_ref().to_uppercase_smolstr()
        ))),
        other => Err(DataError::UnsupportedInstrument {
            exchange: "hyperliquid".into(),
            instrument: format!("{other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::instrument::market_data::{
        MarketDataInstrument, kind::MarketDataInstrumentKind,
    };

    #[test]
    fn test_hyperliquid_market_perpetual() {
        let instrument =
            MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Perpetual));
        let market =
            hyperliquid_market(&instrument.base, &instrument.quote, &instrument.kind).unwrap();
        assert_eq!(market.as_ref(), "BTC");
    }

    #[test]
    fn test_hyperliquid_market_spot() {
        let instrument =
            MarketDataInstrument::from(("btc", "usdc", MarketDataInstrumentKind::Spot));
        let market =
            hyperliquid_market(&instrument.base, &instrument.quote, &instrument.kind).unwrap();
        assert_eq!(market.as_ref(), "BTC/USDC");
    }

    #[test]
    fn test_hyperliquid_market_unsupported_returns_error() {
        use barter_instrument::instrument::market_data::kind::MarketDataFutureContract;
        use chrono::Utc;

        let instrument = MarketDataInstrument::from((
            "btc",
            "usdt",
            MarketDataInstrumentKind::Future(MarketDataFutureContract { expiry: Utc::now() }),
        ));
        let result = hyperliquid_market(&instrument.base, &instrument.quote, &instrument.kind);
        assert!(result.is_err());
    }
}
