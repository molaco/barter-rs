use barter_instrument::asset::name::AssetNameInternal;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kraken`] market that can be subscribed to.
///
/// Kraken v2 uses standard asset names (BTC, not XBT) with slash-separated
/// pairs (e.g. "BTC/USD").
///
/// See docs: <https://docs.kraken.com/api/docs/websocket-v2/trade/>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenMarket(pub(crate) SmolStr);

impl AsRef<str> for KrakenMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

pub(in crate::exchange::kraken) fn kraken_market(
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
) -> KrakenMarket {
    let base_upper = base.as_ref().to_uppercase();
    let quote_upper = quote.as_ref().to_uppercase();
    KrakenMarket(format_smolstr!("{base_upper}/{quote_upper}"))
}
