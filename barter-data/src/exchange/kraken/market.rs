use barter_instrument::asset::name::AssetNameInternal;
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Kraken`] market that can be subscribed to.
///
/// See docs: <https://docs.kraken.com/websockets/#message-subscribe>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenMarket(pub(crate) SmolStr);

impl AsRef<str> for KrakenMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

/// Map common asset names to Kraken WebSocket names.
/// Kraken uses "XBT" for Bitcoin and uppercase pairs (e.g. "XBT/USD").
fn kraken_ws_name(name: &str) -> SmolStr {
    let upper = name.to_uppercase();
    match upper.as_str() {
        "BTC" => SmolStr::new_static("XBT"),
        other => SmolStr::new(other),
    }
}

pub(in crate::exchange::kraken) fn kraken_market(
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
) -> KrakenMarket {
    let base_ws = kraken_ws_name(base.as_ref());
    let quote_ws = kraken_ws_name(quote.as_ref());
    KrakenMarket(format_smolstr!("{base_ws}/{quote_ws}"))
}
