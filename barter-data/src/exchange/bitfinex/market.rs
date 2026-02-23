use super::{Bitfinex, bitfinex_interval};
use crate::{
    Identifier, instrument::MarketInstrumentData,
    subscription::{Subscription, candle::Candles, trade::PublicTrades},
};
use barter_instrument::{
    Keyed, asset::name::AssetNameInternal, instrument::market_data::MarketDataInstrument,
};
use serde::{Deserialize, Serialize};
use smol_str::{SmolStr, ToSmolStr, format_smolstr};

/// Type that defines how to translate a Barter [`Subscription`] into a
/// [`Bitfinex`] market that can be subscribed to.
///
/// See docs: <https://docs.bitfinex.com/docs/ws-public>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct BitfinexMarket(pub SmolStr);

// --- MarketDataInstrument ---

impl Identifier<BitfinexMarket> for Subscription<Bitfinex, MarketDataInstrument, PublicTrades> {
    fn id(&self) -> BitfinexMarket {
        bitfinex_market(&self.instrument.base, &self.instrument.quote)
    }
}

impl Identifier<BitfinexMarket> for Subscription<Bitfinex, MarketDataInstrument, Candles> {
    fn id(&self) -> BitfinexMarket {
        let interval = bitfinex_interval(self.kind.0).expect("validated");
        let symbol = bitfinex_market(&self.instrument.base, &self.instrument.quote);
        BitfinexMarket(format_smolstr!("trade:{}:{}", interval, symbol.0))
    }
}

// --- Keyed<InstrumentKey, MarketDataInstrument> ---

impl<InstrumentKey> Identifier<BitfinexMarket>
    for Subscription<Bitfinex, Keyed<InstrumentKey, MarketDataInstrument>, PublicTrades>
{
    fn id(&self) -> BitfinexMarket {
        bitfinex_market(&self.instrument.value.base, &self.instrument.value.quote)
    }
}

impl<InstrumentKey> Identifier<BitfinexMarket>
    for Subscription<Bitfinex, Keyed<InstrumentKey, MarketDataInstrument>, Candles>
{
    fn id(&self) -> BitfinexMarket {
        let interval = bitfinex_interval(self.kind.0).expect("validated");
        let symbol = bitfinex_market(&self.instrument.value.base, &self.instrument.value.quote);
        BitfinexMarket(format_smolstr!("trade:{}:{}", interval, symbol.0))
    }
}

// --- MarketInstrumentData<InstrumentKey> ---

impl<InstrumentKey> Identifier<BitfinexMarket>
    for Subscription<Bitfinex, MarketInstrumentData<InstrumentKey>, PublicTrades>
{
    fn id(&self) -> BitfinexMarket {
        BitfinexMarket(self.instrument.name_exchange.to_smolstr())
    }
}

impl<InstrumentKey> Identifier<BitfinexMarket>
    for Subscription<Bitfinex, MarketInstrumentData<InstrumentKey>, Candles>
{
    fn id(&self) -> BitfinexMarket {
        let interval = bitfinex_interval(self.kind.0).expect("validated");
        BitfinexMarket(format_smolstr!(
            "trade:{}:{}",
            interval,
            self.instrument.name_exchange
        ))
    }
}

impl AsRef<str> for BitfinexMarket {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

fn bitfinex_market(base: &AssetNameInternal, quote: &AssetNameInternal) -> BitfinexMarket {
    BitfinexMarket(format_smolstr!(
        "t{}{}",
        base.to_string().to_uppercase(),
        quote.to_string().to_uppercase()
    ))
}
