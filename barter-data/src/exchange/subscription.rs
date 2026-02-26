use crate::{Identifier, subscription::Subscription};
use crate::exchange::Connector;
use crate::instrument::InstrumentData;
use crate::subscription::SubscriptionKind;
use barter_integration::subscription::SubscriptionId;
use serde::Deserialize;

/// Defines an exchange specific market and channel combination used by an exchange
/// [`Connector`](super::Connector) to build the
/// [`WsMessage`](barter_integration::protocol::websocket::WsMessage) subscription payloads to
/// send to the exchange server.
///
/// ### Examples
/// #### Binance OrderBooksL2
/// ```json
/// ExchangeSub {
///     channel: BinanceChannel("@depth@100ms"),
///     market: BinanceMarket("btcusdt"),
/// }
/// ```
/// #### Kraken PublicTrades
/// ```json
/// ExchangeSub {
///     channel: KrakenChannel("trade"),
///     market: KrakenChannel("BTC/USDT")
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize)]
pub struct ExchangeSub<Channel, Market> {
    /// Type that defines how to translate a Barter [`Subscription`] into an exchange specific
    /// channel to be subscribed to.
    ///
    /// ### Examples
    /// - [`BinanceChannel("@depth@100ms")`](super::binance::channel::BinanceChannel)
    /// - [`KrakenChannel("trade")`](super::kraken::channel::KrakenChannel)
    pub channel: Channel,

    /// Type that defines how to translate a Barter [`Subscription`] into an exchange specific
    /// market that can be subscribed to.
    ///
    /// ### Examples
    /// - [`BinanceMarket("btcusdt")`](super::binance::market::BinanceMarket)
    /// - [`KrakenMarket("BTC/USDT")`](super::kraken::market::KrakenMarket)
    pub market: Market,
}

impl<Channel, Market> Identifier<SubscriptionId> for ExchangeSub<Channel, Market>
where
    Channel: AsRef<str>,
    Market: AsRef<str>,
{
    fn id(&self) -> SubscriptionId {
        SubscriptionId::from(format!(
            "{}|{}",
            self.channel.as_ref(),
            self.market.as_ref()
        ))
    }
}

impl<Channel, Market> ExchangeSub<Channel, Market>
where
    Channel: AsRef<str>,
    Market: AsRef<str>,
{
    /// Construct a new exchange specific [`Self`] with the Barter [`Subscription`] provided.
    pub fn new<Exchange, Instrument, Kind>(sub: &Subscription<Exchange, Instrument, Kind>) -> Self
    where
        Exchange: Connector<Channel = Channel, Market = Market>,
        Subscription<Exchange, Instrument, Kind>: Identifier<Channel>,
        Instrument: InstrumentData,
        Kind: SubscriptionKind,
    {
        Self {
            channel: sub.id(),
            market: Exchange::resolve_market(
                sub.instrument.market_input(),
                &sub.kind.as_sub_kind(),
            ),
        }
    }
}

impl<Channel, Market> From<(Channel, Market)> for ExchangeSub<Channel, Market>
where
    Channel: AsRef<str>,
    Market: AsRef<str>,
{
    fn from((channel, market): (Channel, Market)) -> Self {
        Self { channel, market }
    }
}
