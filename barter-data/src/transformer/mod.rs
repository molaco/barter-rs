use crate::{
    error::DataError,
    event::MarketEvent,
    subscription::{Map, SubscriptionKind},
};
use barter_integration::{Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId};
use tokio::sync::mpsc;

/// Generic stateless [`ExchangeTransformer`] often used for transforming
/// [`PublicTrades`](crate::subscription::trade::PublicTrades) streams.
pub mod stateless;

/// Defines how to construct a [`Transformer`] used by [`MarketStream`](super::MarketStream)s to
/// translate exchange specific types to normalised Barter types.
pub trait ExchangeTransformer<Exchange, InstrumentKey, Kind>
where
    Self: Transformer<Output = MarketEvent<InstrumentKey, Kind::Event>, Error = DataError> + Sized,
    InstrumentKey: Sync,
    Kind: SubscriptionKind,
{
    /// Initialise a new [`Self`], also fetching any market data snapshots required for the
    /// associated Exchange and SubscriptionKind market stream to function.
    ///
    /// The [`mpsc::UnboundedSender`] can be used by [`Self`] to send messages back to the exchange.
    fn init(
        instrument_map: Map<InstrumentKey>,
        initial_snapshots: &[MarketEvent<InstrumentKey, Kind::Event>],
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    ) -> impl std::future::Future<Output = Result<Self, DataError>> + Send;

    /// Insert entries into the transformer's instrument map for dynamic subscriptions.
    /// Default: logs a warning and drops the entries.
    fn insert_map_entries(&mut self, entries: Vec<(SubscriptionId, InstrumentKey)>) {
        if !entries.is_empty() {
            tracing::warn!(
                count = entries.len(),
                "insert_map_entries called on transformer with no override — entries dropped"
            );
        }
    }

    /// Remove entries from the transformer's instrument map for dynamic unsubscriptions.
    /// Default: logs a warning and ignores the subscription IDs.
    fn remove_map_entries(&mut self, subscription_ids: &[SubscriptionId]) {
        if !subscription_ids.is_empty() {
            tracing::warn!(
                count = subscription_ids.len(),
                "remove_map_entries called on transformer with no override — entries ignored"
            );
        }
    }
}
