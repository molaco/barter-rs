use crate::{
    Identifier,
    error::DataError,
    event::MarketEvent,
    exchange::StreamSelector,
    instrument::InstrumentData,
    streams::{
        handle::TypedHandle, reconnect, reconnect::stream::ReconnectionBackoffPolicy,
    },
    subscription::{Subscription, SubscriptionKind, display_subscriptions_without_exchange},
};
use barter_instrument::exchange::ExchangeId;
use derive_more::Constructor;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use tokio::sync::mpsc;
use tracing::info;

/// Default [`ReconnectionBackoffPolicy`] for a [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub const STREAM_RECONNECTION_POLICY: ReconnectionBackoffPolicy = ReconnectionBackoffPolicy {
    backoff_ms_initial: 125,
    backoff_multiplier: 2,
    backoff_ms_max: 60000,
};

/// Convenient type alias for a [`MarketEvent`] [`Result`] consumed via a
/// [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub type MarketStreamResult<InstrumentKey, Kind> =
    reconnect::Event<ExchangeId, Result<MarketEvent<InstrumentKey, Kind>, DataError>>;

/// Convenient type alias for a [`MarketEvent`] consumed via a
/// [`reconnecting`](`ReconnectingStream`) [`MarketStream`].
pub type MarketStreamEvent<InstrumentKey, Kind> =
    reconnect::Event<ExchangeId, MarketEvent<InstrumentKey, Kind>>;

/// Initialises a market stream with a connection task using a collection
/// of [`Subscription`]s.
///
/// The provided [`ReconnectionBackoffPolicy`] dictates how the exponential backoff scales
/// between reconnections.
///
/// Returns an `mpsc::UnboundedReceiver` of market events and a [`TypedHandle`] for
/// dynamic subscribe/unsubscribe on the live connection.
pub async fn init_market_stream<Exchange, Instrument, Kind>(
    policy: ReconnectionBackoffPolicy,
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
) -> Result<
    (
        mpsc::UnboundedReceiver<MarketStreamResult<Instrument::Key, Kind::Event>>,
        TypedHandle<Exchange, Instrument::Key, Kind>,
    ),
    DataError,
>
where
    Exchange: StreamSelector<Instrument, Kind> + Send + Sync + 'static,
    Instrument: InstrumentData + Display + 'static,
    Instrument::Key: Clone + Send + Sync + 'static,
    Kind: SubscriptionKind + Display + Send + Sync + 'static,
    Kind::Event: Send + 'static,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market> + 'static,
    // 'static required here (not on Connector trait) because tokio::spawn
    // captures Command<Channel, Market, IK> via the mpsc receiver.
    Exchange::Channel: 'static,
    Exchange::Market: 'static,
{
    let exchange = Exchange::ID;

    let stream_key = subscriptions
        .first()
        .map(|sub| StreamKey::new("market_stream", exchange, Some(sub.kind.as_str())))
        .ok_or(DataError::SubscriptionsEmpty)?;

    info!(
        %exchange,
        subscriptions = %display_subscriptions_without_exchange(&subscriptions),
        ?policy,
        ?stream_key,
        "MarketStream with connection task initialising"
    );

    let (command_tx, command_rx) = mpsc::unbounded_channel();
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    let handle = TypedHandle::new(command_tx);

    let (init_tx, init_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(crate::streams::task::connection_task::<
        Exchange,
        Instrument,
        Kind,
        Exchange::Transformer,
        Exchange::Parser,
        Exchange::SnapFetcher,
    >(subscriptions, command_rx, event_tx, policy, init_tx));

    // Wait for first connection to succeed
    init_rx
        .await
        .map_err(|_| DataError::ConnectionTaskTerminated)??;

    Ok((event_rx, handle))
}

#[derive(
    Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct StreamKey<Kind = &'static str> {
    pub stream: &'static str,
    pub exchange: ExchangeId,
    pub kind: Option<Kind>,
}

impl StreamKey {
    pub fn new_general(stream: &'static str, exchange: ExchangeId) -> Self {
        Self::new(stream, exchange, None)
    }
}

impl std::fmt::Debug for StreamKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            None => write!(f, "{}-{}", self.stream, self.exchange),
            Some(kind) => write!(f, "{}-{}-{}", self.stream, self.exchange, kind),
        }
    }
}
