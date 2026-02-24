use crate::{
    Identifier, MarketStream,
    error::DataError,
    event::MarketEvent,
    exchange::StreamSelector,
    instrument::InstrumentData,
    streams::{
        handle::SubscriptionHandle,
        reconnect,
        reconnect::stream::{
            ReconnectingStream, ReconnectionBackoffPolicy, init_reconnecting_stream,
        },
    },
    subscription::{Subscription, SubscriptionKind, display_subscriptions_without_exchange},
};
use barter_instrument::exchange::ExchangeId;
use derive_more::Constructor;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::sync::{Arc, Mutex};
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

/// Initialises a [`reconnecting`](`ReconnectingStream`) [`MarketStream`] using a collection of
/// [`Subscription`]s.
///
/// The provided [`ReconnectionBackoffPolicy`] dictates how the exponential backoff scales
/// between reconnections.
pub async fn init_market_stream<Exchange, Instrument, Kind>(
    policy: ReconnectionBackoffPolicy,
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
) -> Result<
    (
        impl Stream<Item = MarketStreamResult<Instrument::Key, Kind::Event>>,
        Option<SubscriptionHandle<Instrument::Key>>,
    ),
    DataError,
>
where
    Exchange: StreamSelector<Instrument, Kind>,
    Instrument: InstrumentData + Display,
    Kind: SubscriptionKind + Display,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    // Determine ExchangeId associated with these Subscriptions
    let exchange = Exchange::ID;

    // Determine StreamKey for use in logging
    let stream_key = subscriptions
        .first()
        .map(|sub| StreamKey::new("market_stream", exchange, Some(sub.kind.as_str())))
        .ok_or(DataError::SubscriptionsEmpty)?;

    info!(
        %exchange,
        subscriptions = %display_subscriptions_without_exchange(&subscriptions),
        ?policy,
        ?stream_key,
        "MarketStream with auto reconnect initialising"
    );

    // Use a oneshot to send the handle from the first init to the caller.
    // On reconnects, init_reconnect() updates the existing handle's shared state.
    let (handle_tx, handle_rx) = tokio::sync::oneshot::channel();
    let handle_tx = Arc::new(Mutex::new(Some(handle_tx)));

    // Shared reference to the handle, populated after first init.
    // On reconnect, the closure reads this to pass to init_reconnect().
    let shared_handle: Arc<Mutex<Option<SubscriptionHandle<Instrument::Key>>>> =
        Arc::new(Mutex::new(None));

    let stream = init_reconnecting_stream(move || {
        let subscriptions = subscriptions.clone();
        let handle_tx = handle_tx.clone();
        let shared_handle = shared_handle.clone();
        async move {
            // Check if we have an existing handle from a previous init (reconnection path)
            let existing_handle = shared_handle
                .lock()
                .expect("shared_handle mutex poisoned")
                .clone();

            if let Some(ref handle) = existing_handle {
                // Reconnect path: reuse handle, update its shared state
                let stream = Exchange::Stream::init_reconnect::<Exchange::SnapFetcher>(
                    &subscriptions,
                    handle,
                )
                .await?;
                Ok::<_, DataError>(stream)
            } else {
                // First init: create fresh handle
                let (stream, handle) =
                    Exchange::Stream::init::<Exchange::SnapFetcher>(&subscriptions).await?;

                // Store handle for future reconnects
                *shared_handle.lock().expect("shared_handle mutex poisoned") = handle.clone();

                // Send handle to caller via oneshot
                if let Some(tx) = handle_tx.lock().expect("handle_tx mutex poisoned").take() {
                    let _ = tx.send(handle);
                }

                Ok::<_, DataError>(stream)
            }
        }
    })
    .await?
    .with_reconnect_backoff(policy, stream_key)
    .with_termination_on_error(|error| error.is_terminal(), stream_key)
    .with_reconnection_events(exchange);

    let handle = handle_rx.await.ok().flatten();

    Ok((stream, handle))
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
