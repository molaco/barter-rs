use self::{
    mapper::{SubscriptionMapper, WebSocketSubMapper},
    validator::SubscriptionValidator,
};
use crate::{
    Identifier,
    exchange::Connector,
    instrument::InstrumentData,
    subscription::{Map, Subscription, SubscriptionKind, SubscriptionMeta},
};
use async_trait::async_trait;
use barter_integration::{
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage, connect},
};
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use tracing::debug;

/// [`SubscriptionMapper`] implementations defining how to map a
/// collection of Barter [`Subscription`]s into exchange specific [`SubscriptionMeta`].
pub mod mapper;

/// [`SubscriptionValidator`] implementations defining how to
/// validate actioned [`Subscription`]s were successful.
pub mod validator;

/// Defines how to connect to a socket and subscribe to market data streams.
#[async_trait]
pub trait Subscriber {
    type SubMapper: SubscriptionMapper;

    async fn subscribe<Exchange, Instrument, Kind>(
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>: Identifier<Exchange::Channel>;
}

#[derive(Debug)]
pub struct Subscribed<InstrumentKey> {
    pub websocket: WebSocket,
    pub map: Map<InstrumentKey>,
    pub buffered_websocket_events: Vec<WsMessage>,
}

/// Standard [`Subscriber`] for [`WebSocket`]s suitable for most exchanges.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct WebSocketSubscriber;

#[async_trait]
impl Subscriber for WebSocketSubscriber {
    type SubMapper = WebSocketSubMapper;

    async fn subscribe<Exchange, Instrument, Kind>(
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>: Identifier<Exchange::Channel>,
    {
        // Define variables for logging ergonomics
        let exchange = Exchange::ID;
        let url = Exchange::url()?;
        debug!(%exchange, %url, ?subscriptions, "subscribing to WebSocket");

        // Connect to exchange
        let mut websocket = connect(url).await?;
        debug!(%exchange, ?subscriptions, "connected to WebSocket");

        // Map &[Subscription<Exchange, Kind>] to SubscriptionMeta
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        // Build optional rate limiter from exchange configuration
        let rate_limiter = Exchange::send_rate_limit().map(|(max_requests, per_duration)| {
            let quota = governor::Quota::with_period(per_duration / max_requests)
                .expect("send_rate_limit quota period must be > 0")
                .allow_burst(
                    std::num::NonZeroU32::new(max_requests).expect("max_requests must be > 0"),
                );
            governor::RateLimiter::direct(quota)
        });

        // Send Subscriptions over WebSocket with inter-batch delay and rate limiting
        let batch_count = ws_subscriptions.len();
        for (i, subscription) in ws_subscriptions.into_iter().enumerate() {
            // Apply rate limiting if configured
            if let Some(ref limiter) = rate_limiter {
                limiter.until_ready().await;
            }

            debug!(%exchange, payload = ?subscription, "sending exchange subscription");
            websocket
                .send(subscription)
                .await
                .map_err(|error| SocketError::WebSocket(Box::new(error)))?;

            // Sleep between batches to respect frame-rate limits
            if batch_count > 1 && i + 1 < batch_count {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }

        // Validate Subscription responses
        let (map, buffered_websocket_events) = Exchange::SubValidator::validate::<
            Exchange,
            Instrument::Key,
            Kind,
        >(instrument_map, &mut websocket)
        .await?;

        debug!(%exchange, "successfully initialised WebSocket stream with confirmed Subscriptions");
        Ok(Subscribed {
            websocket,
            map,
            buffered_websocket_events,
        })
    }
}
