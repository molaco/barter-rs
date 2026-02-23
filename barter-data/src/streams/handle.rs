use crate::{
    exchange::{Connector, subscription::ExchangeSub},
    subscription::Map,
};
use barter_integration::{
    error::SocketError,
    protocol::websocket::WsMessage,
    subscription::SubscriptionId,
};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Handle for dynamically subscribing/unsubscribing on a live WebSocket connection.
///
/// Obtained from `DynamicStreams::init()` or `StreamBuilder::init()`.
/// The handle sends subscribe/unsubscribe messages to the exchange and updates
/// the shared instrument map so the transformer can process new subscriptions.
#[derive(Debug, Clone)]
pub struct SubscriptionHandle<InstrumentKey> {
    ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    instrument_map: Arc<RwLock<Map<InstrumentKey>>>,
}

impl<InstrumentKey> SubscriptionHandle<InstrumentKey> {
    /// Create a new `SubscriptionHandle`.
    pub fn new(
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        instrument_map: Arc<RwLock<Map<InstrumentKey>>>,
    ) -> Self {
        Self {
            ws_sink_tx,
            instrument_map,
        }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    ///
    /// Sends the subscribe messages to the exchange and updates the instrument map.
    /// This is fire-and-forget -- the exchange may reject the subscription silently.
    pub fn subscribe<Exchange>(
        &self,
        exchange_subs: Vec<ExchangeSub<Exchange::Channel, Exchange::Market>>,
        entries: Vec<(SubscriptionId, InstrumentKey)>,
    ) -> Result<(), SocketError>
    where
        Exchange: Connector,
    {
        // Build and send subscribe messages
        let ws_messages = Exchange::requests(exchange_subs);
        for msg in ws_messages {
            self.ws_sink_tx
                .send(msg)
                .map_err(|_| SocketError::Subscribe("ws_sink_tx closed".to_string()))?;
        }

        // Update instrument map
        let mut map = self
            .instrument_map
            .write()
            .map_err(|_| SocketError::Subscribe("instrument_map RwLock poisoned".to_string()))?;
        for (id, key) in entries {
            map.insert(id, key);
        }

        Ok(())
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    ///
    /// Sends the unsubscribe messages to the exchange and removes entries from the instrument map.
    pub fn unsubscribe<Exchange>(
        &self,
        exchange_subs: Vec<ExchangeSub<Exchange::Channel, Exchange::Market>>,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<(), SocketError>
    where
        Exchange: Connector,
    {
        // Build and send unsubscribe messages
        let ws_messages = Exchange::unsubscribe_requests(exchange_subs);
        for msg in ws_messages {
            self.ws_sink_tx
                .send(msg)
                .map_err(|_| SocketError::Subscribe("ws_sink_tx closed".to_string()))?;
        }

        // Update instrument map
        let mut map = self
            .instrument_map
            .write()
            .map_err(|_| SocketError::Subscribe("instrument_map RwLock poisoned".to_string()))?;
        for id in &subscription_ids {
            map.remove(id);
        }

        Ok(())
    }

    /// Send raw subscribe/unsubscribe messages without updating the instrument map.
    /// Use when you need to manage the map separately.
    pub fn send_raw(&self, messages: Vec<WsMessage>) -> Result<(), SocketError> {
        for msg in messages {
            self.ws_sink_tx
                .send(msg)
                .map_err(|_| SocketError::Subscribe("ws_sink_tx closed".to_string()))?;
        }
        Ok(())
    }

    /// Get a reference to the shared instrument map.
    pub fn instrument_map(&self) -> &Arc<RwLock<Map<InstrumentKey>>> {
        &self.instrument_map
    }
}
