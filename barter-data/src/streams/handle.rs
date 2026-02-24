use crate::{
    exchange::{Connector, subscription::ExchangeSub},
    subscription::Map,
};
use barter_integration::{
    error::SocketError,
    protocol::websocket::WsMessage,
    subscription::SubscriptionId,
};
use std::collections::HashSet;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// A batch of dynamic subscriptions tracked for replay on reconnection.
/// One batch corresponds to one `subscribe()` call.
#[derive(Debug, Clone)]
pub struct DynamicBatch<InstrumentKey> {
    pub entry_ids: HashSet<SubscriptionId>,
    pub entries: Vec<(SubscriptionId, InstrumentKey)>,
    pub subscribe_messages: Vec<WsMessage>,
}

/// Handle for dynamically subscribing/unsubscribing on a live WebSocket connection.
///
/// Obtained from `DynamicStreams::init()` or `StreamBuilder::init()`.
/// The handle sends subscribe/unsubscribe messages to the exchange and updates
/// the shared instrument map so the transformer can process new subscriptions.
///
/// **Reconnection-safe**: Dynamic subscriptions are tracked and automatically
/// re-established after WebSocket reconnections. The `ws_sink_tx` is wrapped in
/// `Arc<RwLock<>>` so it can be swapped to the new connection on reconnect.
#[derive(Debug, Clone)]
pub struct SubscriptionHandle<InstrumentKey> {
    ws_sink_tx: Arc<RwLock<mpsc::UnboundedSender<WsMessage>>>,
    instrument_map: Arc<RwLock<Map<InstrumentKey>>>,
    dynamic_batches: Arc<RwLock<Vec<DynamicBatch<InstrumentKey>>>>,
}

impl<InstrumentKey> SubscriptionHandle<InstrumentKey>
where
    InstrumentKey: Clone,
{
    /// Create a new `SubscriptionHandle`.
    pub fn new(
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
        instrument_map: Arc<RwLock<Map<InstrumentKey>>>,
    ) -> Self {
        Self {
            ws_sink_tx: Arc::new(RwLock::new(ws_sink_tx)),
            instrument_map,
            dynamic_batches: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    ///
    /// Sends the subscribe messages to the exchange, updates the instrument map,
    /// and records the subscription for replay on reconnection.
    pub fn subscribe<Exchange>(
        &self,
        exchange_subs: Vec<ExchangeSub<Exchange::Channel, Exchange::Market>>,
        entries: Vec<(SubscriptionId, InstrumentKey)>,
    ) -> Result<(), SocketError>
    where
        Exchange: Connector,
    {
        // Build subscribe messages
        let ws_messages = Exchange::requests(exchange_subs);

        // Send via current ws_sink_tx
        {
            let tx = self
                .ws_sink_tx
                .read()
                .map_err(|_| SocketError::Subscribe("ws_sink_tx RwLock poisoned".to_string()))?;
            for msg in &ws_messages {
                tx.send(msg.clone())
                    .map_err(|_| SocketError::Subscribe("ws_sink_tx closed".to_string()))?;
            }
        }

        // Update instrument map
        {
            let mut map = self
                .instrument_map
                .write()
                .map_err(|_| {
                    SocketError::Subscribe("instrument_map RwLock poisoned".to_string())
                })?;
            for (id, key) in &entries {
                map.insert(id.clone(), key.clone());
            }
        }

        // Record batch for reconnection replay
        {
            let mut batches = self
                .dynamic_batches
                .write()
                .map_err(|_| {
                    SocketError::Subscribe("dynamic_batches RwLock poisoned".to_string())
                })?;
            let entry_ids: HashSet<SubscriptionId> = entries.iter().map(|(id, _)| id.clone()).collect();
            batches.push(DynamicBatch {
                entry_ids,
                entries,
                subscribe_messages: ws_messages,
            });
        }

        Ok(())
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    ///
    /// Sends the unsubscribe messages to the exchange, removes entries from the
    /// instrument map, and removes them from the reconnection replay list.
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
        {
            let tx = self
                .ws_sink_tx
                .read()
                .map_err(|_| SocketError::Subscribe("ws_sink_tx RwLock poisoned".to_string()))?;
            for msg in ws_messages {
                tx.send(msg)
                    .map_err(|_| SocketError::Subscribe("ws_sink_tx closed".to_string()))?;
            }
        }

        // Update instrument map
        {
            let mut map = self
                .instrument_map
                .write()
                .map_err(|_| {
                    SocketError::Subscribe("instrument_map RwLock poisoned".to_string())
                })?;
            for id in &subscription_ids {
                map.remove(id);
            }
        }

        // Remove from dynamic batches
        {
            let mut batches = self
                .dynamic_batches
                .write()
                .map_err(|_| {
                    SocketError::Subscribe("dynamic_batches RwLock poisoned".to_string())
                })?;
            for batch in batches.iter_mut() {
                for id in &subscription_ids {
                    batch.entry_ids.remove(id);
                }
                batch.entries.retain(|(id, _)| !subscription_ids.contains(id));
            }
            // Drop empty batches entirely — no stale subscribe messages
            batches.retain(|batch| !batch.entry_ids.is_empty());
        }

        Ok(())
    }

    /// Send raw subscribe/unsubscribe messages without updating the instrument map.
    /// Use when you need to manage the map separately.
    pub fn send_raw(&self, messages: Vec<WsMessage>) -> Result<(), SocketError> {
        let tx = self
            .ws_sink_tx
            .read()
            .map_err(|_| SocketError::Subscribe("ws_sink_tx RwLock poisoned".to_string()))?;
        for msg in messages {
            tx.send(msg)
                .map_err(|_| SocketError::Subscribe("ws_sink_tx closed".to_string()))?;
        }
        Ok(())
    }

    /// Get a reference to the shared instrument map.
    pub fn instrument_map(&self) -> &Arc<RwLock<Map<InstrumentKey>>> {
        &self.instrument_map
    }

    /// Get a reference to the dynamic batches tracked for reconnection.
    pub fn dynamic_batches(&self) -> &Arc<RwLock<Vec<DynamicBatch<InstrumentKey>>>> {
        &self.dynamic_batches
    }

    /// Update the internal `ws_sink_tx` to point to a new WebSocket connection.
    /// Called by the reconnection logic after establishing a new connection.
    pub(crate) fn update_ws_sink_tx(&self, new_tx: mpsc::UnboundedSender<WsMessage>) {
        if let Ok(mut tx) = self.ws_sink_tx.write() {
            *tx = new_tx;
        }
    }

    /// Replay all dynamic subscription messages on the current connection.
    /// Called after reconnection to re-establish dynamic subscriptions.
    pub(crate) fn replay_dynamic_subscriptions(&self) -> Result<(), SocketError> {
        let tx = self
            .ws_sink_tx
            .read()
            .map_err(|_| SocketError::Subscribe("ws_sink_tx RwLock poisoned".to_string()))?;
        let batches = self
            .dynamic_batches
            .read()
            .map_err(|_| {
                SocketError::Subscribe("dynamic_batches RwLock poisoned".to_string())
            })?;

        for batch in batches.iter() {
            for msg in &batch.subscribe_messages {
                let _ = tx.send(msg.clone());
            }
        }

        Ok(())
    }

    /// Merge dynamic subscription entries into the instrument map.
    /// Called after reconnection to ensure the transformer can route dynamic subscription data.
    pub(crate) fn merge_dynamic_entries_into_map(&self) {
        if let (Ok(mut map), Ok(batches)) = (
            self.instrument_map.write(),
            self.dynamic_batches.read(),
        ) {
            for batch in batches.iter() {
                for (id, key) in &batch.entries {
                    map.insert(id.clone(), key.clone());
                }
            }
        }
    }
}
