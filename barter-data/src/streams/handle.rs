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
use tokio::sync::{mpsc, oneshot};

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
    ) -> Result<oneshot::Receiver<Result<(), SocketError>>, SocketError>
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

        // Confirmation channel — callers can await or drop (fire-and-forget).
        // TODO: When per-exchange confirmation parsing is implemented, defer
        // resolution until the exchange confirms. For now, resolve immediately.
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        Ok(rx)
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    ///
    /// Sends the unsubscribe messages to the exchange, removes entries from the
    /// instrument map, and removes them from the reconnection replay list.
    pub fn unsubscribe<Exchange>(
        &self,
        exchange_subs: Vec<ExchangeSub<Exchange::Channel, Exchange::Market>>,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<oneshot::Receiver<Result<(), SocketError>>, SocketError>
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

        // Confirmation channel — callers can await or drop (fire-and-forget).
        // TODO: When per-exchange confirmation parsing is implemented, defer
        // resolution until the exchange confirms. For now, resolve immediately.
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Ok(()));
        Ok(rx)
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

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::subscription::SubscriptionId;
    use fnv::FnvHashMap;
    use tokio::sync::mpsc;

    fn test_handle() -> (SubscriptionHandle<String>, mpsc::UnboundedReceiver<WsMessage>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let map = Arc::new(RwLock::new(Map(FnvHashMap::default())));
        (SubscriptionHandle::new(tx, map), rx)
    }

    mod dynamic_batch {
        use super::*;

        #[test]
        fn test_dynamic_batch_add_and_read() {
            let (handle, _rx) = test_handle();

            // Verify dynamic_batches starts empty
            {
                let batches = handle.dynamic_batches().read().unwrap();
                assert!(batches.is_empty(), "expected empty batches on init");
            }

            // Add a batch directly
            {
                let mut batches = handle.dynamic_batches().write().unwrap();
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([SubscriptionId::from("test|btcusdt")]),
                    entries: vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
                    subscribe_messages: vec![WsMessage::text(r#"{"method":"SUBSCRIBE"}"#)],
                });
            }

            // Verify the batch exists
            {
                let batches = handle.dynamic_batches().read().unwrap();
                assert_eq!(batches.len(), 1);
                assert!(batches[0].entry_ids.contains(&SubscriptionId::from("test|btcusdt")));
                assert_eq!(batches[0].entries.len(), 1);
                assert_eq!(batches[0].entries[0].1, "BTC");
                assert_eq!(batches[0].subscribe_messages.len(), 1);
            }
        }

        #[test]
        fn test_dynamic_batch_multiple_batches() {
            let (handle, _rx) = test_handle();

            {
                let mut batches = handle.dynamic_batches().write().unwrap();
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([SubscriptionId::from("test|btcusdt")]),
                    entries: vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
                    subscribe_messages: vec![WsMessage::text(r#"{"method":"SUBSCRIBE","params":["btcusdt"]}"#)],
                });
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([SubscriptionId::from("test|ethusdt")]),
                    entries: vec![(SubscriptionId::from("test|ethusdt"), "ETH".to_string())],
                    subscribe_messages: vec![WsMessage::text(r#"{"method":"SUBSCRIBE","params":["ethusdt"]}"#)],
                });
            }

            let batches = handle.dynamic_batches().read().unwrap();
            assert_eq!(batches.len(), 2);
            assert!(batches[0].entry_ids.contains(&SubscriptionId::from("test|btcusdt")));
            assert!(batches[1].entry_ids.contains(&SubscriptionId::from("test|ethusdt")));
        }

        #[test]
        fn test_replay_dynamic_subscriptions() {
            let (handle, mut rx) = test_handle();

            // Add two batches with subscribe messages
            {
                let mut batches = handle.dynamic_batches().write().unwrap();
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([SubscriptionId::from("test|btcusdt")]),
                    entries: vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
                    subscribe_messages: vec![
                        WsMessage::text(r#"{"method":"SUBSCRIBE","params":["btcusdt@trade"]}"#),
                    ],
                });
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([SubscriptionId::from("test|ethusdt")]),
                    entries: vec![(SubscriptionId::from("test|ethusdt"), "ETH".to_string())],
                    subscribe_messages: vec![
                        WsMessage::text(r#"{"method":"SUBSCRIBE","params":["ethusdt@trade"]}"#),
                    ],
                });
            }

            // Replay subscriptions
            handle.replay_dynamic_subscriptions().expect("replay should succeed");

            // Verify both subscribe messages were sent to the channel
            let msg1 = rx.try_recv().expect("expected first replayed message");
            assert_eq!(
                msg1.to_string(),
                r#"{"method":"SUBSCRIBE","params":["btcusdt@trade"]}"#,
            );

            let msg2 = rx.try_recv().expect("expected second replayed message");
            assert_eq!(
                msg2.to_string(),
                r#"{"method":"SUBSCRIBE","params":["ethusdt@trade"]}"#,
            );

            // No more messages
            assert!(rx.try_recv().is_err(), "expected no more messages after replay");
        }

        #[test]
        fn test_replay_dynamic_subscriptions_empty() {
            let (handle, mut rx) = test_handle();

            // Replay with no batches should succeed and send nothing
            handle.replay_dynamic_subscriptions().expect("replay with empty batches should succeed");
            assert!(rx.try_recv().is_err(), "expected no messages from empty replay");
        }

        #[test]
        fn test_merge_dynamic_entries_into_map() {
            let (handle, _rx) = test_handle();

            // Add batches with entries
            {
                let mut batches = handle.dynamic_batches().write().unwrap();
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([
                        SubscriptionId::from("test|btcusdt"),
                        SubscriptionId::from("test|ethusdt"),
                    ]),
                    entries: vec![
                        (SubscriptionId::from("test|btcusdt"), "BTC".to_string()),
                        (SubscriptionId::from("test|ethusdt"), "ETH".to_string()),
                    ],
                    subscribe_messages: vec![],
                });
            }

            // Merge entries into the map
            handle.merge_dynamic_entries_into_map();

            // Verify entries appear in the instrument map
            {
                let map = handle.instrument_map().read().unwrap();
                let btc = map.find(&SubscriptionId::from("test|btcusdt")).expect("BTC entry should exist");
                assert_eq!(btc, &"BTC".to_string());

                let eth = map.find(&SubscriptionId::from("test|ethusdt")).expect("ETH entry should exist");
                assert_eq!(eth, &"ETH".to_string());
            }
        }

        #[test]
        fn test_merge_dynamic_entries_into_map_preserves_existing() {
            let (handle, _rx) = test_handle();

            // Pre-populate the instrument map with an existing entry
            {
                let mut map = handle.instrument_map().write().unwrap();
                map.insert(SubscriptionId::from("existing|xrpusdt"), "XRP".to_string());
            }

            // Add a batch with a new entry
            {
                let mut batches = handle.dynamic_batches().write().unwrap();
                batches.push(DynamicBatch {
                    entry_ids: HashSet::from([SubscriptionId::from("test|btcusdt")]),
                    entries: vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
                    subscribe_messages: vec![],
                });
            }

            // Merge
            handle.merge_dynamic_entries_into_map();

            // Verify both existing and new entries are present
            let map = handle.instrument_map().read().unwrap();
            assert_eq!(
                map.find(&SubscriptionId::from("existing|xrpusdt")).unwrap(),
                &"XRP".to_string(),
            );
            assert_eq!(
                map.find(&SubscriptionId::from("test|btcusdt")).unwrap(),
                &"BTC".to_string(),
            );
        }
    }

    mod update_ws_sink_tx {
        use super::*;

        #[test]
        fn test_update_ws_sink_tx() {
            let (handle, mut old_rx) = test_handle();

            // Send a message on the original channel to confirm it works
            handle.send_raw(vec![WsMessage::text("before_update")]).unwrap();
            let msg = old_rx.try_recv().expect("expected message on old channel");
            assert_eq!(msg.to_string(), "before_update");

            // Create a new channel and update the sink
            let (new_tx, mut new_rx) = mpsc::unbounded_channel();
            handle.update_ws_sink_tx(new_tx);

            // Send a message — it should arrive on the new receiver
            handle.send_raw(vec![WsMessage::text("after_update")]).unwrap();
            let msg = new_rx.try_recv().expect("expected message on new channel");
            assert_eq!(msg.to_string(), "after_update");

            // Old receiver should NOT get the new message
            assert!(
                old_rx.try_recv().is_err(),
                "old receiver should not receive messages after update",
            );
        }
    }

    mod ws_sink_tx {
        use super::*;

        #[test]
        fn test_send_raw() {
            let (handle, mut rx) = test_handle();

            let messages = vec![
                WsMessage::text(r#"{"action":"subscribe"}"#),
                WsMessage::text(r#"{"action":"ping"}"#),
            ];

            handle.send_raw(messages).expect("send_raw should succeed");

            let msg1 = rx.try_recv().expect("expected first message");
            assert_eq!(msg1.to_string(), r#"{"action":"subscribe"}"#);

            let msg2 = rx.try_recv().expect("expected second message");
            assert_eq!(msg2.to_string(), r#"{"action":"ping"}"#);

            assert!(rx.try_recv().is_err(), "expected no more messages");
        }

        #[test]
        fn test_send_raw_empty_messages() {
            let (handle, mut rx) = test_handle();

            handle.send_raw(vec![]).expect("send_raw with empty vec should succeed");
            assert!(rx.try_recv().is_err(), "expected no messages from empty send");
        }

        #[test]
        fn test_send_raw_closed_channel() {
            let (handle, rx) = test_handle();

            // Drop the receiver to close the channel
            drop(rx);

            let result = handle.send_raw(vec![WsMessage::text("should_fail")]);
            assert!(result.is_err(), "send_raw should fail when receiver is dropped");
        }
    }

    mod instrument_map {
        use super::*;

        #[test]
        fn test_instrument_map_shared() {
            let (handle, _rx) = test_handle();
            let map_ref = handle.instrument_map().clone();

            // Insert via the shared Arc
            {
                let mut map = map_ref.write().unwrap();
                map.insert(SubscriptionId::from("shared|btcusdt"), "BTC_SHARED".to_string());
            }

            // Verify the handle's instrument_map sees the entry
            {
                let map = handle.instrument_map().read().unwrap();
                let value = map.find(&SubscriptionId::from("shared|btcusdt"))
                    .expect("entry inserted via shared Arc should be visible");
                assert_eq!(value, &"BTC_SHARED".to_string());
            }
        }

        #[test]
        fn test_instrument_map_initially_empty() {
            let (handle, _rx) = test_handle();
            let map = handle.instrument_map().read().unwrap();
            assert!(
                map.find(&SubscriptionId::from("nonexistent")).is_err(),
                "instrument_map should start empty",
            );
        }
    }
}
