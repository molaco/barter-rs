use barter_integration::{
    error::SocketError,
    protocol::websocket::WsMessage,
    subscription::SubscriptionId,
};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Command sent from SubscriptionHandle to the connection task.
#[derive(Debug, Clone)]
pub enum Command<InstrumentKey> {
    Subscribe {
        entries: Vec<(SubscriptionId, InstrumentKey)>,
        ws_messages: Vec<WsMessage>,
    },
    Unsubscribe {
        subscription_ids: Vec<SubscriptionId>,
        ws_messages: Vec<WsMessage>,
    },
}

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
/// Sends commands to the connection task via a channel. The task applies commands
/// to the transformer's instrument map and forwards WebSocket messages to the exchange.
///
/// `dynamic_batches` tracks subscriptions for replay on reconnection (cold path only).
#[derive(Debug, Clone)]
pub struct SubscriptionHandle<InstrumentKey> {
    command_tx: mpsc::UnboundedSender<Command<InstrumentKey>>,
    pub(crate) dynamic_batches: Arc<Mutex<Vec<DynamicBatch<InstrumentKey>>>>,
}

impl<InstrumentKey> SubscriptionHandle<InstrumentKey>
where
    InstrumentKey: Clone,
{
    /// Create a new `SubscriptionHandle`.
    pub fn new(
        command_tx: mpsc::UnboundedSender<Command<InstrumentKey>>,
        dynamic_batches: Arc<Mutex<Vec<DynamicBatch<InstrumentKey>>>>,
    ) -> Self {
        Self {
            command_tx,
            dynamic_batches,
        }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    ///
    /// Sends a subscribe command to the connection task and records the batch
    /// for replay on reconnection.
    pub fn subscribe(
        &self,
        entries: Vec<(SubscriptionId, InstrumentKey)>,
        ws_messages: Vec<WsMessage>,
    ) -> Result<(), SocketError> {
        // Record batch for reconnection replay
        {
            let mut batches = self
                .dynamic_batches
                .lock()
                .expect("dynamic_batches mutex poisoned");
            let entry_ids: HashSet<SubscriptionId> =
                entries.iter().map(|(id, _)| id.clone()).collect();
            batches.push(DynamicBatch {
                entry_ids,
                entries: entries.clone(),
                subscribe_messages: ws_messages.clone(),
            });
        }

        // Send command to connection task
        self.command_tx
            .send(Command::Subscribe {
                entries,
                ws_messages,
            })
            .map_err(|_| SocketError::Subscribe("command channel closed".to_string()))
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    ///
    /// Sends an unsubscribe command to the connection task and removes entries
    /// from the reconnection replay list.
    pub fn unsubscribe(
        &self,
        subscription_ids: Vec<SubscriptionId>,
        ws_messages: Vec<WsMessage>,
    ) -> Result<(), SocketError> {
        // Remove from dynamic batches
        {
            let mut batches = self
                .dynamic_batches
                .lock()
                .expect("dynamic_batches mutex poisoned");
            for batch in batches.iter_mut() {
                for id in &subscription_ids {
                    batch.entry_ids.remove(id);
                }
                batch
                    .entries
                    .retain(|(id, _)| !subscription_ids.contains(id));
            }
            // Drop empty batches entirely
            batches.retain(|batch| !batch.entry_ids.is_empty());
        }

        // Send command to connection task
        self.command_tx
            .send(Command::Unsubscribe {
                subscription_ids,
                ws_messages,
            })
            .map_err(|_| SocketError::Subscribe("command channel closed".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_integration::subscription::SubscriptionId;

    fn test_handle() -> (
        SubscriptionHandle<String>,
        mpsc::UnboundedReceiver<Command<String>>,
    ) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let dynamic_batches = Arc::new(Mutex::new(Vec::new()));
        (SubscriptionHandle::new(command_tx, dynamic_batches), command_rx)
    }

    #[test]
    fn test_subscribe_sends_command() {
        let (handle, mut rx) = test_handle();

        let entries = vec![
            (SubscriptionId::from("test|btcusdt"), "BTC".to_string()),
        ];
        let ws_messages = vec![WsMessage::text(r#"{"method":"SUBSCRIBE"}"#)];

        handle.subscribe(entries.clone(), ws_messages.clone()).unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Subscribe { entries: e, ws_messages: m } => {
                assert_eq!(e.len(), 1);
                assert_eq!(e[0].1, "BTC");
                assert_eq!(m.len(), 1);
            }
            _ => panic!("expected Subscribe command"),
        }
    }

    #[test]
    fn test_unsubscribe_sends_command() {
        let (handle, mut rx) = test_handle();

        let sub_ids = vec![SubscriptionId::from("test|btcusdt")];
        let ws_messages = vec![WsMessage::text(r#"{"method":"UNSUBSCRIBE"}"#)];

        handle.unsubscribe(sub_ids.clone(), ws_messages.clone()).unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Unsubscribe { subscription_ids, ws_messages: m } => {
                assert_eq!(subscription_ids.len(), 1);
                assert_eq!(m.len(), 1);
            }
            _ => panic!("expected Unsubscribe command"),
        }
    }

    #[test]
    fn test_subscribe_records_dynamic_batch() {
        let (handle, _rx) = test_handle();

        let entries = vec![
            (SubscriptionId::from("test|btcusdt"), "BTC".to_string()),
            (SubscriptionId::from("test|ethusdt"), "ETH".to_string()),
        ];
        let ws_messages = vec![WsMessage::text(r#"{"method":"SUBSCRIBE"}"#)];

        handle.subscribe(entries, ws_messages).unwrap();

        let batches = handle.dynamic_batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].entries.len(), 2);
        assert!(batches[0].entry_ids.contains(&SubscriptionId::from("test|btcusdt")));
        assert!(batches[0].entry_ids.contains(&SubscriptionId::from("test|ethusdt")));
    }

    #[test]
    fn test_unsubscribe_removes_from_dynamic_batches() {
        let (handle, _rx) = test_handle();

        // Subscribe two batches
        handle.subscribe(
            vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
            vec![WsMessage::text("sub1")],
        ).unwrap();
        handle.subscribe(
            vec![(SubscriptionId::from("test|ethusdt"), "ETH".to_string())],
            vec![WsMessage::text("sub2")],
        ).unwrap();

        assert_eq!(handle.dynamic_batches.lock().unwrap().len(), 2);

        // Unsubscribe BTC — removes first batch entirely
        handle.unsubscribe(
            vec![SubscriptionId::from("test|btcusdt")],
            vec![WsMessage::text("unsub1")],
        ).unwrap();

        let batches = handle.dynamic_batches.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert!(batches[0].entry_ids.contains(&SubscriptionId::from("test|ethusdt")));
    }

    #[test]
    fn test_subscribe_closed_channel() {
        let (handle, rx) = test_handle();
        drop(rx);

        let result = handle.subscribe(
            vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
            vec![WsMessage::text("sub")],
        );
        assert!(result.is_err(), "subscribe should fail when command channel is closed");
    }

    #[test]
    fn test_unsubscribe_closed_channel() {
        let (handle, rx) = test_handle();
        drop(rx);

        let result = handle.unsubscribe(
            vec![SubscriptionId::from("test|btcusdt")],
            vec![WsMessage::text("unsub")],
        );
        assert!(result.is_err(), "unsubscribe should fail when command channel is closed");
    }
}
