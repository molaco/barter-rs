use barter_integration::{
    error::SocketError, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
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

/// Handle for dynamically subscribing/unsubscribing on a live WebSocket connection.
///
/// Commands are fire-and-forget and handled by the connection task actor.
#[derive(Debug, Clone)]
pub struct SubscriptionHandle<InstrumentKey> {
    command_tx: mpsc::UnboundedSender<Command<InstrumentKey>>,
}

impl<InstrumentKey> SubscriptionHandle<InstrumentKey>
where
    InstrumentKey: Clone,
{
    /// Create a new `SubscriptionHandle`.
    pub fn new(command_tx: mpsc::UnboundedSender<Command<InstrumentKey>>) -> Self {
        Self { command_tx }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    pub fn subscribe(
        &self,
        entries: Vec<(SubscriptionId, InstrumentKey)>,
        ws_messages: Vec<WsMessage>,
    ) -> Result<(), SocketError> {
        self.command_tx
            .send(Command::Subscribe {
                entries,
                ws_messages,
            })
            .map_err(|_| SocketError::Subscribe("command channel closed".to_string()))
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    pub fn unsubscribe(
        &self,
        subscription_ids: Vec<SubscriptionId>,
        ws_messages: Vec<WsMessage>,
    ) -> Result<(), SocketError> {
        self.command_tx
            .send(Command::Unsubscribe {
                subscription_ids,
                ws_messages,
            })
            .map_err(|_| SocketError::Subscribe("unsubscribe command channel closed".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handle() -> (
        SubscriptionHandle<String>,
        mpsc::UnboundedReceiver<Command<String>>,
    ) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        (SubscriptionHandle::new(command_tx), command_rx)
    }

    #[test]
    fn test_subscribe_sends_command() {
        let (handle, mut rx) = test_handle();

        let entries = vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())];
        let ws_messages = vec![WsMessage::text(r#"{"method":"SUBSCRIBE"}"#)];

        handle
            .subscribe(entries.clone(), ws_messages.clone())
            .unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Subscribe {
                entries: e,
                ws_messages: m,
            } => {
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

        handle
            .unsubscribe(sub_ids.clone(), ws_messages.clone())
            .unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Unsubscribe {
                subscription_ids,
                ws_messages: m,
            } => {
                assert_eq!(subscription_ids.len(), 1);
                assert_eq!(m.len(), 1);
            }
            _ => panic!("expected Unsubscribe command"),
        }
    }

    #[test]
    fn test_subscribe_closed_channel() {
        let (handle, rx) = test_handle();
        drop(rx);

        let result = handle.subscribe(
            vec![(SubscriptionId::from("test|btcusdt"), "BTC".to_string())],
            vec![WsMessage::text("sub")],
        );
        assert!(
            result.is_err(),
            "subscribe should fail when command channel is closed"
        );
    }

    #[test]
    fn test_unsubscribe_closed_channel() {
        let (handle, rx) = test_handle();
        drop(rx);

        let result = handle.unsubscribe(
            vec![SubscriptionId::from("test|btcusdt")],
            vec![WsMessage::text("unsub")],
        );
        assert!(
            result.is_err(),
            "unsubscribe should fail when command channel is closed"
        );
    }
}
