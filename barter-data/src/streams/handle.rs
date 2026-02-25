use crate::{
    Identifier,
    exchange::{Connector, subscription::ExchangeSub},
    instrument::InstrumentData,
    subscription::{Subscription, SubscriptionKind},
};
use barter_integration::{error::SocketError, subscription::SubscriptionId};
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// Command sent from SubscriptionHandle to the connection task.
#[derive(Debug, Clone)]
pub enum Command<Channel, Market, InstrumentKey> {
    Subscribe {
        /// (SubscriptionId, ExchangeSub, InstrumentKey) per instrument.
        /// SubscriptionId is pre-derived from ExchangeSub::id() by the handle.
        entries: Vec<(SubscriptionId, ExchangeSub<Channel, Market>, InstrumentKey)>,
    },
    Unsubscribe {
        /// SubscriptionIds to remove. The task looks up ExchangeSubs from ActiveSubs
        /// to call Connector::unsubscribe_requests().
        subscription_ids: Vec<SubscriptionId>,
    },
}

/// Handle for dynamically subscribing/unsubscribing on a live WebSocket connection.
///
/// Commands are fire-and-forget and handled by the connection task actor.
#[derive(Debug, Clone)]
pub struct SubscriptionHandle<Channel, Market, InstrumentKey> {
    command_tx: mpsc::UnboundedSender<Command<Channel, Market, InstrumentKey>>,
}

impl<Channel, Market, InstrumentKey> SubscriptionHandle<Channel, Market, InstrumentKey>
where
    InstrumentKey: Clone,
{
    /// Create a new `SubscriptionHandle`.
    pub fn new(
        command_tx: mpsc::UnboundedSender<Command<Channel, Market, InstrumentKey>>,
    ) -> Self {
        Self { command_tx }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    pub fn subscribe(
        &self,
        entries: Vec<(SubscriptionId, ExchangeSub<Channel, Market>, InstrumentKey)>,
    ) -> Result<(), SocketError> {
        self.command_tx
            .send(Command::Subscribe { entries })
            .map_err(|_| SocketError::Subscribe("command channel closed".to_string()))
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    pub fn unsubscribe(
        &self,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<(), SocketError> {
        self.command_tx
            .send(Command::Unsubscribe { subscription_ids })
            .map_err(|_| SocketError::Subscribe("unsubscribe command channel closed".to_string()))
    }
}

/// Type-safe handle for dynamically subscribing/unsubscribing on a live WebSocket connection.
///
/// Provides compile-time safety: a `TypedHandle<BinanceSpot, _, PublicTrades>` cannot
/// accidentally send commands to a Kraken connection.
///
/// Internally converts `Subscription` → `ExchangeSub` → `SubscriptionId` using the same
/// path as static init.
#[derive(Debug, Clone)]
pub struct TypedHandle<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    Kind: SubscriptionKind,
{
    command_tx: mpsc::UnboundedSender<Command<Exchange::Channel, Exchange::Market, InstrumentKey>>,
    _phantom: PhantomData<Kind>,
}

impl<Exchange, InstrumentKey, Kind> TypedHandle<Exchange, InstrumentKey, Kind>
where
    Exchange: Connector,
    Kind: SubscriptionKind,
    InstrumentKey: Clone,
{
    /// Create a new `TypedHandle`.
    pub fn new(
        command_tx: mpsc::UnboundedSender<
            Command<Exchange::Channel, Exchange::Market, InstrumentKey>,
        >,
    ) -> Self {
        Self {
            command_tx,
            _phantom: PhantomData,
        }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    ///
    /// Accepts the same `Subscription` type used for static init. Internally converts
    /// each subscription to an `ExchangeSub`, derives a `SubscriptionId`, and sends a
    /// `Command::Subscribe` to the connection task.
    ///
    /// Returns the `SubscriptionId`s for later unsubscription.
    pub fn subscribe<Instrument>(
        &self,
        subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
    ) -> Result<Vec<SubscriptionId>, SocketError>
    where
        Instrument: InstrumentData<Key = InstrumentKey>,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let entries: Vec<_> = subscriptions
            .iter()
            .map(|sub| {
                let exchange_sub = ExchangeSub::new(sub);
                let sub_id = exchange_sub.id();
                let instrument_key = sub.instrument.key().clone();
                (sub_id, exchange_sub, instrument_key)
            })
            .collect();

        let sub_ids = entries.iter().map(|(id, _, _)| id.clone()).collect();

        self.command_tx
            .send(Command::Subscribe { entries })
            .map_err(|_| SocketError::Subscribe("command channel closed".to_string()))?;

        Ok(sub_ids)
    }

    /// Unsubscribe from instruments by their `SubscriptionId`s (returned from `subscribe`).
    pub fn unsubscribe(
        &self,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<(), SocketError> {
        self.command_tx
            .send(Command::Unsubscribe { subscription_ids })
            .map_err(|_| {
                SocketError::Subscribe("unsubscribe command channel closed".to_string())
            })
    }

    /// Subscribe to a single instrument. Convenience wrapper around `subscribe`.
    pub fn subscribe_one<Instrument>(
        &self,
        subscription: Subscription<Exchange, Instrument, Kind>,
    ) -> Result<SubscriptionId, SocketError>
    where
        Instrument: InstrumentData<Key = InstrumentKey>,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        self.subscribe(vec![subscription])
            .map(|ids| ids.into_iter().next().expect("subscribe returned empty ids"))
    }

    /// Unsubscribe from a single instrument. Convenience wrapper around `unsubscribe`.
    pub fn unsubscribe_one(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(), SocketError> {
        self.unsubscribe(vec![subscription_id])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_handle() -> (
        SubscriptionHandle<String, String, String>,
        mpsc::UnboundedReceiver<Command<String, String, String>>,
    ) {
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        (SubscriptionHandle::new(command_tx), command_rx)
    }

    #[test]
    fn test_subscribe_sends_command() {
        let (handle, mut rx) = test_handle();

        let entries = vec![(
            SubscriptionId::from("test|btcusdt"),
            ExchangeSub {
                channel: "trades".to_string(),
                market: "btcusdt".to_string(),
            },
            "BTC".to_string(),
        )];

        handle.subscribe(entries.clone()).unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Subscribe { entries: e } => {
                assert_eq!(e.len(), 1);
                assert_eq!(e[0].2, "BTC");
                assert_eq!(e[0].1.channel, "trades");
                assert_eq!(e[0].1.market, "btcusdt");
            }
            _ => panic!("expected Subscribe command"),
        }
    }

    #[test]
    fn test_unsubscribe_sends_command() {
        let (handle, mut rx) = test_handle();

        let sub_ids = vec![SubscriptionId::from("test|btcusdt")];

        handle.unsubscribe(sub_ids.clone()).unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Unsubscribe { subscription_ids } => {
                assert_eq!(subscription_ids.len(), 1);
            }
            _ => panic!("expected Unsubscribe command"),
        }
    }

    #[test]
    fn test_subscribe_closed_channel() {
        let (handle, rx) = test_handle();
        drop(rx);

        let result = handle.subscribe(vec![(
            SubscriptionId::from("test|btcusdt"),
            ExchangeSub {
                channel: "trades".to_string(),
                market: "btcusdt".to_string(),
            },
            "BTC".to_string(),
        )]);
        assert!(
            result.is_err(),
            "subscribe should fail when command channel is closed"
        );
    }

    #[test]
    fn test_unsubscribe_closed_channel() {
        let (handle, rx) = test_handle();
        drop(rx);

        let result = handle.unsubscribe(vec![SubscriptionId::from("test|btcusdt")]);
        assert!(
            result.is_err(),
            "unsubscribe should fail when command channel is closed"
        );
    }
}
