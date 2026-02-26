use crate::{
    Identifier,
    error::DataError,
    exchange::{Connector, subscription::ExchangeSub},
    instrument::InstrumentData,
    subscription::{SubKind, Subscription, SubscriptionKind},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{error::SocketError, subscription::SubscriptionId};
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// A single resolved subscription entry for the connection task.
#[derive(Debug, Clone)]
pub struct SubEntry<Channel, Market, InstrumentKey> {
    pub id: SubscriptionId,
    pub exchange_sub: ExchangeSub<Channel, Market>,
    pub instrument_key: InstrumentKey,
}

/// Command sent from a [`TypedHandle`] (or [`SubscriptionHandle`]) to the connection task.
#[derive(Debug, Clone)]
pub enum Command<Channel, Market, InstrumentKey> {
    Subscribe {
        /// Resolved subscription entries per instrument.
        /// SubscriptionId is pre-derived from ExchangeSub::id() by the handle.
        entries: Vec<SubEntry<Channel, Market, InstrumentKey>>,
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
pub(crate) struct SubscriptionHandle<Channel, Market, InstrumentKey> {
    command_tx: mpsc::UnboundedSender<Command<Channel, Market, InstrumentKey>>,
}

impl<Channel, Market, InstrumentKey> SubscriptionHandle<Channel, Market, InstrumentKey>
where
    InstrumentKey: Clone,
{
    /// Create a new `SubscriptionHandle`.
    pub(crate) fn new(
        command_tx: mpsc::UnboundedSender<Command<Channel, Market, InstrumentKey>>,
    ) -> Self {
        Self { command_tx }
    }

    /// Subscribe to new instruments on the live WebSocket connection.
    pub(crate) fn subscribe(
        &self,
        entries: Vec<SubEntry<Channel, Market, InstrumentKey>>,
    ) -> Result<(), SocketError> {
        self.command_tx
            .send(Command::Subscribe { entries })
            .map_err(|_| SocketError::Subscribe("command channel closed".to_string()))
    }

    /// Unsubscribe from instruments on the live WebSocket connection.
    pub(crate) fn unsubscribe(
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
    ) -> Result<Vec<SubscriptionId>, DataError>
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
                SubEntry { id: sub_id, exchange_sub, instrument_key }
            })
            .collect();

        let sub_ids = entries.iter().map(|e| e.id.clone()).collect();

        self.command_tx
            .send(Command::Subscribe { entries })
            .map_err(|_| DataError::CommandChannelClosed)?;

        Ok(sub_ids)
    }

    /// Unsubscribe from instruments by their `SubscriptionId`s (returned from `subscribe`).
    pub fn unsubscribe(
        &self,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<(), DataError> {
        self.command_tx
            .send(Command::Unsubscribe { subscription_ids })
            .map_err(|_| DataError::CommandChannelClosed)
    }

    /// Subscribe to a single instrument. Convenience wrapper around `subscribe`.
    pub fn subscribe_one<Instrument>(
        &self,
        subscription: Subscription<Exchange, Instrument, Kind>,
    ) -> Result<SubscriptionId, DataError>
    where
        Instrument: InstrumentData<Key = InstrumentKey>,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let mut ids = self.subscribe(vec![subscription])?;
        debug_assert_eq!(ids.len(), 1);
        Ok(ids.swap_remove(0))
    }

    /// Unsubscribe from a single instrument. Convenience wrapper around `unsubscribe`.
    pub fn unsubscribe_one(
        &self,
        subscription_id: SubscriptionId,
    ) -> Result<(), DataError> {
        self.unsubscribe(vec![subscription_id])
    }
}

/// Type-erased handle for dynamic subscribe/unsubscribe operations.
///
/// Stored in `DynamicStreamHandles` as `Box<dyn DynHandle<Instrument>>`.
/// Eliminates the need for `Box<dyn Any>` and `downcast_ref`.
pub trait DynHandle<Instrument>: Send + Sync {
    fn subscribe_erased(
        &self,
        subscriptions: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
    ) -> Result<Vec<SubscriptionId>, DataError>;

    fn unsubscribe_erased(
        &self,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<(), DataError>;
}

impl<Instrument> std::fmt::Debug for dyn DynHandle<Instrument> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("dyn DynHandle").finish()
    }
}

impl<Exchange, Instrument, Kind> DynHandle<Instrument>
    for TypedHandle<Exchange, Instrument::Key, Kind>
where
    Exchange: Connector + 'static,
    Instrument: InstrumentData + Clone + 'static,
    Instrument::Key: Clone + Send + Sync + 'static,
    Kind: SubscriptionKind + TryFrom<SubKind, Error = DataError> + Send + Sync + 'static,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    fn subscribe_erased(
        &self,
        subscriptions: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
    ) -> Result<Vec<SubscriptionId>, DataError> {
        let typed_subs = subscriptions
            .into_iter()
            .map(|sub| {
                let kind = Kind::try_from(sub.kind)?;
                Ok(Subscription::new(Exchange::default(), sub.instrument, kind))
            })
            .collect::<Result<Vec<_>, DataError>>()?;
        self.subscribe(typed_subs)
    }

    fn unsubscribe_erased(
        &self,
        subscription_ids: Vec<SubscriptionId>,
    ) -> Result<(), DataError> {
        self.unsubscribe(subscription_ids)
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

        let entries = vec![SubEntry {
            id: SubscriptionId::from("test|btcusdt"),
            exchange_sub: ExchangeSub {
                channel: "trades".to_string(),
                market: "btcusdt".to_string(),
            },
            instrument_key: "BTC".to_string(),
        }];

        handle.subscribe(entries.clone()).unwrap();

        let cmd = rx.try_recv().expect("expected command");
        match cmd {
            Command::Subscribe { entries: e } => {
                assert_eq!(e.len(), 1);
                assert_eq!(e[0].instrument_key, "BTC");
                assert_eq!(e[0].exchange_sub.channel, "trades");
                assert_eq!(e[0].exchange_sub.market, "btcusdt");
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

        let result = handle.subscribe(vec![SubEntry {
            id: SubscriptionId::from("test|btcusdt"),
            exchange_sub: ExchangeSub {
                channel: "trades".to_string(),
                market: "btcusdt".to_string(),
            },
            instrument_key: "BTC".to_string(),
        }]);
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

    mod typed_handle {
        use super::*;
        use super::SubEntry;
        use crate::{
            Identifier,
            exchange::binance::{channel::BinanceChannel, market::BinanceMarket, spot::BinanceSpot},
            subscription::trade::PublicTrades,
        };
        use barter_instrument::instrument::market_data::{
            MarketDataInstrument, kind::MarketDataInstrumentKind,
        };

        /// Helper to create a `TypedHandle<BinanceSpot, MarketDataInstrument, PublicTrades>`
        /// and its corresponding command receiver.
        fn typed_handle() -> (
            TypedHandle<BinanceSpot, MarketDataInstrument, PublicTrades>,
            mpsc::UnboundedReceiver<
                Command<BinanceChannel, BinanceMarket, MarketDataInstrument>,
            >,
        ) {
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            (TypedHandle::new(command_tx), command_rx)
        }

        /// Helper to build a BinanceSpot PublicTrades Subscription for BTC/USDT.
        fn btc_usdt_sub() -> Subscription<BinanceSpot, MarketDataInstrument, PublicTrades> {
            Subscription::new(
                BinanceSpot::default(),
                MarketDataInstrument::from(("btc", "usdt", MarketDataInstrumentKind::Spot)),
                PublicTrades,
            )
        }

        #[test]
        fn test_typed_handle_subscribe_sends_command() {
            let (handle, mut rx) = typed_handle();
            let sub = btc_usdt_sub();

            // Derive expected ExchangeSub and SubscriptionId from the subscription
            // to compare against what TypedHandle produces internally.
            let expected_channel: BinanceChannel = sub.id();
            let expected_market: BinanceMarket = sub.id();

            handle.subscribe(vec![sub]).unwrap();

            let cmd = rx.try_recv().expect("expected Command on receiver");
            match cmd {
                Command::Subscribe { entries } => {
                    assert_eq!(entries.len(), 1);

                    let entry = &entries[0];

                    // Verify ExchangeSub channel and market match Identifier impls
                    assert_eq!(entry.exchange_sub.channel, expected_channel);
                    assert_eq!(entry.exchange_sub.market, expected_market);

                    // Verify SubscriptionId is "{channel}|{market}"
                    let expected_sub_id = SubscriptionId::from(format!(
                        "{}|{}",
                        expected_channel.as_ref(),
                        expected_market.as_ref()
                    ));
                    assert_eq!(entry.id, expected_sub_id);

                    // Verify the instrument key is the MarketDataInstrument itself
                    assert_eq!(
                        entry.instrument_key,
                        MarketDataInstrument::from((
                            "btc",
                            "usdt",
                            MarketDataInstrumentKind::Spot
                        ))
                    );
                }
                _ => panic!("expected Command::Subscribe, got Command::Unsubscribe"),
            }
        }

        #[test]
        fn test_typed_handle_unsubscribe_sends_command() {
            let (handle, mut rx) = typed_handle();

            let sub_ids = vec![
                SubscriptionId::from("@trade|BTCUSDT"),
                SubscriptionId::from("@trade|ETHUSDT"),
            ];

            handle.unsubscribe(sub_ids.clone()).unwrap();

            let cmd = rx.try_recv().expect("expected Command on receiver");
            match cmd {
                Command::Unsubscribe { subscription_ids } => {
                    assert_eq!(subscription_ids.len(), 2);
                    assert_eq!(subscription_ids[0], sub_ids[0]);
                    assert_eq!(subscription_ids[1], sub_ids[1]);
                }
                _ => panic!("expected Command::Unsubscribe, got Command::Subscribe"),
            }
        }

        #[test]
        fn test_typed_handle_subscribe_returns_sub_ids() {
            let (handle, _rx) = typed_handle();

            let sub = btc_usdt_sub();

            // Derive the expected SubscriptionId using the same path as TypedHandle:
            // ExchangeSub::new(&sub) -> exchange_sub.id()
            let exchange_sub: ExchangeSub<BinanceChannel, BinanceMarket> =
                ExchangeSub::new(&sub);
            let expected_id: SubscriptionId = exchange_sub.id();

            let ids = handle.subscribe(vec![sub]).unwrap();

            assert_eq!(ids.len(), 1);
            assert_eq!(ids[0], expected_id);
        }

        #[test]
        fn test_typed_handle_subscribe_multiple_returns_correct_ids() {
            let (handle, _rx) = typed_handle();

            let sub_btc = btc_usdt_sub();
            let sub_eth = Subscription::new(
                BinanceSpot::default(),
                MarketDataInstrument::from(("eth", "usdt", MarketDataInstrumentKind::Spot)),
                PublicTrades,
            );

            let expected_btc_id =
                ExchangeSub::<BinanceChannel, BinanceMarket>::new(&sub_btc).id();
            let expected_eth_id =
                ExchangeSub::<BinanceChannel, BinanceMarket>::new(&sub_eth).id();

            let ids = handle.subscribe(vec![sub_btc, sub_eth]).unwrap();

            assert_eq!(ids.len(), 2);
            assert_eq!(ids[0], expected_btc_id);
            assert_eq!(ids[1], expected_eth_id);

            // Verify the two ids are distinct
            assert_ne!(ids[0], ids[1]);
        }

        #[test]
        fn test_typed_handle_closed_channel() {
            let (handle, rx) = typed_handle();
            drop(rx);

            let sub = btc_usdt_sub();
            let result = handle.subscribe(vec![sub]);

            assert!(
                result.is_err(),
                "subscribe should fail when command channel is closed"
            );
        }

        #[test]
        fn test_typed_handle_unsubscribe_closed_channel() {
            let (handle, rx) = typed_handle();
            drop(rx);

            let result =
                handle.unsubscribe(vec![SubscriptionId::from("@trade|BTCUSDT")]);

            assert!(
                result.is_err(),
                "unsubscribe should fail when command channel is closed"
            );
        }
    }
}
