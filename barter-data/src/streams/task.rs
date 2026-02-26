use crate::{
    Identifier, SnapshotFetcher, distribute_messages_to_exchange,
    error::DataError,
    event::MarketEvent,
    exchange::{Connector, subscription::ExchangeSub},
    instrument::InstrumentData,
    process_buffered_events, schedule_pings_to_exchange,
    streams::{consumer::MarketStreamResult, handle::{Command, SubEntry}, reconnect},
    subscriber::{Subscribed, Subscriber},
    subscription::{Subscription, SubscriptionKind},
    transformer::ExchangeTransformer,
};
use barter_integration::{
    protocol::{
        StreamParser,
        websocket::{WsError, WsMessage},
    },
    subscription::SubscriptionId,
};
use fnv::FnvHashMap;
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

/// Tracks net-active dynamic subscriptions for reconnect replay.
/// Replaces the unbounded `replay_commands: Vec<Command>` log.
struct ActiveSubs<Channel, Market, InstrumentKey> {
    map: FnvHashMap<SubscriptionId, (ExchangeSub<Channel, Market>, InstrumentKey)>,
}

impl<Channel, Market, InstrumentKey> ActiveSubs<Channel, Market, InstrumentKey>
where
    Channel: Clone,
    Market: Clone,
    InstrumentKey: Clone,
{
    fn new() -> Self {
        Self {
            map: FnvHashMap::default(),
        }
    }

    /// Insert entries. Returns the ExchangeSubs for Connector::requests().
    fn subscribe(
        &mut self,
        entries: Vec<SubEntry<Channel, Market, InstrumentKey>>,
    ) -> Vec<ExchangeSub<Channel, Market>> {
        entries
            .into_iter()
            .map(|entry| {
                let sub = entry.exchange_sub.clone();
                self.map.insert(entry.id, (entry.exchange_sub, entry.instrument_key));
                sub
            })
            .collect()
    }

    /// Remove entries. Returns the (SubscriptionId, ExchangeSub) pairs that were
    /// actually removed, so callers know exactly which IDs were present.
    fn unsubscribe(
        &mut self,
        subscription_ids: &[SubscriptionId],
    ) -> Vec<(SubscriptionId, ExchangeSub<Channel, Market>)> {
        subscription_ids
            .iter()
            .filter_map(|id| self.map.remove(id).map(|(sub, _)| (id.clone(), sub)))
            .collect()
    }

    /// All ExchangeSubs for reconnect replay via Connector::requests().
    ///
    /// Clone required: HashMap values aren't contiguous in memory, so we collect
    /// cloned ExchangeSubs into a Vec to pass as a `&[ExchangeSub]` slice.
    fn all_exchange_subs(&self) -> Vec<ExchangeSub<Channel, Market>> {
        self.map.values().map(|(sub, _)| sub.clone()).collect()
    }

    /// All (SubscriptionId, InstrumentKey) pairs for rebuilding the transformer map on reconnect.
    ///
    /// Clone required: insert_map_entries() takes owned Vec<(SubscriptionId, InstrumentKey)>,
    /// and the originals must remain in the HashMap for future reconnects.
    fn instrument_entries(&self) -> Vec<(SubscriptionId, InstrumentKey)> {
        self.map
            .iter()
            .map(|(id, (_, key))| (id.clone(), key.clone()))
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Spawned connection task that owns the WebSocket stream and transformer.
///
/// Outer loop handles reconnection with exponential backoff.
/// Inner loop uses `tokio::select!` to multiplex command processing and frame reading.
pub(crate) async fn connection_task<Exchange, Instrument, Kind, TransformerT, Parser, SnapFetcher>(
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
    mut command_rx: mpsc::UnboundedReceiver<Command<Exchange::Channel, Exchange::Market, Instrument::Key>>,
    event_tx: mpsc::UnboundedSender<MarketStreamResult<Instrument::Key, Kind::Event>>,
    policy: crate::streams::reconnect::stream::ReconnectionBackoffPolicy,
    init_result_tx: oneshot::Sender<Result<(), DataError>>,
) where
    Exchange: Connector + Send + Sync,
    Instrument: InstrumentData,
    Instrument::Key: Clone + Send + Sync,
    Kind: SubscriptionKind + Send + Sync,
    Kind::Event: Send,
    TransformerT: ExchangeTransformer<Exchange, Instrument::Key, Kind> + Send,
    Parser: StreamParser<TransformerT::Input, Message = WsMessage, Error = WsError> + Send,
    SnapFetcher: SnapshotFetcher<Exchange, Kind>,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    let exchange = Exchange::ID;
    let mut init_result_tx = Some(init_result_tx);
    let mut backoff_ms = policy.backoff_ms_initial;
    let mut active_subs = ActiveSubs::new();

    loop {
        // === Connect phase ===
        let connect_result =
            connect_and_init::<Exchange, Instrument, Kind, TransformerT, Parser, SnapFetcher>(
                &subscriptions,
            )
            .await;

        let (mut ws_stream, mut transformer, ws_sink_tx, buffer) = match connect_result {
            Ok(result) => {
                // Reset backoff on successful connection
                backoff_ms = policy.backoff_ms_initial;

                // Signal first connection success
                if let Some(tx) = init_result_tx.take() {
                    let _ = tx.send(Ok(()));
                }
                result
            }
            Err(err) => {
                // Signal first connection failure and exit
                if let Some(tx) = init_result_tx.take() {
                    let _ = tx.send(Err(err));
                    return;
                }

                // Reconnection failed -- backoff and retry
                warn!(%exchange, %err, backoff_ms, "reconnection failed, backing off");
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = std::cmp::min(
                    backoff_ms * u64::from(policy.backoff_multiplier),
                    policy.backoff_ms_max,
                );
                continue;
            }
        };

        // === Forward buffered events ===
        for event in buffer {
            if event_tx.send(reconnect::Event::Item(event)).is_err() {
                return; // event_rx dropped, shut down
            }
        }

        // === Drain commands buffered while disconnected ===
        while let Ok(command) = command_rx.try_recv() {
            match command {
                Command::Subscribe { entries } => {
                    active_subs.subscribe(entries);
                }
                Command::Unsubscribe { subscription_ids } => {
                    active_subs.unsubscribe(&subscription_ids);
                }
            }
        }

        // === Replay dynamic subscriptions on reconnect ===
        if !active_subs.is_empty() {
            // Rebuild transformer map entries
            transformer.insert_map_entries(active_subs.instrument_entries());

            // Re-subscribe via Connector::requests()
            let ws_msgs = Exchange::requests(&active_subs.all_exchange_subs());
            for msg in ws_msgs {
                if ws_sink_tx.send(msg).is_err() {
                    warn!(%exchange, "ws_sink closed during reconnect replay, will reconnect");
                    break;
                }
            }
        }

        // === Inner select! loop ===
        'inner: loop {
            tokio::select! {
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(command) => {
                            match command {
                                Command::Subscribe { entries } => {
                                    // 1. Extract (id, key) pairs before consuming entries
                                    let map_entries: Vec<_> = entries
                                        .iter()
                                        .map(|e| (e.id.clone(), e.instrument_key.clone()))
                                        .collect();

                                    // 2. Insert into active_subs (consumes entries, returns ExchangeSubs)
                                    let exchange_subs = active_subs.subscribe(entries);

                                    // 3. Update transformer map
                                    transformer.insert_map_entries(map_entries);

                                    // 4. Generate and send WS messages
                                    let ws_msgs = Exchange::requests(&exchange_subs);
                                    for msg in ws_msgs {
                                        if ws_sink_tx.send(msg).is_err() {
                                            break 'inner; // ws_sink closed -> reconnect
                                        }
                                    }
                                }
                                Command::Unsubscribe { subscription_ids } => {
                                    // 1. Remove from active_subs (returns actually-removed pairs)
                                    let removed = active_subs.unsubscribe(&subscription_ids);
                                    let (removed_ids, exchange_subs): (Vec<_>, Vec<_>) =
                                        removed.into_iter().unzip();

                                    // 2. Update transformer map with only actually-removed IDs
                                    transformer.remove_map_entries(&removed_ids);

                                    // 3. Generate and send unsubscribe WS messages
                                    let ws_msgs = Exchange::unsubscribe_requests(&exchange_subs);
                                    for msg in ws_msgs {
                                        if ws_sink_tx.send(msg).is_err() {
                                            break 'inner;
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            // command_tx dropped -- handle dropped, shut down task
                            return;
                        }
                    }
                }
                frame = ws_stream.next() => {
                    match frame {
                        Some(Ok(msg)) => {
                            match Parser::parse(Ok(msg)) {
                                Some(Ok(input)) => {
                                    for event in transformer.transform(input) {
                                        if event_tx.send(reconnect::Event::Item(event)).is_err() {
                                            return; // event_rx dropped
                                        }
                                    }
                                }
                                Some(Err(err)) => {
                                    if event_tx
                                        .send(reconnect::Event::Item(Err(DataError::from(err))))
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                None => continue, // skip pings/pongs/control frames
                            }
                        }
                        Some(Err(_ws_err)) => {
                            break; // WS error -> reconnect
                        }
                        None => {
                            break; // WS closed -> reconnect
                        }
                    }
                }
            }
        }

        // Inner loop exited due to WebSocket disconnect -- reconnect
        info!(%exchange, "WebSocket disconnected, reconnecting...");
        if event_tx
            .send(reconnect::Event::Reconnecting(exchange))
            .is_err()
        {
            return;
        }
    }
}

/// Connect to the exchange, subscribe, split WebSocket, init transformer.
/// Returns (ws_stream, transformer, ws_sink_tx, buffered_events).
async fn connect_and_init<Exchange, Instrument, Kind, TransformerT, Parser, SnapFetcher>(
    subscriptions: &[Subscription<Exchange, Instrument, Kind>],
) -> Result<
    (
        barter_integration::protocol::websocket::WsStream,
        TransformerT,
        mpsc::UnboundedSender<WsMessage>,
        Vec<Result<MarketEvent<Instrument::Key, Kind::Event>, DataError>>,
    ),
    DataError,
>
where
    Exchange: Connector + Send + Sync,
    Instrument: InstrumentData,
    Instrument::Key: Clone + Send + Sync,
    Kind: SubscriptionKind + Send + Sync,
    Kind::Event: Send,
    TransformerT: ExchangeTransformer<Exchange, Instrument::Key, Kind> + Send,
    Parser: StreamParser<TransformerT::Input, Message = WsMessage, Error = WsError> + Send,
    SnapFetcher: SnapshotFetcher<Exchange, Kind>,
    Subscription<Exchange, Instrument, Kind>:
        Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
{
    let exchange = Exchange::ID;

    // Connect & subscribe
    let Subscribed {
        websocket,
        map: instrument_map,
        buffered_websocket_events,
    } = Exchange::Subscriber::subscribe(subscriptions).await?;

    // Fetch any required initial MarketEvent snapshots
    let initial_snapshots = SnapFetcher::fetch_snapshots(subscriptions).await?;

    // Split WebSocket into WsStream & WsSink
    let (ws_sink, ws_stream) = websocket.split();

    // Spawn task to distribute messages to exchange via WsSink
    let (ws_sink_tx, ws_sink_rx) = mpsc::unbounded_channel();
    tokio::spawn(distribute_messages_to_exchange(
        exchange, ws_sink, ws_sink_rx,
    ));

    // Spawn optional custom application-level pings
    if let Some(ping_interval) = Exchange::ping_interval() {
        tokio::spawn(schedule_pings_to_exchange(
            exchange,
            ws_sink_tx.clone(),
            ping_interval,
        ));
    }

    // Initialise Transformer
    let mut transformer =
        TransformerT::init(instrument_map, &initial_snapshots, ws_sink_tx.clone()).await?;

    // Process buffered events
    let buffer: Vec<_> = process_buffered_events::<Parser, TransformerT>(
        &mut transformer,
        buffered_websocket_events,
    )
    .into_iter()
    .collect();

    // Add initial snapshots to buffer
    let mut all_events = buffer;
    all_events.extend(initial_snapshots.into_iter().map(Ok));

    Ok((ws_stream, transformer, ws_sink_tx, all_events))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::subscription::ExchangeSub;
    use crate::streams::handle::SubEntry;

    fn test_sub(channel: &str, market: &str) -> ExchangeSub<String, String> {
        ExchangeSub {
            channel: channel.to_string(),
            market: market.to_string(),
        }
    }

    #[test]
    fn test_active_subs_subscribe_insert() {
        let mut subs = ActiveSubs::<String, String, String>::new();

        let entries = vec![
            SubEntry { id: SubscriptionId::from("c1|m1"), exchange_sub: test_sub("c1", "m1"), instrument_key: "BTC".to_string() },
            SubEntry { id: SubscriptionId::from("c2|m2"), exchange_sub: test_sub("c2", "m2"), instrument_key: "ETH".to_string() },
        ];

        let exchange_subs = subs.subscribe(entries);
        assert_eq!(exchange_subs.len(), 2);
        assert_eq!(subs.map.len(), 2);
    }

    #[test]
    fn test_active_subs_unsubscribe_remove() {
        let mut subs = ActiveSubs::<String, String, String>::new();

        let entries = vec![
            SubEntry { id: SubscriptionId::from("c1|m1"), exchange_sub: test_sub("c1", "m1"), instrument_key: "BTC".to_string() },
        ];
        subs.subscribe(entries);

        let removed = subs.unsubscribe(&[SubscriptionId::from("c1|m1")]);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].0, SubscriptionId::from("c1|m1"));
        assert!(subs.is_empty());
    }

    #[test]
    fn test_active_subs_net_zero() {
        let mut subs = ActiveSubs::<String, String, String>::new();

        subs.subscribe(vec![
            SubEntry { id: SubscriptionId::from("c1|m1"), exchange_sub: test_sub("c1", "m1"), instrument_key: "BTC".to_string() },
        ]);
        subs.unsubscribe(&[SubscriptionId::from("c1|m1")]);

        assert!(subs.is_empty());
        assert!(subs.all_exchange_subs().is_empty());
        assert!(subs.instrument_entries().is_empty());
    }

    #[test]
    fn test_active_subs_partial_unsubscribe() {
        let mut subs = ActiveSubs::<String, String, String>::new();

        subs.subscribe(vec![
            SubEntry { id: SubscriptionId::from("c1|m1"), exchange_sub: test_sub("c1", "m1"), instrument_key: "BTC".to_string() },
            SubEntry { id: SubscriptionId::from("c2|m2"), exchange_sub: test_sub("c2", "m2"), instrument_key: "ETH".to_string() },
        ]);
        subs.unsubscribe(&[SubscriptionId::from("c1|m1")]);

        assert_eq!(subs.map.len(), 1);
        let all_subs = subs.all_exchange_subs();
        assert_eq!(all_subs.len(), 1);
        assert_eq!(all_subs[0].channel, "c2");
        assert_eq!(all_subs[0].market, "m2");
    }

    #[test]
    fn test_active_subs_instrument_entries() {
        let mut subs = ActiveSubs::<String, String, String>::new();

        subs.subscribe(vec![
            SubEntry { id: SubscriptionId::from("c1|m1"), exchange_sub: test_sub("c1", "m1"), instrument_key: "BTC".to_string() },
            SubEntry { id: SubscriptionId::from("c2|m2"), exchange_sub: test_sub("c2", "m2"), instrument_key: "ETH".to_string() },
        ]);

        let entries = subs.instrument_entries();
        assert_eq!(entries.len(), 2);

        // Both entries should be present (order not guaranteed from HashMap)
        let keys: Vec<_> = entries.iter().map(|(_, key)| key.as_str()).collect();
        assert!(keys.contains(&"BTC"));
        assert!(keys.contains(&"ETH"));
    }

    #[test]
    fn test_active_subs_unsubscribe_nonexistent() {
        let mut subs = ActiveSubs::<String, String, String>::new();

        let removed = subs.unsubscribe(&[SubscriptionId::from("nonexistent")]);
        assert!(removed.is_empty());
    }
}
