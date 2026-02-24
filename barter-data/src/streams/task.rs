use crate::{
    Identifier,
    error::DataError,
    event::MarketEvent,
    exchange::Connector,
    instrument::InstrumentData,
    process_buffered_events,
    distribute_messages_to_exchange,
    schedule_pings_to_exchange,
    SnapshotFetcher,
    streams::{
        consumer::MarketStreamResult,
        handle::{Command, DynamicBatch},
        reconnect,
    },
    subscriber::{Subscribed, Subscriber},
    subscription::{Subscription, SubscriptionKind},
    transformer::ExchangeTransformer,
};
use barter_integration::protocol::{
    StreamParser,
    websocket::{WsError, WsMessage},
};
use futures::StreamExt;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot};
use tracing::{info, warn};

/// Spawned connection task that owns the WebSocket stream and transformer.
///
/// Outer loop handles reconnection with exponential backoff.
/// Inner loop uses `tokio::select!` to multiplex command processing and frame reading.
pub(crate) async fn connection_task<Exchange, Instrument, Kind, TransformerT, Parser, SnapFetcher>(
    subscriptions: Vec<Subscription<Exchange, Instrument, Kind>>,
    mut command_rx: mpsc::UnboundedReceiver<Command<Instrument::Key>>,
    event_tx: mpsc::UnboundedSender<MarketStreamResult<Instrument::Key, Kind::Event>>,
    dynamic_batches: Arc<Mutex<Vec<DynamicBatch<Instrument::Key>>>>,
    policy: crate::streams::reconnect::stream::ReconnectionBackoffPolicy,
    init_result_tx: oneshot::Sender<Result<(), DataError>>,
)
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
    let mut init_result_tx = Some(init_result_tx);
    let mut backoff_ms = policy.backoff_ms_initial;

    loop {
        // === Connect phase ===
        let connect_result =
            connect_and_init::<Exchange, Instrument, Kind, TransformerT, Parser, SnapFetcher>(
                &subscriptions,
            )
            .await;

        let (ws_stream, mut transformer, ws_sink_tx, buffer) = match connect_result {
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
                    backoff_ms * policy.backoff_multiplier as u64,
                    policy.backoff_ms_max,
                );
                continue;
            }
        };

        // === Forward buffered events ===
        for event in buffer {
            if event_tx
                .send(reconnect::Event::Item(event))
                .is_err()
            {
                return; // event_rx dropped, shut down
            }
        }

        // === Drain any commands buffered while disconnected ===
        while let Ok(command) = command_rx.try_recv() {
            let ws_messages = transformer.apply_command(command);
            for msg in ws_messages {
                let _ = ws_sink_tx.send(msg);
            }
        }

        // === Replay dynamic batches (reconnection resubscribe) ===
        {
            let batches = dynamic_batches
                .lock()
                .expect("dynamic_batches mutex poisoned");
            for batch in batches.iter() {
                // Apply entries to transformer map
                let subscribe_command = Command::Subscribe {
                    entries: batch.entries.clone(),
                    ws_messages: batch.subscribe_messages.clone(),
                };
                let ws_messages = transformer.apply_command(subscribe_command);
                for msg in ws_messages {
                    let _ = ws_sink_tx.send(msg);
                }
            }
        }

        // === Inner select! loop ===
        let mut ws_stream = ws_stream;
        loop {
            tokio::select! {
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(command) => {
                            let ws_messages = transformer.apply_command(command);
                            for msg in ws_messages {
                                if ws_sink_tx.send(msg).is_err() {
                                    break; // sink closed
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

        // Backoff before reconnecting
        tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
        backoff_ms = std::cmp::min(
            backoff_ms * policy.backoff_multiplier as u64,
            policy.backoff_ms_max,
        );
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
        exchange,
        ws_sink,
        ws_sink_rx,
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
