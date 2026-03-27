use crate::{
    Identifier,
    books::OrderBook,
    error::DataError,
    event::MarketEvent,
    exchange::{
        Connector, ExchangeServer,
        okx::Okx,
    },
    subscription::{
        Map,
        book::{OrderBookEvent, OrderBooksL2},
    },
    transformer::ExchangeTransformer,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use chrono::Utc;
use derive_more::Constructor;
use tokio::sync::mpsc::UnboundedSender;
use tracing::debug;

use super::{OkxBookAction, OkxBookMessage};

#[derive(Debug, Constructor)]
pub struct OkxOrderBookL2Meta<InstrumentKey> {
    pub key: InstrumentKey,
    pub sequencer: Option<OkxOrderBookL2Sequencer>,
}

#[derive(Debug)]
pub struct OkxOrderBooksL2Transformer<InstrumentKey> {
    exchange_id: ExchangeId,
    instrument_map: Map<OkxOrderBookL2Meta<InstrumentKey>>,
}

impl<InstrumentKey, Server> ExchangeTransformer<Okx<Server>, InstrumentKey, OrderBooksL2>
    for OkxOrderBooksL2Transformer<InstrumentKey>
where
    InstrumentKey: Clone + PartialEq + Send + Sync,
    Server: ExchangeServer,
{
    /// Initialise with REST snapshots to seed sequencers.
    fn init(
        instrument_map: Map<InstrumentKey>,
        initial_snapshots: &[MarketEvent<InstrumentKey, OrderBookEvent>],
        _: UnboundedSender<WsMessage>,
    ) -> impl std::future::Future<Output = Result<Self, DataError>> + Send {
        let instrument_map = instrument_map
            .0
            .into_iter()
            .map(|(sub_id, instrument_key)| {
                let sequencer = initial_snapshots
                    .iter()
                    .find(|s| s.instrument == instrument_key)
                    .and_then(|s| match &s.kind {
                        OrderBookEvent::Snapshot(ob) => Some(OkxOrderBookL2Sequencer {
                            last_seq_id: ob.sequence(),
                        }),
                        _ => None,
                    });

                (sub_id, OkxOrderBookL2Meta::new(instrument_key, sequencer))
            })
            .collect();

        async move { Ok(Self { exchange_id: Okx::<Server>::ID, instrument_map }) }
    }

    fn insert_map_entries(&mut self, entries: Vec<(SubscriptionId, InstrumentKey)>) {
        for (id, key) in entries {
            if self.instrument_map.0.contains_key(&id) {
                tracing::debug!(%id, "skipping insert_map_entry: already exists");
                continue;
            }
            self.instrument_map
                .insert(id, OkxOrderBookL2Meta::new(key, None));
        }
    }

    fn remove_map_entries(&mut self, subscription_ids: &[SubscriptionId]) {
        for id in subscription_ids {
            self.instrument_map.remove(id);
        }
    }
}

impl<InstrumentKey> Transformer for OkxOrderBooksL2Transformer<InstrumentKey>
where
    InstrumentKey: Clone,
{
    type Error = DataError;
    type Input = OkxBookMessage;
    type Output = MarketEvent<InstrumentKey, OrderBookEvent>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let subscription_id = match input.id() {
            Some(subscription_id) => subscription_id,
            None => return vec![],
        };

        let instrument = match self.instrument_map.find_mut(&subscription_id) {
            Ok(instrument) => instrument,
            Err(unidentifiable) => return vec![Err(DataError::from(unidentifiable))],
        };

        // Process each data entry (typically just one)
        let mut results = Vec::new();

        for update in input.data {
            match input.action {
                OkxBookAction::Snapshot => {
                    // Initialise sequencer from snapshot
                    instrument.sequencer.replace(OkxOrderBookL2Sequencer {
                        last_seq_id: update.seq_id,
                    });

                    let orderbook = OrderBook::new(
                        update.seq_id,
                        Some(update.time),
                        update.bids,
                        update.asks,
                    );
                    results.push(Ok(MarketEvent {
                        time_exchange: update.time,
                        time_received: Utc::now(),
                        exchange: self.exchange_id,
                        instrument: instrument.key.clone(),
                        kind: OrderBookEvent::Snapshot(orderbook),
                    }));
                }
                OkxBookAction::Update => {
                    let Some(sequencer) = &mut instrument.sequencer else {
                        debug!("OKX update received before snapshot");
                        continue;
                    };

                    // Validate sequence: prevSeqId must match our last_seq_id
                    if update.prev_seq_id >= 0
                        && update.prev_seq_id as u64 != sequencer.last_seq_id
                    {
                        results.push(Err(DataError::InvalidSequence {
                            prev_last_update_id: sequencer.last_seq_id,
                            first_update_id: update.seq_id,
                        }));
                        continue;
                    }

                    sequencer.last_seq_id = update.seq_id;

                    let orderbook = OrderBook::new(
                        update.seq_id,
                        Some(update.time),
                        update.bids,
                        update.asks,
                    );
                    results.push(Ok(MarketEvent {
                        time_exchange: update.time,
                        time_received: Utc::now(),
                        exchange: self.exchange_id,
                        instrument: instrument.key.clone(),
                        kind: OrderBookEvent::Update(orderbook),
                    }));
                }
            }
        }

        results
    }
}

#[derive(Debug)]
pub struct OkxOrderBookL2Sequencer {
    pub last_seq_id: u64,
}
