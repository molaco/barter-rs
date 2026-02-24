use super::ExchangeTransformer;
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
};
use async_trait::async_trait;
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::Deserialize;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;

/// Standard generic stateless [`ExchangeTransformer`] to translate exchange specific types into
/// normalised Barter types. Often used with
/// [`PublicTrades`](crate::subscription::trade::PublicTrades) or
/// [`OrderBooksL1`](crate::subscription::book::OrderBooksL1) streams.
#[derive(Clone, Debug)]
pub struct StatelessTransformer<Exchange, InstrumentKey, Kind, Input> {
    instrument_map: Arc<RwLock<Map<InstrumentKey>>>,
    ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    phantom: PhantomData<(Exchange, Kind, Input)>,
}

#[async_trait]
impl<Exchange, InstrumentKey, Kind, Input> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for StatelessTransformer<Exchange, InstrumentKey, Kind, Input>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Input: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, Input)>,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self {
            instrument_map: Arc::new(RwLock::new(instrument_map)),
            ws_sink_tx,
            phantom: PhantomData,
        })
    }

    fn shared_instrument_map(&self) -> Option<Arc<RwLock<Map<InstrumentKey>>>> {
        Some(self.instrument_map.clone())
    }

    fn ws_sink_tx(&self) -> Option<mpsc::UnboundedSender<WsMessage>> {
        Some(self.ws_sink_tx.clone())
    }

    fn set_shared_instrument_map(&mut self, map: Arc<RwLock<Map<InstrumentKey>>>) {
        self.instrument_map = map;
    }
}

impl<Exchange, InstrumentKey, Kind, Input> Transformer
    for StatelessTransformer<Exchange, InstrumentKey, Kind, Input>
where
    Exchange: Connector,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind,
    Input: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, Input)>,
{
    type Error = DataError;
    type Input = Input;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        // Determine if the message has an identifiable SubscriptionId
        let subscription_id = match input.id() {
            Some(subscription_id) => subscription_id,
            None => return vec![],
        };

        // Acquire read lock on the shared instrument map
        let instrument_map = self
            .instrument_map
            .read()
            .expect("instrument_map RwLock poisoned");

        // Find Instrument associated with Input and transform
        match instrument_map.find(&subscription_id) {
            Ok(instrument) => {
                MarketIter::<InstrumentKey, Kind::Event>::from((
                    Exchange::ID,
                    instrument.clone(),
                    input,
                ))
                .0
            }
            Err(unidentifiable) => vec![Err(DataError::from(unidentifiable))],
        }
    }
}
