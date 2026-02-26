use super::ExchangeTransformer;
use crate::{
    Identifier,
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};
use serde::Deserialize;
use std::marker::PhantomData;
use tokio::sync::mpsc;

/// Standard generic stateless [`ExchangeTransformer`] to translate exchange specific types into
/// normalised Barter types. Often used with
/// [`PublicTrades`](crate::subscription::trade::PublicTrades) or
/// [`OrderBooksL1`](crate::subscription::book::OrderBooksL1) streams.
#[derive(Debug)]
pub struct StatelessTransformer<Exchange, InstrumentKey, Kind, Input> {
    instrument_map: Map<InstrumentKey>,
    phantom: PhantomData<(Exchange, Kind, Input)>,
}

impl<Exchange, InstrumentKey, Kind, Input> ExchangeTransformer<Exchange, InstrumentKey, Kind>
    for StatelessTransformer<Exchange, InstrumentKey, Kind, Input>
where
    Exchange: Connector + Send,
    InstrumentKey: Clone + Send + Sync,
    Kind: SubscriptionKind + Send,
    Input: Identifier<Option<SubscriptionId>> + for<'de> Deserialize<'de>,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, Input)>,
{
    fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        _ws_sink_tx: mpsc::UnboundedSender<WsMessage>,
    ) -> impl std::future::Future<Output = Result<Self, DataError>> + Send {
        async move {
            Ok(Self {
                instrument_map,
                phantom: PhantomData,
            })
        }
    }

    fn insert_map_entries(&mut self, entries: Vec<(SubscriptionId, InstrumentKey)>) {
        for (id, key) in entries {
            self.instrument_map.insert(id, key);
        }
    }

    fn remove_map_entries(&mut self, subscription_ids: &[SubscriptionId]) {
        for id in subscription_ids {
            self.instrument_map.remove(id);
        }
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
        let subscription_id = match input.id() {
            Some(subscription_id) => subscription_id,
            None => return vec![],
        };

        match self.instrument_map.find(&subscription_id) {
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
