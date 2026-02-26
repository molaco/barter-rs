use crate::{
    Identifier,
    error::DataError,
    exchange::{
        Connector,
        binance::{futures::BinanceFuturesUsd, market::BinanceMarket, spot::BinanceSpot},
        bitfinex::{Bitfinex, market::BitfinexMarket},
        bitmex::{Bitmex, market::BitmexMarket},
        bybit::{futures::BybitPerpetualsUsd, market::BybitMarket, spot::BybitSpot},
        coinbase::{Coinbase, market::CoinbaseMarket},
        gateio::{
            future::{GateioFuturesBtc, GateioFuturesUsd},
            market::GateioMarket,
            option::GateioOptions,
            perpetual::{GateioPerpetualsBtc, GateioPerpetualsUsd},
            spot::GateioSpot,
        },
        hyperliquid::{Hyperliquid, market::HyperliquidMarket},
        kraken::{Kraken, market::KrakenMarket},
        okx::{Okx, market::OkxMarket},
    },
    instrument::InstrumentData,
    streams::{
        consumer::{MarketStreamResult, STREAM_RECONNECTION_POLICY, init_market_stream},
        handle::DynHandle,
        reconnect::stream::ReconnectingStream,
    },
    subscription::{
        SubKind, Subscription, SubscriptionKind,
        book::{OrderBookEvent, OrderBookL1, OrderBooksL1, OrderBooksL2},
        candle::{Candle, Candles},
        liquidation::{Liquidation, Liquidations},
        trade::{PublicTrade, PublicTrades},
    },
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::{
    Validator,
    channel::{UnboundedRx, UnboundedTx, mpsc_unbounded},
    error::SocketError,
};
use fnv::FnvHashMap;
use futures::{Stream, stream::SelectAll};
use futures_util::{StreamExt, future::try_join_all};
use itertools::Itertools;
use std::{
    fmt::{Debug, Display},
    sync::Arc,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use async_trait::async_trait;
use std::marker::PhantomData;
use vecmap::VecMap;

pub mod indexed;

/// Key for looking up type-erased handles in DynamicStreamHandles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct HandleKey {
    exchange: ExchangeId,
    sub_kind: SubKind,
}

/// Discriminant-only version of [`SubKind`] for registry lookup keys.
/// Strips the [`Interval`] data from [`SubKind::Candles`] so that a single
/// factory handles all candle intervals for a given exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SubKindVariant {
    PublicTrades,
    OrderBooksL1,
    OrderBooksL2,
    OrderBooksL3,
    Liquidations,
    Candles,
}

impl From<SubKind> for SubKindVariant {
    fn from(sk: SubKind) -> Self {
        match sk {
            SubKind::PublicTrades => Self::PublicTrades,
            SubKind::OrderBooksL1 => Self::OrderBooksL1,
            SubKind::OrderBooksL2 => Self::OrderBooksL2,
            SubKind::OrderBooksL3 => Self::OrderBooksL3,
            SubKind::Liquidations => Self::Liquidations,
            SubKind::Candles(_) => Self::Candles,
        }
    }
}

/// Holds type-erased `TypedHandle`s for runtime subscribe/unsubscribe.
///
/// Each handle is keyed by `(ExchangeId, SubKind)` and stored as `Box<dyn DynHandle<Instrument>>`.
/// No turbofish or downcast needed: the `DynHandle` trait converts erased subscriptions
/// to concrete types internally.
#[derive(Clone)]
pub struct DynamicStreamHandles<Instrument> {
    handles: Arc<FnvHashMap<HandleKey, Box<dyn DynHandle<Instrument>>>>,
}

impl<Instrument> std::fmt::Debug for DynamicStreamHandles<Instrument> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicStreamHandles")
            .field("num_handles", &self.handles.len())
            .finish()
    }
}

impl<Instrument> DynamicStreamHandles<Instrument>
where
    Instrument: InstrumentData + Clone + 'static,
    Instrument::Key: Clone + Send + Sync + 'static,
{
    /// Subscribe to new instruments on a live connection.
    ///
    /// Derives the `(ExchangeId, SubKind)` key from the first subscription.
    pub fn subscribe(
        &self,
        subscriptions: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
    ) -> Result<Vec<barter_integration::subscription::SubscriptionId>, DataError> {
        let (exchange, sub_kind) = subscriptions
            .first()
            .map(|s| (s.exchange, s.kind))
            .ok_or(DataError::SubscriptionsEmpty)?;
        let key = HandleKey {
            exchange,
            sub_kind,
        };
        let handle = self
            .handles
            .get(&key)
            .ok_or(DataError::NoConnection {
                exchange,
                sub_kind,
            })?;
        handle.subscribe_erased(subscriptions)
    }

    /// Unsubscribe from instruments on a live connection.
    pub fn unsubscribe(
        &self,
        exchange: ExchangeId,
        sub_kind: SubKind,
        subscription_ids: Vec<barter_integration::subscription::SubscriptionId>,
    ) -> Result<(), DataError> {
        let key = HandleKey {
            exchange,
            sub_kind,
        };
        let handle = self
            .handles
            .get(&key)
            .ok_or(DataError::NoConnection {
                exchange,
                sub_kind,
            })?;
        handle.unsubscribe_erased(subscription_ids)
    }
}

/// Declarative macro that builds a [`StreamRegistry`] from a table of
/// `Exchange => [Kind1, Kind2, ...]` entries.
///
/// Each entry calls `registry.register::<Exchange, Kind>()`, which inserts a
/// [`TypedStreamFactory`] keyed by `(Exchange::ID, Kind::VARIANT)`.
macro_rules! register_streams {
    ($($exchange_ty:ty => [$($kind_ty:ty),* $(,)?]),* $(,)?) => {{
        let mut registry = StreamRegistry::new();
        $($(
            registry.register::<$exchange_ty, $kind_ty>();
        )*)*
        registry
    }};
}

#[derive(Debug)]
pub struct DynamicStreams<InstrumentKey> {
    pub trades:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    pub l1s:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    pub l2s: VecMap<
        ExchangeId,
        UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>,
    >,
    pub liquidations:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>>,
    pub candles:
        VecMap<ExchangeId, UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> DynamicStreams<InstrumentKey> {
    /// Initialise a set of `Streams` by providing one or more [`Subscription`] batches.
    ///
    /// Each batch (ie/ `impl Iterator<Item = Subscription>`) will initialise at-least-one
    /// WebSocket `Stream` under the hood. If the batch contains more-than-one [`ExchangeId`] and/or
    /// [`SubKind`], it will be further split under the hood for compile-time reasons.
    ///
    /// ## Examples
    /// Please see barter-data-rs/examples/dynamic_multi_stream_multi_exchange.rs for a
    /// comprehensive example of how to use this market data stream initialiser.
    pub async fn init<SubBatchIter, SubIter, Sub, Instrument>(
        subscription_batches: SubBatchIter,
    ) -> Result<(Self, DynamicStreamHandles<Instrument>), DataError>
    where
        SubBatchIter: IntoIterator<Item = SubIter>,
        SubIter: IntoIterator<Item = Sub>,
        Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
        Instrument: InstrumentData<Key = InstrumentKey> + Ord + Display + Send + Sync + 'static,
        InstrumentKey: Debug + Clone + PartialEq + Send + Sync + 'static,
        Subscription<BinanceSpot, Instrument, PublicTrades>: Identifier<BinanceMarket>,
        Subscription<BinanceSpot, Instrument, OrderBooksL1>: Identifier<BinanceMarket>,
        Subscription<BinanceSpot, Instrument, OrderBooksL2>: Identifier<BinanceMarket>,
        Subscription<BinanceSpot, Instrument, Candles>: Identifier<BinanceMarket>,
        Subscription<BinanceFuturesUsd, Instrument, PublicTrades>: Identifier<BinanceMarket>,
        Subscription<BinanceFuturesUsd, Instrument, OrderBooksL1>: Identifier<BinanceMarket>,
        Subscription<BinanceFuturesUsd, Instrument, OrderBooksL2>: Identifier<BinanceMarket>,
        Subscription<BinanceFuturesUsd, Instrument, Liquidations>: Identifier<BinanceMarket>,
        Subscription<BinanceFuturesUsd, Instrument, Candles>: Identifier<BinanceMarket>,
        Subscription<Bitfinex, Instrument, PublicTrades>: Identifier<BitfinexMarket>,
        Subscription<Bitfinex, Instrument, Candles>: Identifier<BitfinexMarket>,
        Subscription<Bitmex, Instrument, PublicTrades>: Identifier<BitmexMarket>,
        Subscription<Bitmex, Instrument, Candles>: Identifier<BitmexMarket>,
        Subscription<BybitSpot, Instrument, PublicTrades>: Identifier<BybitMarket>,
        Subscription<BybitSpot, Instrument, OrderBooksL1>: Identifier<BybitMarket>,
        Subscription<BybitSpot, Instrument, OrderBooksL2>: Identifier<BybitMarket>,
        Subscription<BybitSpot, Instrument, Candles>: Identifier<BybitMarket>,
        Subscription<BybitPerpetualsUsd, Instrument, PublicTrades>: Identifier<BybitMarket>,
        Subscription<BybitPerpetualsUsd, Instrument, OrderBooksL1>: Identifier<BybitMarket>,
        Subscription<BybitPerpetualsUsd, Instrument, OrderBooksL2>: Identifier<BybitMarket>,
        Subscription<BybitPerpetualsUsd, Instrument, Candles>: Identifier<BybitMarket>,
        Subscription<Coinbase, Instrument, PublicTrades>: Identifier<CoinbaseMarket>,
        Subscription<Coinbase, Instrument, Candles>: Identifier<CoinbaseMarket>,
        Subscription<GateioSpot, Instrument, PublicTrades>: Identifier<GateioMarket>,
        Subscription<GateioSpot, Instrument, Candles>: Identifier<GateioMarket>,
        Subscription<GateioFuturesUsd, Instrument, PublicTrades>: Identifier<GateioMarket>,
        Subscription<GateioFuturesUsd, Instrument, Candles>: Identifier<GateioMarket>,
        Subscription<GateioFuturesBtc, Instrument, PublicTrades>: Identifier<GateioMarket>,
        Subscription<GateioFuturesBtc, Instrument, Candles>: Identifier<GateioMarket>,
        Subscription<GateioPerpetualsUsd, Instrument, PublicTrades>: Identifier<GateioMarket>,
        Subscription<GateioPerpetualsUsd, Instrument, Candles>: Identifier<GateioMarket>,
        Subscription<GateioPerpetualsBtc, Instrument, PublicTrades>: Identifier<GateioMarket>,
        Subscription<GateioPerpetualsBtc, Instrument, Candles>: Identifier<GateioMarket>,
        Subscription<GateioOptions, Instrument, PublicTrades>: Identifier<GateioMarket>,
        Subscription<GateioOptions, Instrument, Candles>: Identifier<GateioMarket>,
        Subscription<Hyperliquid, Instrument, PublicTrades>: Identifier<HyperliquidMarket>,
        Subscription<Hyperliquid, Instrument, Candles>: Identifier<HyperliquidMarket>,
        Subscription<Kraken, Instrument, PublicTrades>: Identifier<KrakenMarket>,
        Subscription<Kraken, Instrument, OrderBooksL1>: Identifier<KrakenMarket>,
        Subscription<Kraken, Instrument, Candles>: Identifier<KrakenMarket>,
        Subscription<Okx, Instrument, PublicTrades>: Identifier<OkxMarket>,
        Subscription<Okx, Instrument, Candles>: Identifier<OkxMarket>,
    {
        // Validate & dedup Subscription batches
        let batches = validate_batches(subscription_batches)?;

        // Generate required Channels from Subscription batches
        let channels = Channels::try_from(&batches)?;

        // Build the stream factory registry
        let registry = register_streams! {
            BinanceSpot       => [PublicTrades, OrderBooksL1, OrderBooksL2, Candles],
            BinanceFuturesUsd => [PublicTrades, OrderBooksL1, OrderBooksL2, Liquidations, Candles],
            Bitfinex          => [PublicTrades, Candles],
            Bitmex            => [PublicTrades, Candles],
            BybitSpot         => [PublicTrades, OrderBooksL1, OrderBooksL2, Candles],
            BybitPerpetualsUsd => [PublicTrades, OrderBooksL1, OrderBooksL2, Candles],
            Coinbase          => [PublicTrades, Candles],
            GateioSpot        => [PublicTrades, Candles],
            GateioFuturesUsd  => [PublicTrades, Candles],
            GateioFuturesBtc  => [PublicTrades, Candles],
            GateioPerpetualsUsd => [PublicTrades, Candles],
            GateioPerpetualsBtc => [PublicTrades, Candles],
            GateioOptions     => [PublicTrades, Candles],
            Hyperliquid       => [PublicTrades, Candles],
            Kraken            => [PublicTrades, OrderBooksL1, Candles],
            Okx               => [PublicTrades, Candles],
        };

        // Group subs by (ExchangeId, SubKind) and launch all connections concurrently
        let txs_ref = &channels.txs;

        let futures = batches.into_iter().map(|mut batch| {
            batch.sort_unstable_by_key(|sub| (sub.exchange, sub.kind));
            let by_exchange_by_sub_kind =
                batch.into_iter().chunk_by(|sub| (sub.exchange, sub.kind));

            let batch_futures =
                by_exchange_by_sub_kind
                    .into_iter()
                    .map(|((exchange, sub_kind), subs)| {
                        let subs = subs.into_iter().collect::<Vec<_>>();
                        let factory = registry
                            .get(exchange, sub_kind)
                            .ok_or(DataError::Unsupported { exchange, sub_kind });
                        async move {
                            factory?
                                .init_and_forward(
                                    subs,
                                    STREAM_RECONNECTION_POLICY,
                                    txs_ref,
                                )
                                .await
                        }
                    });

            try_join_all(batch_futures)
        });

        // Collect handle pairs from all batches and flatten into a single map
        let nested_results = try_join_all(futures).await?;
        let collected_handles: FnvHashMap<HandleKey, Box<dyn DynHandle<Instrument>>> =
            nested_results.into_iter().flatten().collect();

        let handles = DynamicStreamHandles {
            handles: Arc::new(collected_handles),
        };

        Ok((
            Self {
                trades: channels
                    .rxs
                    .trades
                    .into_iter()
                    .map(|(exchange, rx)| (exchange, rx.into_stream()))
                    .collect(),
                l1s: channels
                    .rxs
                    .l1s
                    .into_iter()
                    .map(|(exchange, rx)| (exchange, rx.into_stream()))
                    .collect(),
                l2s: channels
                    .rxs
                    .l2s
                    .into_iter()
                    .map(|(exchange, rx)| (exchange, rx.into_stream()))
                    .collect(),
                liquidations: channels
                    .rxs
                    .liquidations
                    .into_iter()
                    .map(|(exchange, rx)| (exchange, rx.into_stream()))
                    .collect(),
                candles: channels
                    .rxs
                    .candles
                    .into_iter()
                    .map(|(exchange, rx)| (exchange, rx.into_stream()))
                    .collect(),
            },
            handles,
        ))
    }

    /// Remove an exchange [`PublicTrade`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_trades(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>> {
        self.trades.remove(&exchange)
    }

    /// Select and merge every exchange [`PublicTrade`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_trades(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, PublicTrade>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.trades).into_values())
    }

    /// Remove an exchange [`OrderBookL1`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l1s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>> {
        self.l1s.remove(&exchange)
    }

    /// Select and merge every exchange [`OrderBookL1`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l1s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookL1>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l1s).into_values())
    }

    /// Remove an exchange [`OrderBookEvent`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_l2s(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        self.l2s.remove(&exchange)
    }

    /// Select and merge every exchange [`OrderBookEvent`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_l2s(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, OrderBookEvent>>> {
        futures_util::stream::select_all::select_all(std::mem::take(&mut self.l2s).into_values())
    }

    /// Remove an exchange [`Liquidation`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_liquidations(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>> {
        self.liquidations.remove(&exchange)
    }

    /// Select and merge every exchange [`Liquidation`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_liquidations(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Liquidation>>> {
        futures_util::stream::select_all::select_all(
            std::mem::take(&mut self.liquidations).into_values(),
        )
    }

    /// Remove an exchange [`Candle`] `Stream` from the [`DynamicStreams`] collection.
    ///
    /// Note that calling this method will permanently remove this `Stream` from [`Self`].
    pub fn select_candles(
        &mut self,
        exchange: ExchangeId,
    ) -> Option<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        self.candles.remove(&exchange)
    }

    /// Select and merge every exchange [`Candle`] `Stream` using
    /// [`SelectAll`](futures_util::stream::select_all::select_all).
    pub fn select_all_candles(
        &mut self,
    ) -> SelectAll<UnboundedReceiverStream<MarketStreamResult<InstrumentKey, Candle>>> {
        futures_util::stream::select_all::select_all(
            std::mem::take(&mut self.candles).into_values(),
        )
    }

    /// Select and merge every exchange `Stream` for every data type using [`select_all`](futures_util::stream::select_all::select_all)
    ///
    /// Note that using [`MarketStreamResult<Instrument, DataKind>`] as the `Output` is suitable for most
    /// use cases.
    pub fn select_all<Output>(self) -> impl Stream<Item = Output>
    where
        InstrumentKey: Send + Sync + 'static,
        Output: 'static,
        MarketStreamResult<InstrumentKey, PublicTrade>: Into<Output>,
        MarketStreamResult<InstrumentKey, OrderBookL1>: Into<Output>,
        MarketStreamResult<InstrumentKey, OrderBookEvent>: Into<Output>,
        MarketStreamResult<InstrumentKey, Liquidation>: Into<Output>,
        MarketStreamResult<InstrumentKey, Candle>: Into<Output>,
    {
        let Self {
            trades,
            l1s,
            l2s,
            liquidations,
            candles,
        } = self;

        let trades = trades
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l1s = l1s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let l2s = l2s
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let liquidations = liquidations
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let candles = candles
            .into_values()
            .map(|stream| stream.map(MarketStreamResult::into).boxed());

        let all = trades
            .chain(l1s)
            .chain(l2s)
            .chain(liquidations)
            .chain(candles);

        futures_util::stream::select_all::select_all(all)
    }
}

pub fn validate_batches<SubBatchIter, SubIter, Sub, Instrument>(
    batches: SubBatchIter,
) -> Result<Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>, DataError>
where
    SubBatchIter: IntoIterator<Item = SubIter>,
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
    Instrument: InstrumentData + Ord,
{
    batches
        .into_iter()
        .map(validate_subscriptions::<SubIter, Sub, Instrument>)
        .collect()
}

pub fn validate_subscriptions<SubIter, Sub, Instrument>(
    batch: SubIter,
) -> Result<Vec<Subscription<ExchangeId, Instrument, SubKind>>, DataError>
where
    SubIter: IntoIterator<Item = Sub>,
    Sub: Into<Subscription<ExchangeId, Instrument, SubKind>>,
    Instrument: InstrumentData + Ord,
{
    // Validate Subscriptions
    let mut batch = batch
        .into_iter()
        .map(Sub::into)
        .map(Validator::validate)
        .collect::<Result<Vec<_>, SocketError>>()?;

    // Remove duplicate Subscriptions
    batch.sort();
    batch.dedup();

    Ok(batch)
}

struct Channels<InstrumentKey> {
    txs: Arc<Txs<InstrumentKey>>,
    rxs: Rxs<InstrumentKey>,
}

impl<'a, Instrument> TryFrom<&'a Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>>
    for Channels<Instrument::Key>
where
    Instrument: InstrumentData,
{
    type Error = DataError;

    fn try_from(
        value: &'a Vec<Vec<Subscription<ExchangeId, Instrument, SubKind>>>,
    ) -> Result<Self, Self::Error> {
        let mut txs = Txs::default();
        let mut rxs = Rxs::default();

        for sub in value.iter().flatten() {
            match sub.kind {
                SubKind::PublicTrades => {
                    if let (None, None) =
                        (txs.trades.get(&sub.exchange), rxs.trades.get(&sub.exchange))
                    {
                        let (tx, rx) = mpsc_unbounded();
                        txs.trades.insert(sub.exchange, tx);
                        rxs.trades.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL1 => {
                    if let (None, None) = (txs.l1s.get(&sub.exchange), rxs.l1s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l1s.insert(sub.exchange, tx);
                        rxs.l1s.insert(sub.exchange, rx);
                    }
                }
                SubKind::OrderBooksL2 => {
                    if let (None, None) = (txs.l2s.get(&sub.exchange), rxs.l2s.get(&sub.exchange)) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.l2s.insert(sub.exchange, tx);
                        rxs.l2s.insert(sub.exchange, rx);
                    }
                }
                SubKind::Liquidations => {
                    if let (None, None) = (
                        txs.liquidations.get(&sub.exchange),
                        rxs.liquidations.get(&sub.exchange),
                    ) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.liquidations.insert(sub.exchange, tx);
                        rxs.liquidations.insert(sub.exchange, rx);
                    }
                }
                SubKind::Candles(_) => {
                    if let (None, None) = (
                        txs.candles.get(&sub.exchange),
                        rxs.candles.get(&sub.exchange),
                    ) {
                        let (tx, rx) = mpsc_unbounded();
                        txs.candles.insert(sub.exchange, tx);
                        rxs.candles.insert(sub.exchange, rx);
                    }
                }
                unsupported => return Err(DataError::UnsupportedSubKind(unsupported)),
            }
        }

        Ok(Channels {
            txs: Arc::new(txs),
            rxs,
        })
    }
}

struct Txs<InstrumentKey> {
    trades: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    l1s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    l2s: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    liquidations:
        FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Liquidation>>>,
    candles: FnvHashMap<ExchangeId, UnboundedTx<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> Default for Txs<InstrumentKey> {
    fn default() -> Self {
        Self {
            trades: Default::default(),
            l1s: Default::default(),
            l2s: Default::default(),
            liquidations: Default::default(),
            candles: Default::default(),
        }
    }
}

struct Rxs<InstrumentKey> {
    trades: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, PublicTrade>>>,
    l1s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookL1>>>,
    l2s: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, OrderBookEvent>>>,
    liquidations:
        FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Liquidation>>>,
    candles: FnvHashMap<ExchangeId, UnboundedRx<MarketStreamResult<InstrumentKey, Candle>>>,
}

impl<InstrumentKey> Default for Rxs<InstrumentKey> {
    fn default() -> Self {
        Self {
            trades: Default::default(),
            l1s: Default::default(),
            l2s: Default::default(),
            liquidations: Default::default(),
            candles: Default::default(),
        }
    }
}

/// Routes a [`SubscriptionKind`] to its corresponding output channel in [`Txs`].
///
/// Each concrete kind implements this once; the factory uses it to forward
/// events to the correct `DynamicStreams` field (trades, l1s, l2s, etc.).
trait OutputSelector<IK>: SubscriptionKind {
    /// The [`SubKindVariant`] for this kind, used as the registry lookup key.
    const VARIANT: SubKindVariant;

    /// Returns a reference to the output sender for `exchange`, or `None` if
    /// no channel was created for that exchange.
    fn sender(
        txs: &Txs<IK>,
        exchange: ExchangeId,
    ) -> Option<&UnboundedTx<MarketStreamResult<IK, Self::Event>>>;
}

impl<IK> OutputSelector<IK> for PublicTrades {
    const VARIANT: SubKindVariant = SubKindVariant::PublicTrades;

    fn sender(
        txs: &Txs<IK>,
        exchange: ExchangeId,
    ) -> Option<&UnboundedTx<MarketStreamResult<IK, PublicTrade>>> {
        txs.trades.get(&exchange)
    }
}

impl<IK> OutputSelector<IK> for OrderBooksL1 {
    const VARIANT: SubKindVariant = SubKindVariant::OrderBooksL1;

    fn sender(
        txs: &Txs<IK>,
        exchange: ExchangeId,
    ) -> Option<&UnboundedTx<MarketStreamResult<IK, OrderBookL1>>> {
        txs.l1s.get(&exchange)
    }
}

impl<IK> OutputSelector<IK> for OrderBooksL2 {
    const VARIANT: SubKindVariant = SubKindVariant::OrderBooksL2;

    fn sender(
        txs: &Txs<IK>,
        exchange: ExchangeId,
    ) -> Option<&UnboundedTx<MarketStreamResult<IK, OrderBookEvent>>> {
        txs.l2s.get(&exchange)
    }
}

impl<IK> OutputSelector<IK> for Liquidations {
    const VARIANT: SubKindVariant = SubKindVariant::Liquidations;

    fn sender(
        txs: &Txs<IK>,
        exchange: ExchangeId,
    ) -> Option<&UnboundedTx<MarketStreamResult<IK, Liquidation>>> {
        txs.liquidations.get(&exchange)
    }
}

impl<IK> OutputSelector<IK> for Candles {
    const VARIANT: SubKindVariant = SubKindVariant::Candles;

    fn sender(
        txs: &Txs<IK>,
        exchange: ExchangeId,
    ) -> Option<&UnboundedTx<MarketStreamResult<IK, Candle>>> {
        txs.candles.get(&exchange)
    }
}

/// Object-safe, type-erased factory for initializing a market stream connection
/// and forwarding events to the correct output channel.
///
/// Each `(Exchange, Kind)` pair is represented by one [`TypedStreamFactory`]
/// that implements this trait.
#[async_trait]
trait StreamFactory<Instrument, IK>: Send + Sync {
    async fn init_and_forward(
        &self,
        subs: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
        policy: crate::streams::reconnect::stream::ReconnectionBackoffPolicy,
        txs: &Txs<IK>,
    ) -> Result<(HandleKey, Box<dyn DynHandle<Instrument>>), DataError>;
}

/// Zero-sized generic factory that monomorphizes [`init_market_stream`] for a
/// specific `(Exchange, Kind)` pair.
struct TypedStreamFactory<Exchange, Kind>(PhantomData<(Exchange, Kind)>);

impl<E, K> TypedStreamFactory<E, K> {
    const fn new() -> Self {
        Self(PhantomData)
    }
}

#[async_trait]
impl<E, Instrument, K, IK> StreamFactory<Instrument, IK> for TypedStreamFactory<E, K>
where
    E: crate::exchange::StreamSelector<Instrument, K> + Send + Sync + 'static,
    Instrument: InstrumentData<Key = IK> + Display + Send + Sync + 'static,
    IK: Debug + Clone + PartialEq + Send + Sync + 'static,
    K: SubscriptionKind + OutputSelector<IK> + TryFrom<SubKind, Error = DataError> + Display + Send + Sync + 'static,
    K::Event: Clone + Debug + Send + 'static,
    Subscription<E, Instrument, K>: Identifier<E::Channel> + Identifier<E::Market> + 'static,
    E::Channel: 'static,
    E::Market: 'static,
{
    async fn init_and_forward(
        &self,
        subs: Vec<Subscription<ExchangeId, Instrument, SubKind>>,
        policy: crate::streams::reconnect::stream::ReconnectionBackoffPolicy,
        txs: &Txs<IK>,
    ) -> Result<(HandleKey, Box<dyn DynHandle<Instrument>>), DataError> {
        let first = subs.first().ok_or(DataError::SubscriptionsEmpty)?;
        let exchange = first.exchange;
        let sub_kind = first.kind;

        // Convert erased subs to typed subs via TryFrom<SubKind>
        let typed_subs: Vec<Subscription<E, Instrument, K>> = subs
            .into_iter()
            .map(|sub| {
                let kind = K::try_from(sub.kind)?;
                Ok(Subscription::new(E::default(), sub.instrument, kind))
            })
            .collect::<Result<Vec<_>, DataError>>()?;

        // Initialize the market stream (returns event receiver + typed handle)
        let (event_rx, handle) = init_market_stream(policy, typed_subs).await?;

        // Route events to the correct output channel via OutputSelector
        let tx = K::sender(txs, exchange)
            .ok_or(DataError::NoConnection { exchange, sub_kind })?
            .clone();
        tokio::spawn(
            UnboundedReceiverStream::new(event_rx).forward_to(tx),
        );

        // Return the handle pair for collection by the caller
        Ok((
            HandleKey { exchange, sub_kind },
            Box::new(handle) as Box<dyn DynHandle<Instrument>>,
        ))
    }
}

/// Runtime lookup table mapping `(ExchangeId, SubKindVariant)` to a type-erased
/// [`StreamFactory`].
struct StreamRegistry<Instrument, IK>(
    FnvHashMap<(ExchangeId, SubKindVariant), Box<dyn StreamFactory<Instrument, IK>>>,
);

impl<Instrument, IK> StreamRegistry<Instrument, IK>
where
    Instrument: Send + 'static,
    IK: Send + 'static,
{
    fn new() -> Self {
        Self(FnvHashMap::default())
    }

    fn register<E, K>(&mut self)
    where
        TypedStreamFactory<E, K>: StreamFactory<Instrument, IK> + 'static,
        E: Connector,
        K: OutputSelector<IK>,
    {
        self.0.insert(
            (E::ID, K::VARIANT),
            Box::new(TypedStreamFactory::<E, K>::new()),
        );
    }

    fn get(
        &self,
        exchange: ExchangeId,
        sub_kind: SubKind,
    ) -> Option<&dyn StreamFactory<Instrument, IK>> {
        let variant = SubKindVariant::from(sub_kind);
        self.0.get(&(exchange, variant)).map(|f| f.as_ref())
    }
}

