use crate::{
    EngineEvent,
    engine::{
        Engine, EngineOutput,
        Processor,
        audit::{AuditTick, Auditor, context::EngineContext},
        clock::EngineClock,
        process_with_audit,
        state::{
            EngineState, instrument::data::InstrumentDataState, position::PositionExited,
        },
    },
    statistic::{summary::TradingSummary, time::TimeInterval},
    shutdown::SyncShutdown,
};
use barter_execution::{
    AccountEventKind, AccountSnapshot,
    balance::AssetBalance,
    trade::Trade,
};
use barter_instrument::{asset::AssetIndex, instrument::InstrumentIndex, asset::QuoteAsset};
use barter_integration::{FeedEnded, Terminal};
use barter_integration::snapshot::Snapshot;
use rust_decimal::Decimal;
use std::marker::PhantomData;

/// Receives step-wise backtest events after the engine has processed each input.
///
/// Implement this trait to collect chart replay data, closed positions, equity samples, or
/// snapshots without coupling Barter's batch runner to a UI-specific output type.
pub trait BacktestEventSink<Engine, Audit> {
    /// Called after each processed event, with access to the updated engine state.
    fn on_step(&mut self, _engine: &Engine, _audit: &AuditTick<Audit, EngineContext>) {}

    /// Called when the stepper is finished and the engine has been shut down.
    fn on_finish(&mut self, _engine: &Engine, _shutdown_audit: &AuditTick<Audit, EngineContext>) {}
}

/// Event sink that discards every step.
#[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct NoopBacktestEventSink;

impl<Engine, Audit> BacktestEventSink<Engine, Audit> for NoopBacktestEventSink {}

/// Event sink that stores every audit tick in memory.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct VecBacktestEventSink<Audit> {
    pub ticks: Vec<AuditTick<Audit, EngineContext>>,
}

impl<Audit> Default for VecBacktestEventSink<Audit> {
    fn default() -> Self {
        Self { ticks: Vec::new() }
    }
}

impl<Engine, Audit> BacktestEventSink<Engine, Audit> for VecBacktestEventSink<Audit>
where
    Audit: Clone,
{
    fn on_step(&mut self, _engine: &Engine, audit: &AuditTick<Audit, EngineContext>) {
        self.ticks.push(audit.clone());
    }
}

/// Audit type produced by Barter's indexed [`Engine`] backtest path.
pub type BacktestEngineAudit<MarketKind, OnTradingDisabled, OnDisconnect> =
    crate::engine::audit::EngineAudit<
        EngineEvent<MarketKind>,
        EngineOutput<OnTradingDisabled, OnDisconnect>,
    >;

/// Account-level sample observed while stepping a Barter engine.
#[derive(Debug, Clone, PartialEq)]
pub enum BacktestAccountSample {
    Snapshot(AccountSnapshot),
    Balance(Snapshot<AssetBalance<AssetIndex>>),
}

/// Domain-shaped result extracted from one Barter engine audit tick.
#[derive(Debug, Clone, PartialEq)]
pub struct BacktestEngineStep<MarketKind, OnTradingDisabled, OnDisconnect> {
    pub audit: AuditTick<BacktestEngineAudit<MarketKind, OnTradingDisabled, OnDisconnect>, EngineContext>,
    pub trades: Vec<Trade<QuoteAsset, InstrumentIndex>>,
    pub position_exits: Vec<PositionExited<QuoteAsset>>,
    pub account_samples: Vec<BacktestAccountSample>,
}

impl<MarketKind, OnTradingDisabled, OnDisconnect>
    BacktestEngineStep<MarketKind, OnTradingDisabled, OnDisconnect>
where
    MarketKind: Clone,
    OnTradingDisabled: Clone,
    OnDisconnect: Clone,
{
    pub fn from_audit(
        audit: &AuditTick<BacktestEngineAudit<MarketKind, OnTradingDisabled, OnDisconnect>, EngineContext>,
    ) -> Self {
        let mut step = Self {
            audit: audit.clone(),
            trades: Vec::new(),
            position_exits: Vec::new(),
            account_samples: Vec::new(),
        };

        let crate::engine::audit::EngineAudit::Process(process) = &audit.event else {
            return step;
        };

        if let EngineEvent::Account(crate::execution::AccountStreamEvent::Item(account)) =
            &process.event
        {
            match &account.kind {
                AccountEventKind::Snapshot(snapshot) => {
                    step.account_samples
                        .push(BacktestAccountSample::Snapshot(snapshot.clone()));
                }
                AccountEventKind::BalanceSnapshot(balance) => {
                    step.account_samples
                        .push(BacktestAccountSample::Balance(balance.clone()));
                }
                AccountEventKind::Trade(trade) => step.trades.push(trade.clone()),
                AccountEventKind::OrderSnapshot(_) | AccountEventKind::OrderCancelled(_) => {}
            }
        }

        for output in &process.outputs {
            if let EngineOutput::PositionExit(position) = output {
                step.position_exits.push(position.clone());
            }
        }

        step
    }
}

/// Collects Barter-engine backtest outputs in a reusable domain-shaped result surface.
#[derive(Debug, Clone)]
pub struct BacktestEngineEventSink<MarketKind, OnTradingDisabled, OnDisconnect, Interval> {
    pub risk_free_return: Decimal,
    pub summary_interval: Interval,
    pub steps: Vec<BacktestEngineStep<MarketKind, OnTradingDisabled, OnDisconnect>>,
    pub final_summary: Option<TradingSummary<Interval>>,
}

impl<MarketKind, OnTradingDisabled, OnDisconnect, Interval>
    BacktestEngineEventSink<MarketKind, OnTradingDisabled, OnDisconnect, Interval>
{
    pub fn new(risk_free_return: Decimal, summary_interval: Interval) -> Self {
        Self {
            risk_free_return,
            summary_interval,
            steps: Vec::new(),
            final_summary: None,
        }
    }

    pub fn trades(&self) -> impl Iterator<Item = &Trade<QuoteAsset, InstrumentIndex>> {
        self.steps.iter().flat_map(|step| step.trades.iter())
    }

    pub fn position_exits(&self) -> impl Iterator<Item = &PositionExited<QuoteAsset>> {
        self.steps
            .iter()
            .flat_map(|step| step.position_exits.iter())
    }

    pub fn account_samples(&self) -> impl Iterator<Item = &BacktestAccountSample> {
        self.steps
            .iter()
            .flat_map(|step| step.account_samples.iter())
    }
}

impl<
    Clock,
    GlobalData,
    InstrumentData,
    ExecutionTxs,
    Strategy,
    Risk,
    OnTradingDisabled,
    OnDisconnect,
    Interval,
>
    BacktestEventSink<
        Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>,
        BacktestEngineAudit<InstrumentData::MarketEventKind, OnTradingDisabled, OnDisconnect>,
    >
    for BacktestEngineEventSink<
        InstrumentData::MarketEventKind,
        OnTradingDisabled,
        OnDisconnect,
        Interval,
    >
where
    Clock: EngineClock,
    InstrumentData: InstrumentDataState,
    InstrumentData::MarketEventKind: Clone,
    OnTradingDisabled: Clone,
    OnDisconnect: Clone,
    Interval: TimeInterval,
{
    fn on_step(
        &mut self,
        _engine: &Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>,
        audit: &AuditTick<
            BacktestEngineAudit<InstrumentData::MarketEventKind, OnTradingDisabled, OnDisconnect>,
            EngineContext,
        >,
    ) {
        self.steps.push(BacktestEngineStep::from_audit(audit));
    }

    fn on_finish(
        &mut self,
        engine: &Engine<Clock, EngineState<GlobalData, InstrumentData>, ExecutionTxs, Strategy, Risk>,
        _shutdown_audit: &AuditTick<
            BacktestEngineAudit<InstrumentData::MarketEventKind, OnTradingDisabled, OnDisconnect>,
            EngineContext,
        >,
    ) {
        self.final_summary = Some(
            engine
                .trading_summary_generator(self.risk_free_return)
                .generate(self.summary_interval),
        );
    }
}

/// Outcome of a single backtest step.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum BacktestStepOutcome {
    Continue,
    Terminal,
}

/// Result of processing one event with a [`BacktestStepper`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BacktestStep<Audit> {
    pub audit: AuditTick<Audit, EngineContext>,
    pub outcome: BacktestStepOutcome,
}

impl<Audit> BacktestStep<Audit> {
    pub fn is_terminal(&self) -> bool {
        self.outcome == BacktestStepOutcome::Terminal
    }
}

/// Step-wise backtest driver for an already constructed engine.
///
/// This is deliberately lower-level than [`super::run_backtests`]. It does not own market-data
/// loading, execution construction, or concurrent batch orchestration; callers feed it events and
/// can observe every step through a [`BacktestEventSink`].
#[derive(Debug)]
pub struct BacktestStepper<Engine, Event, Sink = NoopBacktestEventSink>
where
    Engine: Processor<Event>,
{
    engine: Engine,
    sink: Sink,
    terminal_audit: Option<AuditTick<Engine::Audit, EngineContext>>,
    phantom_event: PhantomData<Event>,
}

impl<Engine, Event> BacktestStepper<Engine, Event, NoopBacktestEventSink>
where
    Engine: Processor<Event>,
{
    pub fn new(engine: Engine) -> Self {
        Self::with_sink(engine, NoopBacktestEventSink)
    }
}

impl<Engine, Event, Sink> BacktestStepper<Engine, Event, Sink>
where
    Engine: Processor<Event>,
{
    pub fn with_sink(engine: Engine, sink: Sink) -> Self {
        Self {
            engine,
            sink,
            terminal_audit: None,
            phantom_event: PhantomData,
        }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }

    pub fn sink(&self) -> &Sink {
        &self.sink
    }

    pub fn sink_mut(&mut self) -> &mut Sink {
        &mut self.sink
    }

    pub fn is_terminal(&self) -> bool {
        self.terminal_audit.is_some()
    }

    /// Process one input event and notify the configured sink.
    ///
    /// Returns `None` if the stepper has already observed a terminal audit.
    pub fn step(&mut self, event: Event) -> Option<BacktestStep<Engine::Audit>>
    where
        Engine: Auditor<Engine::Audit, Context = EngineContext>,
        Engine::Audit: Clone + Terminal,
        Sink: BacktestEventSink<Engine, Engine::Audit>,
    {
        if self.is_terminal() {
            return None;
        }

        let audit = process_with_audit(&mut self.engine, event);
        let outcome = if audit.event.is_terminal() {
            BacktestStepOutcome::Terminal
        } else {
            BacktestStepOutcome::Continue
        };

        self.sink.on_step(&self.engine, &audit);

        if outcome == BacktestStepOutcome::Terminal {
            self.terminal_audit = Some(audit.clone());
        }

        Some(BacktestStep { audit, outcome })
    }

    /// Process events until the feed ends or the engine emits a terminal audit.
    pub fn run<Events>(&mut self, events: Events) -> Option<BacktestStep<Engine::Audit>>
    where
        Events: IntoIterator<Item = Event>,
        Engine: Auditor<Engine::Audit, Context = EngineContext>,
        Engine::Audit: Clone + Terminal,
        Sink: BacktestEventSink<Engine, Engine::Audit>,
    {
        let mut last = None;

        for event in events {
            let step = self.step(event)?;
            let terminal = step.is_terminal();
            last = Some(step);

            if terminal {
                break;
            }
        }

        last
    }

    /// Finish the backtest and return the engine, sink, terminal audit, and shutdown output.
    ///
    /// If no terminal audit has been observed, this emits a synthetic `FeedEnded` audit before
    /// shutting down the engine. This mirrors the existing engine runners' feed-ended behavior
    /// while preserving access to the final engine state.
    pub fn finish(mut self) -> BacktestStepperOutput<Engine, Engine::Audit, Sink, Engine::Result>
    where
        Engine: Auditor<Engine::Audit, Context = EngineContext> + SyncShutdown,
        Engine::Audit: Clone + From<FeedEnded> + Terminal,
        Sink: BacktestEventSink<Engine, Engine::Audit>,
    {
        let shutdown_audit = match self.terminal_audit.take() {
            Some(audit) => audit,
            None => {
                let audit = self.engine.audit(FeedEnded);
                self.sink.on_step(&self.engine, &audit);
                audit
            }
        };

        let shutdown = self.engine.shutdown();
        self.sink.on_finish(&self.engine, &shutdown_audit);

        BacktestStepperOutput {
            engine: self.engine,
            sink: self.sink,
            shutdown_audit,
            shutdown,
        }
    }
}

/// Output returned by [`BacktestStepper::finish`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BacktestStepperOutput<Engine, Audit, Sink, Shutdown> {
    pub engine: Engine,
    pub sink: Sink,
    pub shutdown_audit: AuditTick<Audit, EngineContext>,
    pub shutdown: Shutdown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Sequence, shutdown::Shutdown};
    use barter_integration::FeedEnded;
    use chrono::{DateTime, Utc};

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    enum TestEvent {
        Value(u8),
        Stop,
    }

    #[derive(Debug, Copy, Clone, Eq, PartialEq)]
    enum TestAudit {
        FeedEnded,
        Shutdown,
        Processed(TestEvent),
    }

    impl From<FeedEnded> for TestAudit {
        fn from(_: FeedEnded) -> Self {
            Self::FeedEnded
        }
    }

    impl From<Shutdown> for TestAudit {
        fn from(_: Shutdown) -> Self {
            Self::Shutdown
        }
    }

    impl Terminal for TestAudit {
        fn is_terminal(&self) -> bool {
            matches!(self, Self::FeedEnded | Self::Shutdown | Self::Processed(TestEvent::Stop))
        }
    }

    #[derive(Debug, Default, Eq, PartialEq)]
    struct TestEngine {
        processed: Vec<u8>,
        next_sequence: u64,
        shutdowns: usize,
    }

    impl Processor<TestEvent> for TestEngine {
        type Audit = TestAudit;

        fn process(&mut self, event: TestEvent) -> Self::Audit {
            if let TestEvent::Value(value) = event {
                self.processed.push(value);
            }

            TestAudit::Processed(event)
        }
    }

    impl Auditor<TestAudit> for TestEngine {
        type Snapshot = ();
        type Context = EngineContext;

        fn audit_snapshot(&mut self) -> AuditTick<Self::Snapshot, Self::Context> {
            AuditTick {
                event: (),
                context: self.next_context(),
            }
        }

        fn audit<Kind>(&mut self, kind: Kind) -> AuditTick<TestAudit, Self::Context>
        where
            TestAudit: From<Kind>,
        {
            AuditTick {
                event: TestAudit::from(kind),
                context: self.next_context(),
            }
        }
    }

    impl SyncShutdown for TestEngine {
        type Result = usize;

        fn shutdown(&mut self) -> Self::Result {
            self.shutdowns += 1;
            self.shutdowns
        }
    }

    impl TestEngine {
        fn next_context(&mut self) -> EngineContext {
            let context = EngineContext {
                sequence: Sequence(self.next_sequence),
                time: DateTime::<Utc>::MIN_UTC,
            };
            self.next_sequence += 1;
            context
        }
    }

    #[derive(Debug, Default, Eq, PartialEq)]
    struct RecordingSink {
        processed_lens: Vec<usize>,
        finished: bool,
    }

    impl BacktestEventSink<TestEngine, TestAudit> for RecordingSink {
        fn on_step(&mut self, engine: &TestEngine, _audit: &AuditTick<TestAudit, EngineContext>) {
            self.processed_lens.push(engine.processed.len());
        }

        fn on_finish(
            &mut self,
            _engine: &TestEngine,
            _shutdown_audit: &AuditTick<TestAudit, EngineContext>,
        ) {
            self.finished = true;
        }
    }

    #[test]
    fn step_processes_one_event_and_notifies_sink_after_state_update() {
        let mut stepper = BacktestStepper::with_sink(TestEngine::default(), RecordingSink::default());

        let step = stepper.step(TestEvent::Value(7)).unwrap();

        assert_eq!(step.outcome, BacktestStepOutcome::Continue);
        assert_eq!(step.audit.event, TestAudit::Processed(TestEvent::Value(7)));
        assert_eq!(stepper.engine().processed, vec![7]);
        assert_eq!(stepper.sink().processed_lens, vec![1]);
    }

    #[test]
    fn finish_emits_feed_ended_when_no_terminal_event_was_seen() {
        let mut stepper = BacktestStepper::with_sink(TestEngine::default(), RecordingSink::default());
        stepper.run([TestEvent::Value(1), TestEvent::Value(2)]);

        let output = stepper.finish();

        assert_eq!(output.engine.processed, vec![1, 2]);
        assert_eq!(output.shutdown_audit.event, TestAudit::FeedEnded);
        assert_eq!(output.shutdown, 1);
        assert_eq!(output.sink.processed_lens, vec![1, 2, 2]);
        assert!(output.sink.finished);
    }

    #[test]
    fn terminal_event_stops_run_and_finish_uses_terminal_audit() {
        let mut stepper = BacktestStepper::with_sink(TestEngine::default(), RecordingSink::default());

        let last = stepper
            .run([TestEvent::Value(1), TestEvent::Stop, TestEvent::Value(2)])
            .unwrap();
        assert!(last.is_terminal());
        assert!(stepper.step(TestEvent::Value(3)).is_none());

        let output = stepper.finish();

        assert_eq!(output.engine.processed, vec![1]);
        assert_eq!(output.shutdown_audit.event, TestAudit::Processed(TestEvent::Stop));
        assert_eq!(output.sink.processed_lens, vec![1, 1]);
        assert!(output.sink.finished);
    }

    #[test]
    fn vec_sink_collects_audit_ticks() {
        let mut stepper =
            BacktestStepper::with_sink(TestEngine::default(), VecBacktestEventSink::default());

        stepper.run([TestEvent::Value(1)]);
        let output = stepper.finish();

        assert_eq!(output.sink.ticks.len(), 2);
        assert_eq!(output.sink.ticks[0].event, TestAudit::Processed(TestEvent::Value(1)));
        assert_eq!(output.sink.ticks[1].event, TestAudit::FeedEnded);
    }

    #[test]
    fn engine_sink_collects_domain_outputs_from_real_barter_engine() {
        use crate::{
            engine::{
                Engine,
                clock::HistoricalClock,
                execution_tx::MultiExchangeTxMap,
                state::{
                    EngineState, global::DefaultGlobalData,
                    instrument::data::DefaultInstrumentMarketData, trading::TradingState,
                },
            },
            execution::AccountStreamEvent,
            risk::DefaultRiskManager,
            statistic::time::Daily,
            strategy::DefaultStrategy,
        };
        use barter_data::event::DataKind;
        use barter_execution::{
            AccountEvent, AccountEventKind,
            balance::{AssetBalance, Balance},
            order::id::{OrderId, StrategyId},
            trade::{AssetFees, Trade, TradeId},
        };
        use barter_instrument::{
            Side, Underlying,
            asset::{AssetIndex, QuoteAsset},
            exchange::ExchangeId,
            index::IndexedInstruments,
            instrument::{Instrument, InstrumentIndex},
        };
        use barter_integration::channel::mpsc_unbounded;
        use barter_integration::snapshot::Snapshot;
        use rust_decimal_macros::dec;

        type State = EngineState<DefaultGlobalData, DefaultInstrumentMarketData>;

        let instruments = IndexedInstruments::builder()
            .add_instrument(Instrument::spot(
                ExchangeId::BinanceSpot,
                "binance_spot_btc_usdt",
                "BTCUSDT",
                Underlying::new("btc", "usdt"),
                None,
            ))
            .build();
        let clock = HistoricalClock::new(DateTime::<Utc>::MIN_UTC);
        let state = EngineState::builder(&instruments, DefaultGlobalData::default(), |_| {
            DefaultInstrumentMarketData::default()
        })
        .time_engine_start(DateTime::<Utc>::MIN_UTC)
        .trading_state(TradingState::Disabled)
        .balances([(ExchangeId::BinanceSpot, "usdt", Balance::new(dec!(1000), dec!(1000)))])
        .build();
        let (execution_tx, _execution_rx) = mpsc_unbounded();
        let execution_txs = MultiExchangeTxMap::from_iter([(ExchangeId::BinanceSpot, Some(execution_tx))]);
        let engine = Engine::new(
            clock,
            state,
            execution_txs,
            DefaultStrategy::<State>::default(),
            DefaultRiskManager::<State>::default(),
        );
        let sink = BacktestEngineEventSink::<DataKind, (), (), Daily>::new(dec!(0), Daily);
        let mut stepper = BacktestStepper::with_sink(engine, sink);

        let trade = |id: &str, side: Side, price| {
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                exchange: barter_instrument::exchange::ExchangeIndex(0),
                kind: AccountEventKind::Trade(Trade {
                    id: TradeId::new(id),
                    order_id: OrderId::new(id),
                    instrument: InstrumentIndex(0),
                    strategy: StrategyId::new("test"),
                    time_exchange: DateTime::<Utc>::MIN_UTC,
                    side,
                    price,
                    quantity: dec!(1),
                    fees: AssetFees::<QuoteAsset>::default(),
                }),
            }))
        };

        stepper.run([
            EngineEvent::Account(AccountStreamEvent::Item(AccountEvent {
                exchange: barter_instrument::exchange::ExchangeIndex(0),
                kind: AccountEventKind::BalanceSnapshot(Snapshot(AssetBalance {
                    asset: AssetIndex(1),
                    balance: Balance::new(dec!(1000), dec!(1000)),
                    time_exchange: DateTime::<Utc>::MIN_UTC,
                })),
            })),
            trade("entry", Side::Buy, dec!(100)),
            trade("exit", Side::Sell, dec!(110)),
        ]);
        let output = stepper.finish();

        assert_eq!(output.sink.account_samples().count(), 1);
        assert_eq!(output.sink.trades().count(), 2);
        let exits = output.sink.position_exits().collect::<Vec<_>>();
        assert_eq!(exits.len(), 1);
        assert_eq!(exits[0].pnl_realised, dec!(10));

        let summary = output.sink.final_summary.as_ref().unwrap();
        let tear = summary.instruments.get_index(0).unwrap().1;
        assert_eq!(tear.pnl, dec!(10));
    }
}
