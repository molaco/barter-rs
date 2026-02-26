use barter_data::{
    event::DataKind,
    streams::{
        builder::dynamic::DynamicStreams, consumer::MarketStreamResult,
        reconnect::stream::ReconnectingStream,
    },
    subscription::{SubKind, Subscription},
};
use barter_instrument::{
    exchange::ExchangeId,
    instrument::market_data::{MarketDataInstrument, kind::MarketDataInstrumentKind},
};
use futures::StreamExt;
use std::time::Duration;
use tracing::{info, warn};

#[rustfmt::skip]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialise INFO Tracing log subscriber
    init_logging();

    use ExchangeId::*;
    use MarketDataInstrumentKind::*;
    use SubKind::*;

    // Notes:
    // - DynamicStream::init requires an IntoIterator<Item = "subscription batch">.
    // - Each "subscription batch" is an IntoIterator<Item = Subscription>.
    // - Every "subscription batch" will initialise at-least-one WebSocket stream under the hood.
    // - If the "subscription batch" contains more-than-one ExchangeId and/or SubKind, the batch
    //   will be further split under the hood for compile-time reasons.

    // Initialise market reconnect::Event streams for various ExchangeIds and SubscriptionKinds
    let (streams, handles) = DynamicStreams::init([
        // Batch notes:
        // Since batch contains 1 ExchangeId and 1 SubscriptionKind, so only 1 (1x1) WebSockets
        // will be spawned for this batch.
        vec![
            (BinanceSpot, "btc", "usdt", Spot, PublicTrades),
            (BinanceSpot, "eth", "usdt", Spot, PublicTrades),
        ],

        // Batch notes:
        // Since batch contains 1 ExchangeId and 3 SubscriptionKinds, 3 (1x3) WebSocket connections
        // will be spawned for this batch (back-end requires to further split).
        vec![
            (BinanceFuturesUsd, "btc", "usdt", Perpetual, PublicTrades),
            (BinanceFuturesUsd, "btc", "usdt", Perpetual, OrderBooksL1),
            (BinanceFuturesUsd, "btc", "usdt", Perpetual, Liquidations),

        ],

        // Batch notes:
        // Since batch contains 2 ExchangeIds and 1 SubscriptionKind, 2 (2x1) WebSocket connections
        // will be spawned for this batch (back-end requires to further split).
        vec![
            (Okx, "btc", "usdt", Spot, PublicTrades),
            (Okx, "btc", "usdt", Perpetual, PublicTrades),
            (Bitmex, "btc", "usdt", Perpetual, PublicTrades),
            (Okx, "eth", "usdt", Spot, PublicTrades),
            (Okx, "eth", "usdt", Perpetual, PublicTrades),
            (Bitmex, "eth", "usdt", Perpetual, PublicTrades),
        ],
    ]).await.unwrap();

    // Select all streams, mapping each SubscriptionKind `MarketStreamResult<T>` into a unified
    // `Output` (eg/ `MarketStreamResult<_, DataKind>`), where MarketStreamResult<T>: Into<Output>
    // Notes:
    //  - DynamicStreams::init returns (streams, handles)
    //  - `handles` can be used for runtime subscribe/unsubscribe (see DynamicStreamHandles)
    //  - Use `streams.select_trades(ExchangeId)` to return a stream of trades from a given exchange.
    //  - Use `streams.select_<T>(ExchangeId)` to return a stream of T from a given exchange.
    //  - Use `streams.select_all_trades(ExchangeId)` to return a stream of trades from all exchanges
    let mut merged = streams
        .select_all::<MarketStreamResult<MarketDataInstrument, DataKind>>()
        .with_error_handler(|error| warn!(?error, "MarketStream generated error"));

    // Spawn a task to consume the merged stream so that main can continue
    let stream_task = tokio::spawn(async move {
        while let Some(event) = merged.next().await {
            info!("{event:?}");
        }
    });

    // --- Runtime subscribe/unsubscribe demonstration ---

    // Wait 5 seconds, then subscribe to a new instrument on an existing connection
    tokio::time::sleep(Duration::from_secs(5)).await;
    info!("Subscribing to BinanceSpot eth/usdt PublicTrades at runtime...");
    let sub_ids = handles.subscribe(vec![
        Subscription::new(
            ExchangeId::BinanceSpot,
            MarketDataInstrument::from(("eth", "usdt", MarketDataInstrumentKind::Spot)),
            SubKind::PublicTrades,
        ),
    ])?;
    info!(?sub_ids, "Subscribed to new instruments");

    // Wait 10 seconds, then unsubscribe from the instruments we just added
    tokio::time::sleep(Duration::from_secs(10)).await;
    info!("Unsubscribing from BinanceSpot eth/usdt PublicTrades...");
    handles.unsubscribe(ExchangeId::BinanceSpot, SubKind::PublicTrades, sub_ids)?;
    info!("Unsubscribed successfully");

    // Continue consuming the stream until it ends
    stream_task.await?;

    Ok(())
}

// Initialise an INFO `Subscriber` for `Tracing` Json logs and install it as the global default.
fn init_logging() {
    tracing_subscriber::fmt()
        // Filter messages based on the INFO
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        // Disable colours on release builds
        .with_ansi(cfg!(debug_assertions))
        // Enable Json formatting
        .json()
        // Install this Tracing subscriber as global default
        .init()
}
