# Phase 3: Fetcher / Collector Split — Detailed Implementation Plan

## Overview

Move all collector-level orchestration (pagination, stream composition, date scheduling, caching, retry ownership, concurrency) out of `barter-data` into a new `barter-collector` crate. `barter-data` becomes a pure single-batch fetcher library. No external consumers exist, so this is a clean cut with no deprecation concerns.

**Constraints (from Architecture Guide):**
- File-based modules (`pagination.rs`), not `mod.rs` (Arch Guide §1)
- Flat layout, virtual manifest, dependency inheritance (Arch Guide §3)
- Default private, escalate deliberately; `#[non_exhaustive]` on public types (Arch Guide §2)
- Enum for cursor strategy (closed set), not trait objects (Arch Guide §6)
- Traits are ports; exchange impls are adapters; collector is orchestrator (Arch Guide §5)
- `buffer_unordered` for backpressure (Arch Guide §9)
- No blocking I/O in async (Rusty §9)
- `unwrap`/`expect` only with invariant comments (Rusty §7)

---

## Step 3A: Create `barter-collector` Crate Skeleton

### 3A.1 Create directory structure

```
barter-collector/
  Cargo.toml
  src/
    lib.rs
    config.rs
    download.rs
    pagination.rs
    scheduling.rs
    caching.rs
    streams.rs
    filters.rs
```

### 3A.2 Write `barter-collector/Cargo.toml`

```toml
[package]
name = "barter-collector"
version = "0.1.0"
edition = "2024"
license = "MIT"
description = "Orchestration layer for barter-data: pagination, streaming, caching, retry"

[dependencies]
barter-data = { path = "../barter-data", features = ["rest", "bulk"] }
barter-integration = { workspace = true }
barter-instrument = { workspace = true }
tokio = { workspace = true, features = ["sync", "macros", "rt-multi-thread", "time"] }
futures = { workspace = true }
futures-util = { workspace = true }
chrono = { workspace = true }
tracing = { workspace = true }
governor = { workspace = true }
reqwest = { workspace = true }
thiserror = { workspace = true }
sha2 = { workspace = true }
rand = { workspace = true }
pin-project = { workspace = true }

[dev-dependencies]
tokio = { workspace = true, features = ["test-util"] }
tempfile = { workspace = true }
```

### 3A.3 Add to workspace root `Cargo.toml`

Add `"barter-collector"` to the `members` list (currently: barter, barter-data, barter-execution, barter-integration, barter-macro, barter-instrument).

Add workspace dependency entry:
```toml
barter-collector = { version = "0.1.0", path = "barter-collector" }
```

### 3A.4 Write `barter-collector/src/lib.rs`

```rust
pub mod config;
pub mod download;
pub mod pagination;
pub mod scheduling;
pub mod caching;
pub mod streams;
pub mod filters;
```

**Files touched:** `Cargo.toml` (root), new `barter-collector/` directory tree
**Validation:** `cargo check -p barter-collector`

---

## Step 3B: Move Scheduling Utilities

### 3B.1 Move `date_range()` from `barter-data/src/bulk/mod.rs:83-95`

**Current location:** `barter-data/src/bulk/mod.rs` lines 83-95
**New location:** `barter-collector/src/scheduling.rs`

```rust
use chrono::NaiveDate;

/// Generate an inclusive sequence of dates from `start` to `end`.
pub fn date_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut current = start;
    while current <= end {
        dates.push(current);
        current = current.succ_opt().unwrap_or(current);
        if current == start {
            break;  // Overflow protection
        }
    }
    dates
}
```

**Call sites to update (5 production + 3 tests):**
1. `barter-data/src/exchange/binance/bulk/mod.rs:395` — `stream_bulk_trades()`
2. `barter-data/src/exchange/binance/bulk/mod.rs:429` — `stream_bulk_klines()`
3. `barter-data/src/exchange/okx/bulk/mod.rs:272` — `stream_bulk_trades()`
4. `barter-data/src/exchange/bybit/bulk/mod.rs:189` — `stream_bulk_trades()`
5. `barter-data/src/exchange/hyperliquid/bulk/mod.rs:254,294` — `stream_bulk_trades_multi()` and `stream_bulk_trades()`
6. Tests in `barter-data/src/bulk/mod.rs:132,140,155`

**Migration note:** These call sites are inside `stream_bulk_*` methods which themselves move to the collector in Step 3E. So in practice, only the import path changes — the functions end up calling `date_range` from the same crate. The tests in `bulk/mod.rs` move with the function.

### 3B.2 Move `partition_date_range()` from `barter-data/src/exchange/okx/bulk/mod.rs:198-239`

**Current location:** `okx/bulk/mod.rs` lines 198-239
**New location:** `barter-collector/src/scheduling.rs`

Returns `(Vec<(i32, u32)>, Vec<NaiveDate>)` — complete months vs remaining days. Currently used only in tests (the OKX monthly archive optimization is prepared infrastructure, not yet wired into `stream_bulk_trades`). Move function + all 9 tests.

### 3B.3 Move `last_day_of_month()` from `barter-data/src/exchange/okx/bulk/mod.rs:242-254`

**Current location:** `okx/bulk/mod.rs` lines 242-254 (private)
**New location:** `barter-collector/src/scheduling.rs` (make `pub`)

Helper for `partition_date_range()`. Move together with its 5 tests.

### 3B.4 Move `download_monthly_trades()` from `okx/bulk/mod.rs:120-189`

**Current location:** `okx/bulk/mod.rs` lines 120-189
**New location:** Keep in `barter-data` for now — it's a single-batch download function (fetcher-level), not orchestration. It will be called by the collector's stream composers when the monthly archive optimization is wired up.

**Important:** This function contains an internal `retry_with_backoff` call (line 134). Step 3G must strip that retry wrapper to make it a single-attempt call, consistent with all other fetcher methods. Added to the 3G.1 call site list as item #12.

**Files touched:** `barter-data/src/bulk/mod.rs`, `barter-data/src/exchange/okx/bulk/mod.rs`, `barter-collector/src/scheduling.rs`
**Validation:** `cargo test -p barter-collector -- scheduling`

---

## Step 3C: Move Caching Logic

### 3C.1 Move collector-level caching functions to `barter-collector/src/caching.rs`

Move from `barter-data/src/bulk/checksum.rs`:

| Function | Lines | Rationale |
|----------|-------|-----------|
| `should_skip()` | 54-60 | Cache skip decision — orchestration concern |
| `write_verified_marker()` | 62-103 | Marker file management — orchestration concern |
| `compute_sha256()` | 23-28 | Used by marker logic, not by fetcher verification |

Move from `barter-data/src/exchange/binance/bulk/mod.rs`:

| Function | Lines | Rationale |
|----------|-------|-----------|
| `marker_path_for_url()` | 245-250 | URL→path mapping for cache — orchestration concern |

### 3C.2 Keep in `barter-data` (fetcher-level)

| Function | Lines | Rationale |
|----------|-------|-----------|
| `verify_sha256()` | checksum.rs:5-21 | Pure computation used by single-batch download verification |
| `parse_binance_checksum()` | checksum.rs:30-48 | Parsing a checksum file is fetcher-level work |

### 3C.3 Restructure `BulkConfig`

**Current `BulkConfig`** (barter-data/src/bulk/mod.rs:38-59):
```rust
pub struct BulkConfig {
    pub concurrency: usize,        // → moves to CollectorConfig
    pub verify_checksum: bool,     // stays (fetcher needs this)
    pub cache_dir: Option<PathBuf>, // → moves to CollectorConfig
}
```

**After split:**

In `barter-data/src/bulk/mod.rs`:
```rust
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BulkConfig {
    pub verify_checksum: bool,
}
```

In `barter-collector/src/config.rs`:
```rust
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CollectorConfig {
    pub concurrency: usize,
    pub cache_dir: Option<PathBuf>,
    pub retry: RetryPolicy,
    pub max_trade_pages: u32,
}
```

**Update all `BulkConfig` users:**

| File | Usage | Change |
|------|-------|--------|
| `binance/bulk/mod.rs:80` | `BinanceBulkClient.config` | Remove `concurrency` and `cache_dir` access |
| `okx/bulk/mod.rs:20` | `OkxBulkClient.config` | Same |
| `bybit/bulk/mod.rs:64` | `BybitBulkClient.config` | Same |
| `hyperliquid/bulk/mod.rs:30` | `HyperliquidBulkClient.config` | Same |
| All `with_config()` constructors | Accept `BulkConfig` | Update signatures |
| All `stream_bulk_*` methods | Read `config.concurrency` | These methods move to collector anyway |

**Files touched:** `barter-data/src/bulk/checksum.rs`, `barter-data/src/bulk/mod.rs`, `barter-data/src/exchange/binance/bulk/mod.rs`, `barter-collector/src/caching.rs`, `barter-collector/src/config.rs`
**Validation:** `cargo test -p barter-collector -- caching`, `cargo test -p barter-data --features bulk`

---

## Step 3D: Move Pagination State Machines

### 3D.1 Design generic cursor enum in `barter-collector/src/pagination.rs`

**Current state:** 9 separate per-exchange pagination structs, each with a `stream::unfold` closure. The cursor advancement strategies fall into 4 categories:

| Strategy | Used By | Cursor Type | Direction |
|----------|---------|-------------|-----------|
| **Time-cursor** | Binance klines, OKX klines, Bybit klines, Coinbase klines, Hyperliquid klines | `DateTime<Utc>` | Forward (+1ms or +1s) |
| **ID-cursor** | Binance trades (`fromId: u64`), OKX trades (`after: String`) | `String` or `u64` | Forward (Binance) / Backward (OKX) |
| **Nonce-cursor** | Kraken klines (`since: i64`), Kraken trades (`since: String`) | `String` or `i64` | Forward (opaque) |
| **Backward-cursor** | Coinbase trades (end→start, batch-then-reverse) | `DateTime<Utc>` | Backward |

**Design rationale — strategy callbacks, not a monolithic enum:**

The original plan proposed a `Cursor` enum with 4 variants (Time, Id, Nonce, Backward) to unify all pagination. However, the per-exchange behaviors differ too deeply to unify under a single enum match:
- Binance trades use a **two-phase** strategy: first request uses `startTime`/`endTime`, subsequent use `fromId` (no startTime allowed on Spot or -1128 error). Sub-window clamping via `max_trades_time_window()`.
- OKX trades paginate **backward** and reverse each batch, with a safety counter.
- Coinbase trades **collect everything** backward then reverse the whole result.
- Kraken uses **opaque nonces** with stall detection (cursor unchanged = done).

A single `stream_trades` function matching on cursor variants would re-implement all 5 exchange `stream_trades` bodies in one place — a "god function" that defeats the purpose of extraction.

**Better approach: strategy trait + per-exchange strategy impls (Arch Guide §5: adapters).**

```rust
use chrono::{DateTime, Utc};

/// Outcome of one pagination step.
#[derive(Debug)]
pub enum PageResult<T> {
    /// Yield this batch and continue with the next page.
    Continue(Vec<T>),
    /// Yield this batch and stop — no more pages.
    Done(Vec<T>),
    /// No more data — stop without yielding.
    Empty,
}

/// Strategy for paginating through a fetcher.
///
/// Each exchange provides its own implementation. The collector's
/// `stream::unfold` calls `fetch_page` in a loop until `Done`/`Empty`.
/// This replaces the 9 per-exchange `PaginationState` structs with
/// one trait + one impl per exchange×data-type combination.
///
/// Implementations are stateful (carry cursor, page count, etc.).
pub trait PaginationStrategy: Send {
    type Item: Send;

    /// Fetch one page of data. Advances internal cursor state.
    /// The collector wraps this call with retry + rate limiting.
    fn fetch_page(&mut self)
        -> Pin<Box<dyn Future<Output = Result<PageResult<Self::Item>, DataError>> + Send + '_>>;
}
```

**Per-exchange strategy impls (examples):**

```rust
/// Binance kline pagination: time-cursor forward.
pub struct BinanceKlinePagination<Server> {
    client: BinanceRestClient<Server>,
    market: String,
    interval: Interval,
    cursor: DateTime<Utc>,
    end: Option<DateTime<Utc>>,
    limit: Option<u32>,
}

impl<Server: BinanceRestServer> PaginationStrategy for BinanceKlinePagination<Server> {
    type Item = Candle;
    fn fetch_page(&mut self) -> ... {
        // Build request from self.cursor
        // Call self.client.fetch_klines(request)
        // If empty → PageResult::Empty
        // Advance self.cursor = last.close_time + 1ms
        // If cursor >= end → PageResult::Done(batch)
        // Else → PageResult::Continue(batch)
    }
}

/// OKX trade pagination: ID-based backward with reversal.
pub struct OkxTradePagination {
    client: OkxRestClient,
    market: String,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
    cursor: Option<String>,
    pages_fetched: u32,
    max_pages: u32,
}

impl PaginationStrategy for OkxTradePagination {
    type Item = RestTrade;
    fn fetch_page(&mut self) -> ... {
        // Build request with after=self.cursor
        // Call self.client.fetch_trades(request)
        // Reverse batch (OKX newest-first → oldest-first)
        // Filter by [start, end]
        // Advance self.cursor = batch.last().trade_id
        // Check termination: pages >= max, oldest < start, empty
    }
}

/// Coinbase trade pagination: backward collect-then-reverse.
pub struct CoinbaseTradePagination { ... }

/// Kraken kline pagination: nonce-based forward.
pub struct KrakenKlinePagination { ... }

/// Bybit trade pagination: single batch (no real pagination).
pub struct SingleBatchStrategy<F, T> { fetcher: F, request: ..., done: bool }
```

**Key advantages over the enum approach:**
1. Each strategy impl is self-contained — no god-function matching on cursor variants
2. Exchange-specific logic stays close to the exchange (the strategies live in `barter-collector`, grouped by exchange)
3. Adding a new exchange means adding a new strategy impl, not another variant + match arm
4. The `stream::unfold` in `streams.rs` is trivial: just call `strategy.fetch_page()` in a loop

**Pagination state is internal to each strategy.** No separate `PaginationState` struct needed — each strategy owns its cursor, page count, and done flag as fields.

**Module layout in `barter-collector/src/pagination.rs`:**

```rust
// Core trait
pub trait PaginationStrategy: Send { ... }
pub enum PageResult<T> { Continue(Vec<T>), Done(Vec<T>), Empty }

// Per-exchange strategies (can later be split into pagination/binance.rs etc.)
pub mod binance;    // BinanceKlinePagination, BinanceTradePagination
pub mod okx;        // OkxKlinePagination, OkxTradePagination
pub mod bybit;      // BybitKlinePagination, SingleBatchStrategy
pub mod coinbase;   // CoinbaseKlinePagination, CoinbaseTradePagination
pub mod kraken;     // KrakenKlinePagination, KrakenTradePagination
pub mod hyperliquid;// HyperliquidKlinePagination
```

### 3D.2 Delete per-exchange pagination structs (9 total) and their `stream::unfold` closures

These structs are replaced by `PaginationStrategy` impls in the collector. Delete both the struct and its associated `stream::unfold` closure (the `stream_klines`/`stream_trades` method body).

| # | Exchange | Struct | File | Lines |
|---|----------|--------|------|-------|
| 1 | Binance | `PaginationState<Server>` (klines) | `binance/rest/mod.rs` | 163-171 |
| 2 | Binance | `TradePaginationState<Server>` (trades) | `binance/rest/mod.rs` | 345-357 |
| 3 | OKX | `PaginationState` (klines) | `okx/rest/mod.rs` | 184-194 |
| 4 | OKX | `TradePaginationState` (trades) | `okx/rest/mod.rs` | 422-433 |
| 5 | Bybit | `PaginationState<Server>` (klines) | `bybit/rest/mod.rs` | 165-173 |
| 6 | Coinbase | `PaginationState` (klines) | `coinbase/rest/mod.rs` | 136-144 |
| 7 | Kraken | `PaginationState` (klines) | `kraken/rest/mod.rs` | 271-278 |
| 8 | Kraken | `TradePaginationState` (trades) | `kraken/rest/mod.rs` | 408-416 |
| 9 | Hyperliquid | `PaginationState` (klines) | `hyperliquid/rest/mod.rs` | 136-146 |

**Note:** Coinbase trades and Bybit trades don't have pagination structs — Coinbase uses inline mutable variables in an async block (lines 424-507), Bybit uses `stream::once` (single batch, line 455). Both patterns are still deleted; the logic moves into `CoinbaseTradePagination` and `SingleBatchStrategy` respectively.

### 3D.3 Move `MAX_TRADE_PAGES` to `CollectorConfig`

**Current:** `const MAX_TRADE_PAGES: u32 = 10_000;` in `okx/rest/mod.rs:415`
**New:** `CollectorConfig.max_trade_pages: u32` with default `10_000`

### 3D.4 Move `filter_trades_by_time` to `barter-collector/src/filters.rs`

**Current location:** `okx/rest/mod.rs:386-408`

```rust
/// Filter trades to a [start, end] time range.
pub fn filter_trades_by_time(
    trades: Vec<RestTrade>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Vec<RestTrade> { ... }
```

**Call sites (2):**
- `okx/rest/mod.rs:486` — in `fetch_trades()`
- `okx/rest/mod.rs:630` — in `stream_trades()` pagination closure

Both these call sites are in `stream_trades()` which moves to the collector. The single-batch `fetch_trades()` call can be replaced by the collector calling `filter_trades_by_time` after receiving the batch.

**Files touched:** All exchange `rest/mod.rs` files (delete structs + `stream::unfold` closures), `barter-collector/src/pagination.rs`, `barter-collector/src/filters.rs`, `barter-collector/src/config.rs`
**Validation:** `cargo check -p barter-collector`, `cargo test -p barter-collector -- pagination`

---

## Step 3E: Move Stream Composition

### 3E.1 Create generic stream composers in `barter-collector/src/streams.rs`

This is the core of the collector. Three generic stream functions replace all per-exchange `stream_*` trait methods.

#### `stream_paginated` — single generic driver for all REST pagination

Replaces all 9 per-exchange `stream_klines()` and `stream_trades()` implementations.

```rust
use crate::pagination::{PaginationStrategy, PageResult};
use crate::config::CollectorConfig;

/// Stream data by driving a PaginationStrategy in a loop.
///
/// Uses `stream::unfold` with the strategy as state.
/// Wraps each `strategy.fetch_page()` call with `retry_with_backoff`.
/// Rate limiter is acquired INSIDE the retry loop (fixes the rate-limiter bug).
pub fn stream_paginated<S: PaginationStrategy + 'static>(
    strategy: S,
    config: &CollectorConfig,
    rate_limiter: Arc<ExchangeRateLimiter>,
) -> Pin<Box<dyn Stream<Item = Result<Vec<S::Item>, DataError>> + Send>> {
    Box::pin(stream::unfold(strategy, move |mut strategy| {
        let retry = config.retry.clone();
        let limiter = rate_limiter.clone();
        async move {
            let result = retry_with_backoff(&retry, is_retriable_data_error, || {
                async {
                    limiter.until_ready().await;
                    strategy.fetch_page().await
                }
            }).await;

            match result {
                Ok(PageResult::Continue(batch)) => Some((Ok(batch), strategy)),
                Ok(PageResult::Done(batch)) if !batch.is_empty() => Some((Ok(batch), /* mark done */)),
                Ok(PageResult::Done(_)) | Ok(PageResult::Empty) => None,
                Err(e) => Some((Err(e), strategy)),  // yield error, stop
            }
        }
    }))
}
```

**This eliminates the need for separate `stream_klines`/`stream_trades` functions.** The exchange-specific behavior is fully encapsulated in the `PaginationStrategy` impl. The stream driver is ~20 lines of generic code.

**Usage examples:**

```rust
// Binance klines
let strategy = BinanceKlinePagination::new(client, market, interval, start, end, limit);
let stream = stream_paginated(strategy, &config, rate_limiter);

// OKX trades (backward with reversal)
let strategy = OkxTradePagination::new(client, market, start, end, config.max_trade_pages);
let stream = stream_paginated(strategy, &config, rate_limiter);

// Coinbase trades (collect-backward-reverse handled inside CoinbaseTradePagination)
let strategy = CoinbaseTradePagination::new(client, market, start, end);
let stream = stream_paginated(strategy, &config, rate_limiter);

// Bybit trades (single batch — trivially wraps fetch_trades in a one-shot strategy)
let strategy = SingleBatchStrategy::new(|| client.fetch_trades(request));
let stream = stream_paginated(strategy, &config, rate_limiter);
```

**Exchange-specific behaviors handled inside each strategy impl:**

| Exchange | Strategy | Internal Behavior |
|----------|----------|-------------------|
| **Binance klines** | `BinanceKlinePagination` | Time-cursor: advance past `close_time + 1ms` |
| **Binance trades** | `BinanceTradePagination` | Two-phase: first request uses time window, subsequent use `fromId`. Sub-window clamping via `max_trades_time_window()` |
| **OKX klines** | `OkxKlinePagination` | Time-cursor with OKX `before`/`after` semantics reversal |
| **OKX trades** | `OkxTradePagination` | ID-based backward, reverse each batch, safety counter (max pages) |
| **Coinbase klines** | `CoinbaseKlinePagination` | Time-cursor: advance past `close_time + 1s` (seconds-based) |
| **Coinbase trades** | `CoinbaseTradePagination` | Backward collect: `fetch_page` internally collects all pages, yields one giant `PageResult::Done(reversed)` |
| **Kraken klines** | `KrakenKlinePagination` | Nonce-cursor: advance by opaque `last` from response, stall detection |
| **Kraken trades** | `KrakenTradePagination` | Nonce-cursor: nanosecond string nonces, filter by end time |
| **Bybit klines** | `BybitKlinePagination` | Time-cursor: advance past `open_time + 1ms` (Bybit uses open_time) |
| **Bybit trades** | `SingleBatchStrategy` | One-shot: yields single batch then `Empty` |
| **Hyperliquid klines** | `HyperliquidKlinePagination` | Time-cursor: advance past `close_time + 1ms`, POST body requires both start+end |

#### `stream_bulk`

Replaces 5 per-exchange `stream_bulk_trades()` and 1 `stream_bulk_klines()` implementation.

```rust
use barter_data::bulk::{BulkDayTradeFetcher, BulkDayKlineFetcher};

/// Stream bulk trades by fanning out over a date range with bounded concurrency.
///
/// Generic over any fetcher that implements `BulkDayTradeFetcher`.
/// Uses `stream::iter(dates).map(|d| fetcher.fetch_day_trades(...)).buffer_unordered(concurrency)`.
pub fn stream_bulk_trades<'a, F: BulkDayTradeFetcher + 'a>(
    fetcher: &'a F,
    market: &'a str,
    dates: Vec<NaiveDate>,
    config: &'a CollectorConfig,
) -> Pin<Box<dyn Stream<Item = Result<Vec<RestTrade>, DataError>> + Send + 'a>> {
    Box::pin(
        stream::iter(dates)
            .map(move |date| fetcher.fetch_day_trades(market, date))
            .buffer_unordered(config.concurrency)
            .filter_map(|result| async {
                match result {
                    Ok(None) => None,              // 404 — skip silently
                    Ok(Some(data)) => Some(Ok(data)),
                    Err(e) => Some(Err(e)),
                }
            })
    )
}

/// Stream bulk klines — same pattern, different trait.
pub fn stream_bulk_klines<'a, F: BulkDayKlineFetcher + 'a>(
    fetcher: &'a F,
    market: &'a str,
    interval: Interval,
    dates: Vec<NaiveDate>,
    config: &'a CollectorConfig,
) -> Pin<Box<dyn Stream<Item = Result<Vec<Candle>, DataError>> + Send + 'a>> {
    Box::pin(
        stream::iter(dates)
            .map(move |date| fetcher.fetch_day_klines(market, interval, date))
            .buffer_unordered(config.concurrency)
            .filter_map(|result| async {
                match result {
                    Ok(None) => None,
                    Ok(Some(data)) => Some(Ok(data)),
                    Err(e) => Some(Err(e)),
                }
            })
    )
}
```

All 5 exchange bulk implementations follow this identical pattern today:
- Binance trades: `date_range → map(download_and_parse_trades) → buffer_unordered → filter_map`
- Binance klines: same
- OKX trades: same (with +1 day extension for UTC+8)
- Bybit trades: same
- Hyperliquid trades: same

The exchange-specific part (URL construction + parsing) is the `BulkDayTradeFetcher::fetch_day_trades` impl — no closures needed.

### 3E.2 Handle Coinbase backward pagination

Coinbase trades use a unique collect-then-reverse pattern (not `stream::unfold`). The current implementation at `coinbase/rest/mod.rs:424-507` collects all batches in a Vec, reverses, then yields via `stream::iter`.

**With the strategy pattern, this is cleanly encapsulated:** `CoinbaseTradePagination::fetch_page()` internally loops backward collecting all batches, then returns a single `PageResult::Done(all_trades_reversed)`. The `stream_paginated` driver sees one page result and stops — no special-casing needed.

### 3E.3 Handle Hyperliquid multi-coin bulk streaming

`HyperliquidBulkClient::stream_bulk_trades_multi()` at `hyperliquid/bulk/mod.rs:249-280` returns `impl Stream<Item = Result<HashMap<String, Vec<RestTrade>>, DataError>>`. This is a higher-level orchestration method that stays in the collector as a specialized stream composer.

### 3E.4 Fix rate-limiter-outside-retry bug

**Current bug:** All 6 REST clients call `self.wait_for_rate_limit().await` BEFORE the `retry_with_backoff()` call. On retry, the rate limiter is not re-acquired, so retries can burst past rate limits.

**Affected files (every `fetch_*` method):**

| Exchange | File | Lines | Method |
|----------|------|-------|--------|
| Binance | `binance/rest/mod.rs` | 226 | `fetch_klines()` |
| Binance | `binance/rest/mod.rs` | 395 | `fetch_trades()` |
| OKX | `okx/rest/mod.rs` | 149 | `fetch_trades_raw()` |
| OKX | `okx/rest/mod.rs` | 248 | `fetch_klines()` |
| Bybit | `bybit/rest/mod.rs` | 228 | `fetch_klines()` |
| Bybit | `bybit/rest/mod.rs` | 395 | `fetch_trades()` |
| Coinbase | `coinbase/rest/mod.rs` | 198 | `fetch_klines()` |
| Coinbase | `coinbase/rest/mod.rs` | 354 | `fetch_trades()` |
| Kraken | `kraken/rest/mod.rs` | 164 | `fetch_raw()` |
| Kraken | `kraken/rest/mod.rs` | 225 | `fetch_trades_raw()` |
| Hyperliquid | `hyperliquid/rest/mod.rs` | 211 | `fetch_klines()` |

**Fix:** In the collector's stream composers, move rate limiter acquisition INSIDE the retry closure:

```rust
retry_with_backoff(&config.retry, is_retriable_data_error, || {
    let fetcher = fetcher.clone();
    let request = request.clone();
    async move {
        fetcher.wait_for_rate_limit().await;  // INSIDE retry loop
        fetcher.fetch_klines(request).await
    }
})
```

Since `fetch_klines`/`fetch_trades` in `barter-data` will no longer do their own retry (Step 3G), the rate limiter call inside those methods gets removed too. The collector owns both retry and rate limiting.

**Files touched:** All exchange `rest/mod.rs` files (delete `stream_*` methods), all exchange `bulk/mod.rs` files (delete `stream_bulk_*` methods), `barter-collector/src/streams.rs`
**Validation:** `cargo test --workspace --all-features`

---

## Step 3F: Slim `barter-data` Traits

### 3F.1 Slim REST traits in `barter-data/src/rest/mod.rs`

**Current `KlineFetcher` trait (lines 29-50):**
```rust
pub trait KlineFetcher {
    fn supported_intervals() -> &'static [Interval];
    fn fetch_klines(&self, request: KlineRequest) -> impl Future<...> + Send;
    fn stream_klines(&self, request: KlineRequest) -> impl Stream<...> + Send;  // DELETE
}
```

**After:**
```rust
pub trait KlineFetcher: Send + Sync {
    fn supported_intervals() -> &'static [Interval];
    fn fetch_klines(&self, request: KlineRequest)
        -> impl Future<Output = Result<Vec<Candle>, DataError>> + Send;
}
```

**Current `TradeFetcher` trait (lines 84-131):**
```rust
pub trait TradeFetcher: Send + Sync {
    fn fetch_trades(&self, request: TradeRequest)
        -> Pin<Box<dyn Future<...> + Send + '_>>;
    fn stream_trades(&self, request: TradeRequest)      // DELETE
        -> Pin<Box<dyn Stream<...> + Send + '_>>;
}
```

**After:**
```rust
pub trait TradeFetcher: Send + Sync {
    fn fetch_trades(&self, request: TradeRequest)
        -> Pin<Box<dyn Future<Output = Result<Vec<RestTrade>, DataError>> + Send + '_>>;
}
```

**Delete `TradeRequest.initial_cursor`:** Field at line 70. Cursor carriage is the collector's job. Remove from struct and all construction sites:
- `okx/rest/mod.rs:522` — `state.cursor = request.initial_cursor`
- `coinbase/rest/mod.rs:460` — similar

### 3F.2 Slim bulk traits in `barter-data/src/bulk/mod.rs`

**Current `BulkTradeFetcher` (lines 65-70):**
```rust
pub trait BulkTradeFetcher: Send + Sync {
    fn stream_bulk_trades(&self, request: BulkTradeRequest)  // DELETE
        -> Pin<Box<dyn Stream<...> + Send + '_>>;
}
```

**Replace with single-day fetcher:**
```rust
pub trait BulkDayTradeFetcher: Send + Sync {
    fn fetch_day_trades(
        &self,
        market: &str,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<RestTrade>>, DataError>> + Send + '_>>;
}
```

**Current `BulkKlineFetcher` (lines 75-80):**
```rust
pub trait BulkKlineFetcher {  // Missing Send + Sync!
    fn stream_bulk_klines(&self, request: BulkKlineRequest)  // DELETE
        -> impl Stream<...> + Send;
}
```

**Replace with:**
```rust
pub trait BulkDayKlineFetcher: Send + Sync {  // Fix: add Send + Sync
    fn fetch_day_klines(
        &self,
        market: &str,
        interval: Interval,
        date: NaiveDate,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Vec<Candle>>, DataError>> + Send + '_>>;
}
```

**Delete `BulkConfig.concurrency` and `BulkConfig.cache_dir`** — already moved to `CollectorConfig` in Step 3C.3.

### 3F.3 Update each exchange to implement slimmed traits

**Per-exchange changes:**

| Exchange | Current Method | New Method | Key Change |
|----------|---------------|------------|------------|
| **Binance** | `download_and_parse_trades()` (free fn, lines 281-303) | `impl BulkDayTradeFetcher for BinanceBulkClient<Server>` | Wrap existing fn as trait method |
| **Binance** | `download_and_parse_klines()` (free fn, lines 307-326) | `impl BulkDayKlineFetcher for BinanceBulkClient<Server>` | Same |
| **OKX** | `self.download_and_parse_trades()` (method, lines 41-110) | `impl BulkDayTradeFetcher for OkxBulkClient` | Strip retry wrapper (Step 3G) |
| **Bybit** | `self.download_and_parse_trades()` (method, lines 107-181) | `impl BulkDayTradeFetcher for BybitBulkClient<Server>` | Strip retry wrapper |
| **Kraken** | `parse_zip_trades()` (static, lines 15-87) | `impl BulkDayTradeFetcher for KrakenArchiveParser` | Adapt local parser to trait |
| **Hyperliquid** | `self.download_and_parse_trades()` (method, lines 188-213) | `impl BulkDayTradeFetcher for HyperliquidBulkClient` | Single-market day download; 24-hour fan-out stays |

**Hyperliquid detail:** The 24-hour fan-out within a single day (lines 188-213, `try_join_all` over 24 hours) is a fetcher-level concern — it's how Hyperliquid's archive is structured. It stays in `barter-data`. The day-level concurrency (`buffer_unordered` across dates) moves to the collector.

### 3F.4 Delete factory functions from `barter-data`

**Delete `rest_client()`** at `barter-data/src/rest/mod.rs:137-160`
- Creates `Box<dyn TradeFetcher>` by ExchangeId
- This is orchestration logic — recreate in `barter-collector` if needed

**Delete `bulk_client()`** at `barter-data/src/bulk/mod.rs:101-122`
- Creates `Box<dyn BulkTradeFetcher>` by ExchangeId
- Same rationale — recreate in `barter-collector`

**Files touched:** `barter-data/src/rest/mod.rs`, `barter-data/src/bulk/mod.rs`, all 6 exchange `rest/mod.rs` files, all 5 exchange `bulk/mod.rs` files
**Validation:** `cargo check -p barter-data --all-features`, `cargo check -p barter-collector`

---

## Step 3G: Strip Retry from Fetcher Internals

### 3G.1 Remove `retry_with_backoff` from exchange `fetch_*` and `download_*` methods

**All 12 call sites inside exchange methods become direct calls:**

| # | Exchange | File | Line | Method | Change |
|---|----------|------|------|--------|--------|
| 1 | Binance | `binance/rest/mod.rs` | 229 | `fetch_klines()` | Remove retry wrapper, keep `client.execute(req)` |
| 2 | Binance | `binance/rest/mod.rs` | 395 | `fetch_trades()` | Same |
| 3 | OKX | `okx/rest/mod.rs` | 152 | `fetch_trades_raw()` | Same |
| 4 | OKX | `okx/rest/mod.rs` | 252 | `fetch_klines()` | Same |
| 5 | Bybit | `bybit/rest/mod.rs` | 232 | `fetch_klines()` | Same |
| 6 | Bybit | `bybit/rest/mod.rs` | 399 | `fetch_trades()` | Same |
| 7 | Coinbase | `coinbase/rest/mod.rs` | 201 | `fetch_klines()` | Same |
| 8 | Coinbase | `coinbase/rest/mod.rs` | 358 | `fetch_trades()` | Same |
| 9 | Kraken | `kraken/rest/mod.rs` | 168 | `fetch_raw()` | Same |
| 10 | Kraken | `kraken/rest/mod.rs` | 229 | `fetch_trades_raw()` | Same |
| 11 | Hyperliquid | `hyperliquid/rest/mod.rs` | 215 | `fetch_klines()` | Same |
| 12 | OKX | `okx/bulk/mod.rs` | 134 | `download_monthly_trades()` | Remove retry wrapper (stays in barter-data as single-attempt) |

**Also remove `wait_for_rate_limit()` calls** from each method (11 sites listed in 3E.4). Rate limiting moves to the collector.

**Note:** Keep `wait_for_rate_limit()` as a method on the client structs — the collector will call it. Or better: expose the rate limiter via an accessor so the collector can call `until_ready()` directly.

### 3G.2 Collector wraps fetcher calls with retry

In `barter-collector/src/streams.rs`, each stream composer wraps calls:

```rust
// Inside stream_klines unfold closure:
let batch = retry_with_backoff(&config.retry, is_retriable_data_error, || {
    async {
        fetcher.rate_limiter().until_ready().await;
        fetcher.fetch_klines(request.clone()).await
    }
}).await?;
```

### 3G.3 Remove `RetryPolicy` from bulk client structs

**Current:** All 4 bulk clients store `pub retry: RetryPolicy`:
- `BinanceBulkClient<Server>` at `binance/bulk/mod.rs:82`
- `OkxBulkClient` at `okx/bulk/mod.rs:21`
- `BybitBulkClient<Server>` at `bybit/bulk/mod.rs:66`
- `HyperliquidBulkClient` at `hyperliquid/bulk/mod.rs:32`

**Also:** `HyperliquidRestClient` at `hyperliquid/rest/mod.rs:81`

**Change:** Remove `retry` field from all. `CollectorConfig.retry` is the single source.

### 3G.4 Move `download_bytes_resumable` to `barter-collector`

**Current locations:**
- `binance/bulk/mod.rs:150-224` — Binance version (1 MB threshold)
- `hyperliquid/bulk/mod.rs:70-172` — Hyperliquid version (identical pattern with S3 auth)

**New location:** `barter-collector/src/caching.rs` (or a new `barter-collector/src/download.rs`)

Create a single generic resumable download function:

```rust
/// Download bytes with retry and HTTP Range resume.
///
/// On retry after >= `resume_threshold` bytes received, sends
/// `Range: bytes=N-` to resume. Handles 200/206/404/416 responses.
pub async fn download_bytes_resumable(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    url: &str,
    extra_headers: Option<HeaderMap>,  // For S3 auth (Hyperliquid)
    resume_threshold: usize,           // 1 MB default
) -> Result<Option<Vec<u8>>, DataError> { ... }
```

### 3G.5 Replace `std::sync::Mutex` with `tokio::sync::Mutex`

**Current:** `download_bytes_resumable` uses `Arc<std::sync::Mutex<Vec<u8>>>` for partial buffer state:
- `binance/bulk/mod.rs:158` — `let partial = std::sync::Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));`
- `hyperliquid/bulk/mod.rs:80` — same pattern

**Analysis:** The current code is technically safe because the Mutex is never held across `.await` points — each lock/unlock happens within a synchronous block. However, per the plan's constraint (Rusty §9), we should either:

1. Use `tokio::sync::Mutex` for correctness signaling
2. **Eliminate shared state entirely** — since `retry_with_backoff` calls the closure sequentially (not concurrently), the `Arc<Mutex>` is unnecessary. Use a simple `&mut Vec<u8>` carried through the retry state.

**Recommendation:** Option 2. Restructure `download_bytes_resumable` to own the buffer directly:

```rust
pub async fn download_bytes_resumable(...) -> Result<Option<Vec<u8>>, DataError> {
    let mut partial = Vec::new();
    let mut backoff = retry.initial_backoff;

    for attempt in 0..=retry.max_retries {
        let existing_len = partial.len();
        let mut request = client.get(url);
        if existing_len >= resume_threshold {
            request = request.header("Range", format!("bytes={existing_len}-"));
        }
        // ... handle response, append to partial or clear on 200/416
        // On retriable error: sleep(backoff), continue
        // On success: return Ok(Some(partial))
    }
    // Final attempt failed
    Err(last_error)
}
```

This eliminates the Mutex entirely while preserving the partial-buffer-across-retries behavior.

**Files touched:** All 6 exchange `rest/mod.rs` files, all 4 exchange `bulk/mod.rs` files, `barter-data/src/retry.rs` (stays but retry calls removed from fetchers), `barter-collector/src/streams.rs`
**Validation:** `cargo test --workspace --all-features`. Zero `retry_with_backoff` calls in `barter-data` outside actual `retry.rs` definition. Zero `stream::unfold` in `barter-data`. Zero `buffer_unordered` in `barter-data`.

---

## Step 3H: Move `retry.rs` Utilities

### 3H.1 Move retry module to collector

**Current:** `barter-data/src/retry.rs` contains `RetryPolicy`, `retry_with_backoff`, `is_retriable_data_error`

**After Step 3G:** No code in `barter-data` calls these functions. Move the entire module to `barter-collector/src/retry.rs`.

**Alternative:** If `barter-data` still needs retry for some edge case, keep `RetryPolicy` and `retry_with_backoff` as shared utilities (possibly in `barter-integration`) and only move `is_retriable_data_error` to the collector. But per the plan, fetchers become single-attempt, so the full module moves.

Also move the re-export from `barter-data/src/rest/mod.rs` which currently has:
```rust
pub mod retry;
```

at `barter-data/src/rest/retry.rs` — this file appears to exist but is likely a re-export. Check and clean up.

---

## Execution Order & Dependencies

```
3A (skeleton)
 └→ 3B (scheduling) ─────────────────────────┐
 └→ 3C (caching + config) ───────────────────┤
 └→ 3D (pagination types) ───────────────────┤
                                               ├→ 3E (stream composers)
 3F (slim traits) ────────────────────────────┤     └→ 3H (move retry)
 3G (strip retry from fetchers) ──────────────┘
```

- **3A** must be first (creates the crate)
- **3B, 3C, 3D** are independent of each other, can be done in parallel
- **3F** (slim traits) is independent and can be done in parallel with 3B-3D
- **3G** (strip retry) depends on 3F (traits must be slimmed first) and 3E (collector must own retry)
- **3E** (stream composers) depends on all of 3B, 3C, 3D, 3F (needs types + slimmed traits)
- **3H** (move retry module) depends on 3G (all callers removed)

**Recommended implementation order:**
1. **3A** — Create crate skeleton, verify `cargo check`
2. **3B + 3C + 3D + 3F** — In parallel: move utilities and slim traits
3. **3E** — Write stream composers (the core logic)
4. **3G** — Strip retry from fetchers
5. **3H** — Move retry module
6. Final validation: `cargo test --workspace --all-features`

---

## Validation Checklist

After all steps complete:

- [ ] `cargo check --workspace --all-features` — clean
- [ ] `cargo test --workspace --all-features` — passes
- [ ] `cargo clippy --workspace --all-features` — clean
- [ ] Zero `stream::unfold` in `barter-data` (search: `grep -r "stream::unfold" barter-data/`)
- [ ] Zero `buffer_unordered` in `barter-data` (search: `grep -r "buffer_unordered" barter-data/`)
- [ ] Zero `retry_with_backoff` calls in `barter-data` (only definition may remain if shared)
- [ ] Zero `PaginationState` / `TradePaginationState` structs in `barter-data`
- [ ] `barter-data` traits have no `stream_*` methods
- [ ] `BulkConfig` has no `concurrency` or `cache_dir` fields
- [ ] No `std::sync::Mutex` in async download paths
- [ ] Rate limiter acquired inside retry loop in all collector stream composers
- [ ] `CollectorConfig` owns: concurrency, cache_dir, retry policy, max_trade_pages

---

## Files Inventory

### New files (barter-collector)

| File | Content |
|------|---------|
| `barter-collector/Cargo.toml` | Crate manifest |
| `barter-collector/src/lib.rs` | Module declarations |
| `barter-collector/src/config.rs` | `CollectorConfig` struct |
| `barter-collector/src/pagination.rs` | `PaginationStrategy` trait, `PageResult` enum, per-exchange strategy impls (may later split into `pagination/binance.rs` etc.) |
| `barter-collector/src/scheduling.rs` | `date_range`, `partition_date_range`, `last_day_of_month` |
| `barter-collector/src/caching.rs` | `should_skip`, `write_verified_marker`, `compute_sha256`, `marker_path_for_url` |
| `barter-collector/src/streams.rs` | `stream_klines`, `stream_trades`, `stream_bulk` |
| `barter-collector/src/filters.rs` | `filter_trades_by_time` |
| `barter-collector/src/retry.rs` | `RetryPolicy`, `retry_with_backoff`, `is_retriable_data_error` (moved from barter-data) |
| `barter-collector/src/download.rs` | `download_bytes_resumable` (unified from Binance + Hyperliquid) |

### Modified files (barter-data)

| File | Changes |
|------|---------|
| `src/rest/mod.rs` | Delete `stream_klines` from `KlineFetcher`, delete `stream_trades` from `TradeFetcher`, delete `initial_cursor` from `TradeRequest`, delete `rest_client()` |
| `src/bulk/mod.rs` | Replace `BulkTradeFetcher`/`BulkKlineFetcher` with `BulkDayTradeFetcher`/`BulkDayKlineFetcher`, slim `BulkConfig`, delete `bulk_client()`, delete `date_range()` |
| `src/bulk/checksum.rs` | Delete `should_skip`, `write_verified_marker`, `compute_sha256`; keep `verify_sha256`, `parse_binance_checksum` |
| `src/retry.rs` | Moved to barter-collector (or kept as shared if needed) |
| `exchange/binance/rest/mod.rs` | Delete `PaginationState`, `TradePaginationState`, `stream_klines()`, `stream_trades()`; remove retry wrapper + rate limit from `fetch_klines()`/`fetch_trades()` |
| `exchange/binance/bulk/mod.rs` | Delete `stream_bulk_trades()`, `stream_bulk_klines()`, `download_bytes_resumable()`, `download_and_verify()` cache logic; implement `BulkDayTradeFetcher`/`BulkDayKlineFetcher`; remove `RetryPolicy` from struct |
| `exchange/okx/rest/mod.rs` | Delete `PaginationState`, `TradePaginationState`, `stream_klines()`, `stream_trades()`, `filter_trades_by_time()`, `MAX_TRADE_PAGES`; remove retry + rate limit from `fetch_*` |
| `exchange/okx/bulk/mod.rs` | Delete `stream_bulk_trades()`, `partition_date_range()`, `last_day_of_month()`; implement `BulkDayTradeFetcher`; remove `RetryPolicy` |
| `exchange/bybit/rest/mod.rs` | Delete `PaginationState`, `stream_klines()`, `stream_trades()`; remove retry + rate limit from `fetch_*` |
| `exchange/bybit/bulk/mod.rs` | Delete `stream_bulk_trades()`; implement `BulkDayTradeFetcher`; remove `RetryPolicy` |
| `exchange/coinbase/rest/mod.rs` | Delete `PaginationState`, `stream_klines()`, `stream_trades()`; remove retry + rate limit from `fetch_*` |
| `exchange/kraken/rest/mod.rs` | Delete `PaginationState`, `TradePaginationState`, `stream_klines()`, `stream_trades()`; remove retry + rate limit from `fetch_*` |
| `exchange/kraken/bulk/mod.rs` | Adapt `KrakenArchiveParser` to implement `BulkDayTradeFetcher` |
| `exchange/hyperliquid/rest/mod.rs` | Delete `PaginationState`, `stream_klines()`; remove retry + rate limit from `fetch_klines()`; remove `RetryPolicy` from struct |
| `exchange/hyperliquid/bulk/mod.rs` | Delete `stream_bulk_trades()`, `stream_bulk_trades_multi()`, `download_bytes_resumable` equivalent; implement `BulkDayTradeFetcher`; remove `RetryPolicy` |

### Root workspace

| File | Changes |
|------|---------|
| `Cargo.toml` | Add `barter-collector` to members + workspace.dependencies |
