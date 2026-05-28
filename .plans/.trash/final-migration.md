# Final Migration Plan: collector-daemon → Corrected barter-data

## Context

collector-daemon (`/home/molaco/Documents/collector-daemon/`) depends on `../barter-rs/barter-data` (pre-correction, `feat/oss-improvements` branch). The corrected barter-data (at `../barter-rs-correction`) removed streaming methods, factory functions, renamed traits, slimmed `BulkConfig`, and stripped internal retry/rate-limiting from fetcher methods.

**Design principle:** collector-daemon calls barter-data's single-batch APIs directly and manages its own loops, retry, rate limiting, and concurrency. It does NOT depend on `barter-collector`.

**Scope:** ~26 broken call sites across 6 files in `crates/collector-backfill/` + 1 in `crates/collector/`.

---

## Pre-flight

```bash
# Verify current state compiles against old barter-data
cd /home/molaco/Documents/collector-daemon
cargo check --workspace  # should pass (currently does)
```

---

## Step 1: Restore `initial_cursor` on `TradeRequest` in barter-data

**Repo:** `barter-rs-correction`
**File:** `barter-data/src/rest/mod.rs`

Add back the `initial_cursor` field:

```rust
pub struct TradeRequest {
    pub market: String,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    pub initial_cursor: Option<String>,
}
```

**Why this is correct:** `initial_cursor` is an exchange API parameter (OKX's `after` trade ID for backward pagination), not orchestration logic. The daemon's `catchup.rs` chains 1-hour chunks by carrying the cursor from one chunk's `BackfillResult.last_cursor` to the next chunk's `TradeRequest.initial_cursor`. Without it, each chunk would redundantly traverse from the newest trade.

**Also update:** Any test or internal code in barter-data that constructs `TradeRequest` must include the field. The crate-root re-export already includes `TradeRequest`.

**Commit in barter-rs-correction.** Verify: `cargo test -p barter-data --all-features`

---

## Step 2: Expose `stream_bulk_trades_multi` for Hyperliquid

**Repo:** `barter-rs-correction`
**File:** `barter-data/src/exchange/hyperliquid/bulk/mod.rs`

The daemon's `hyperliquid.rs:109` calls `client.stream_bulk_trades_multi(start_date, end_date)` — a streaming method that yields `HashMap<String, Vec<RestTrade>>` per hourly S3 file (all coins in one download). This was deleted in Phase 3.

**Action:** Re-expose `stream_bulk_trades_multi` as a public inherent method on `HyperliquidBulkClient`. It's a Hyperliquid-specific optimization (one S3 file contains all coins — downloading per-coin would be wasteful).

This is an exception to the "no streaming in barter-data" rule because it's an exchange-specific data format concern (the S3 file is inherently multi-coin), not generic pagination orchestration.

**Alternatively:** The daemon can call `download_and_parse_trades_multi(date)` per-day in its own loop. But the date iteration + `buffer_unordered` is trivial for the daemon to own. Expose the per-day method as public instead:

```rust
pub async fn fetch_day_trades_multi(
    &self,
    date: NaiveDate,
) -> Result<Option<HashMap<String, Vec<RestTrade>>>, DataError>
```

The daemon iterates dates and fans out per-coin. This is cleaner than re-adding a streaming method.

**Commit in barter-rs-correction.** Verify: `cargo test -p barter-data --all-features`

---

## Step 3: Point daemon to corrected barter-data

**Repo:** `collector-daemon`
**File:** `Cargo.toml` (workspace root)

```toml
# Change:
barter-data = { path = "../barter-rs/barter-data", features = ["rest", "bulk"] }
# To:
barter-data = { path = "../barter-rs-correction/barter-data", features = ["rest", "bulk"] }
```

Run `cargo check --workspace 2>&1 | head -100` to see all errors. This is the inventory of everything that needs fixing.

**Expected errors:** ~26 sites covering deleted `stream_trades`, `stream_bulk_trades`, `bulk_client`, `rest_client`, renamed `BulkTradeFetcher`, slimmed `BulkConfig`.

**Also verify:** No errors in `crates/collector/src/recorder.rs`, `crates/collector-candle/`, `crates/collector-storage/`, `crates/collector-api/`. These use WebSocket/Candle/subscription types which are unchanged. If they error, the scope is larger than expected — investigate before proceeding.

---

## Step 4: Create local factory functions

**Repo:** `collector-daemon`
**File:** New `crates/collector-backfill/src/factory.rs`

Recreate the deleted `bulk_client()` and `rest_client()` factories locally. These are orchestration/wiring code that belongs in the daemon, not barter-data (Arch Guide §5 — composition root).

```rust
use barter_data::bulk::{BulkConfig, BulkDayTradeFetcher};
use barter_data::rest::{ExchangeRateLimiter, TradeFetcher};
use barter_instrument::exchange::ExchangeId;
use std::sync::Arc;

pub fn bulk_client(
    exchange_id: ExchangeId,
    config: BulkConfig,
) -> Option<Box<dyn BulkDayTradeFetcher>> {
    use barter_data::exchange::{
        binance::{bulk::BinanceBulkClient, futures::BinanceServerFuturesUsd, spot::BinanceServerSpot},
        bybit::{bulk::BybitBulkClient, futures::BybitServerPerpetualsUsd, spot::BybitServerSpot},
        hyperliquid::bulk::HyperliquidBulkClient,
        okx::bulk::OkxBulkClient,
    };
    // Same match logic as the deleted barter_data::bulk::bulk_client
    match exchange_id {
        ExchangeId::BinanceSpot => Some(Box::new(BinanceBulkClient::<BinanceServerSpot>::with_config(config))),
        // ... all variants
        _ => None,
    }
}

pub fn rest_client(
    exchange_id: ExchangeId,
    rate_limiter: Option<Arc<ExchangeRateLimiter>>,
) -> Option<Box<dyn TradeFetcher>> {
    // Same match logic as the deleted barter_data::rest::rest_client
    // ...
}
```

**Update imports** in all files that used `barter_data::bulk::bulk_client` or `barter_data::rest::rest_client`:

| File | Old import | New import |
|------|-----------|------------|
| `archive/download.rs:6` | `use barter_data::bulk::{bulk_client, ...}` | `use crate::factory::bulk_client` |
| `archive/download.rs:8` | `use barter_data::rest::{rest_client, ...}` | `use crate::factory::rest_client` |
| `lib.rs:12` | `pub use barter_data::rest::{rest_client, ...}` | `pub use crate::factory::rest_client` |
| `online.rs:11` | `use crate::{rest_client, ...}` | Already uses crate re-export — update `lib.rs` re-export |
| `main.rs:1023` | `backfill::rest_client(...)` | Works via updated lib.rs re-export |

**Call sites** (2 `bulk_client` + 6 `rest_client`): No logic changes — just import path.

**Declare module** in `crates/collector-backfill/src/lib.rs`:
```rust
pub mod factory;
pub use factory::{bulk_client, rest_client};
```

---

## Step 5: Update trait names and `BulkConfig`

**Repo:** `collector-daemon`

### 5a. Rename `BulkTradeFetcher` → `BulkDayTradeFetcher`

| File | Line | Change |
|------|------|--------|
| `archive/download.rs:6` | import | `BulkTradeFetcher` → `BulkDayTradeFetcher` |
| `archive/download.rs:19` | param type | `&dyn BulkTradeFetcher` → `&dyn BulkDayTradeFetcher` |

### 5b. Slim `BulkConfig` construction

All 3 sites set the same pattern. Remove `concurrency` and `cache_dir`:

```rust
// Before:
let bulk_config = BulkConfig {
    concurrency: opts.concurrency,
    verify_checksum: true,
    cache_dir: None,
};

// After:
let bulk_config = BulkConfig::new(true); // verify_checksum: true
```

The `opts.concurrency` value is used later in Step 6 for the daemon's own `buffered()` concurrency.

| File | Line |
|------|------|
| `archive/download.rs:123-127` | `run_archive_backfill` |
| `archive/download.rs:412-416` | `run_archive_phase` |
| `archive/hyperliquid.rs:92-96` | `run_hyperliquid_multi_backfill` |

### 5c. Verify `BulkTradeRequest` unchanged

`BulkTradeRequest { market, start, end }` — fields unchanged in the correction. No migration needed.

---

## Step 6: Replace `stream_bulk_trades()` with day-by-day fetch

**Repo:** `collector-daemon`
**File:** `crates/collector-backfill/src/archive/download.rs`

**1 site:** `stream_bulk_into()` (lines 18-45)

Currently:
```rust
let stream = client.stream_bulk_trades(request);
tokio::pin!(stream);
while let Some(batch_result) = stream.next().await { ... }
```

Replace with date-by-day iteration using `buffered()` (NOT `buffer_unordered` — MonotonicityChecker and BulkFlushState require date order):

```rust
use futures::{stream, StreamExt};

async fn stream_bulk_into(
    client: &dyn BulkDayTradeFetcher,
    request: BulkTradeRequest,
    state: &mut BulkFlushState,
    concurrency: usize,
) -> anyhow::Result<()> {
    let dates = dates_in_range(request.start, request.end);
    let market = request.market;

    let stream = stream::iter(dates)
        .map(|date| {
            let market = market.clone();
            async move {
                (date, client.fetch_day_trades(&market, date).await)
            }
        })
        .buffered(concurrency);

    tokio::pin!(stream);

    let mut mono = MonotonicityChecker::new();

    while let Some((date, result)) = stream.next().await {
        match result {
            Ok(Some(batch)) => {
                for rest_trade in batch {
                    mono.check(&rest_trade.id);
                    let parquet_trade = rest_trade_to_parquet_trade(rest_trade, 0);
                    state.push(parquet_trade)?;
                }
            }
            Ok(None) => continue, // 404 — skip date
            Err(e) => {
                tracing::warn!(%date, error = %e, "skipping failed archive date");
                continue; // skip-and-continue for production resilience
            }
        }
    }

    Ok(())
}
```

**Add `dates_in_range` helper** (or import from collector-backfill's existing `plan.rs:dates_in_range`):
```rust
fn dates_in_range(start: NaiveDate, end: NaiveDate) -> impl Iterator<Item = NaiveDate> {
    std::iter::successors(Some(start), move |d| {
        let next = *d + chrono::Duration::days(1);
        (next <= end).then_some(next)
    })
}
```

**Update call sites** — `stream_bulk_into` now takes `concurrency: usize` parameter:
- `run_archive_backfill` (line ~138): pass `opts.concurrency`
- `run_archive_phase` (line ~423): pass `opts.concurrency`

**Note on retry:** Bulk `fetch_day_trades` internally still retries via `download_bytes_resumable` (Group B exception from Phase 3). No daemon-side retry needed for bulk paths.

**Note on rate limiting:** Bulk downloads don't use `ExchangeRateLimiter` — they hit archive CDN endpoints (data.binance.vision, etc.), not rate-limited REST APIs. No `wait_for_rate_limit` needed.

---

## Step 7: Replace `stream_trades()` with pagination loop

**Repo:** `collector-daemon`
**4 sites** — each needs per-request rate limiting and retry (both stripped from `fetch_trades()` in the correction).

### Common pagination loop pattern

```rust
use barter_data::retry::{RetryPolicy, retry_with_backoff, is_retriable_data_error};

async fn fetch_trades_paginated(
    client: &dyn TradeFetcher,
    request: TradeRequest,
    // ... site-specific params
) -> Result<...> {
    let mut cursor_start = request.start;
    let mut cursor_id = request.initial_cursor.clone();

    loop {
        // Per-request rate limiting (stripped from fetch_trades in correction)
        client.wait_for_rate_limit().await;

        // Per-request retry (stripped from fetch_trades in correction)
        let batch_request = TradeRequest {
            market: request.market.clone(),
            start: cursor_start,
            end: request.end,
            limit: request.limit,
            initial_cursor: cursor_id.clone(),
        };

        let batch = retry_with_backoff(
            &RetryPolicy::default(),
            is_retriable_data_error,
            || client.fetch_trades(batch_request.clone()),
        ).await?;

        if batch.is_empty() { break; }

        // Advance cursor (exchange-specific via last trade)
        let Some(last) = batch.last() else { break; };
        cursor_start = Some(last.time + chrono::TimeDelta::milliseconds(1));
        cursor_id = Some(last.id.clone());

        // ... site-specific batch processing
    }
}
```

### Site 1: `stream_rest_into()` in `download.rs:53`

**Context:** Sequential REST backfill. Simple — push into BulkFlushState.
**Cursor:** No carry-forward. `initial_cursor: None`.
**Timeout:** None.
**Error:** Hard fail (`?`).

Replace `client.stream_trades(request)` with the pagination loop above. Each batch → convert to ParquetTrade → push into `BulkFlushState`.

### Site 2: `run_rest_backfill_parallel()` spawned tasks in `download.rs:275`

**Context:** Each spawned task streams one 1-hour chunk independently.
**Cursor:** No carry-forward. Each chunk starts fresh.
**Timeout:** None per-page.
**Error:** Error sentinel sent via channel, chunk continues.

Replace `client.stream_trades(request)` inside each spawned task with a pagination loop. The semaphore + mpsc channel architecture remains unchanged.

**Lifetime fix:** The `client` is already `Arc<Box<dyn TradeFetcher>>`. Inside the spawned task, clone the `Arc`:

```rust
let client = Arc::clone(&client);
tokio::spawn(async move {
    // pagination loop using client.fetch_trades(...)
});
```

### Site 3: `stream_backfill_batches()` in `online.rs:97`

**Context:** Chunked catchup with cursor carry-forward. Most complex site.
**Cursor:** Yes — tracks `last_cursor` (oldest trade ID per batch) in `BackfillResult`.
**Timeout:** 60s per page via `tokio::time::timeout`.
**Error:** Partial progress preserved — cursor + trade count returned even on error.

Replace the stream loop with:

```rust
loop {
    let page_result = tokio::time::timeout(
        STREAM_PAGE_TIMEOUT,
        async {
            client.wait_for_rate_limit().await;
            retry_with_backoff(&RetryPolicy::default(), is_retriable_data_error, || {
                client.fetch_trades(request.clone())
            }).await
        }
    ).await;

    match page_result {
        Ok(Ok(batch)) => {
            if batch.is_empty() { break; }
            // Track oldest trade ID for cursor carry-forward
            if let Some(first) = batch.first() {
                last_cursor = Some(first.id.clone());
            }
            // Process batch, update cursor for next page
            // ...
        }
        Ok(Err(e)) => { stream_error = Some(e); break; }
        Err(_timeout) => { stream_error = Some(anyhow!("page timeout")); break; }
    }
}
// Return BackfillResult { trades: total, last_cursor, error: stream_error }
```

**Cursor direction note:** OKX paginates backward (newest→oldest). The `initial_cursor` on `TradeRequest` is passed to OKX as the `after` parameter. The cursor advances by setting `initial_cursor` to the oldest trade ID in each batch. The daemon doesn't need to know the exchange's pagination direction — `fetch_trades()` handles it internally and returns trades in order.

### Site 4: `fetch_and_send()` in `client.rs:21`

**Context:** WS gap recovery. Tracks `last_sent_ts` for `ShrinkGap`.
**Cursor:** Timestamp only (no trade ID).
**Timeout:** Outer 5-minute timeout, no per-page timeout.
**Error:** Flush partial buffer + send `ShrinkGap` before returning error.

Replace `client.stream_trades(request)` with a pagination loop. Preserve the `ShrinkGap` partial-progress mechanism:

```rust
loop {
    client.wait_for_rate_limit().await;
    let batch = retry_with_backoff(&RetryPolicy::default(), is_retriable_data_error, || {
        client.fetch_trades(request.clone())
    }).await;

    match batch {
        Ok(trades) if trades.is_empty() => break,
        Ok(trades) => {
            // Process trades, update cursor_start, track last_sent_ts
            for trade in &trades { /* convert + buffer */ }
            let Some(last) = trades.last() else { break; };
            cursor_start = Some(last.time + chrono::TimeDelta::milliseconds(1));
        }
        Err(e) => {
            // Flush partial buffer
            // Send ShrinkGap with last_sent_ts
            return Err(e.into());
        }
    }
}
```

---

## Step 8: Replace `stream_bulk_trades_multi()` for Hyperliquid

**Repo:** `collector-daemon`
**File:** `crates/collector-backfill/src/archive/hyperliquid.rs`

**1 site:** `run_hyperliquid_multi_backfill()` (line 109)

Currently calls `client.stream_bulk_trades_multi(start_date, end_date)` which yields `HashMap<String, Vec<RestTrade>>` per day.

If Step 2 exposed `fetch_day_trades_multi(date)`, replace with:

```rust
let dates = dates_in_range(start_date, end_date);
let stream = stream::iter(dates)
    .map(|date| async move {
        (date, client.fetch_day_trades_multi(date).await)
    })
    .buffered(opts.concurrency);

tokio::pin!(stream);

while let Some((date, result)) = stream.next().await {
    match result {
        Ok(Some(coin_trades)) => {
            process_multi_coin_batch(coin_trades, &mut flush_states, &coin_ranges)?;
        }
        Ok(None) => continue,
        Err(e) => {
            skipped_errors += 1;
            tracing::warn!(%date, error = %e, "skipping failed Hyperliquid date");
            continue;
        }
    }
}
```

**Note:** `fetch_day_trades_multi` internally uses `try_join_all` for 24 hours (kept in barter-data as fetcher-level). No daemon-side retry/rate-limit needed for S3 bulk downloads.

---

## Step 9: Clean up imports and remove phantom dependency

**Repo:** `collector-daemon`

### 9a. Update all import statements

After Steps 4-8, update imports across all modified files:

| Old import | New import |
|-----------|------------|
| `barter_data::bulk::bulk_client` | `crate::factory::bulk_client` |
| `barter_data::bulk::BulkTradeFetcher` | `barter_data::bulk::BulkDayTradeFetcher` |
| `barter_data::rest::rest_client` | `crate::factory::rest_client` |

### 9b. Remove phantom `barter-integration` dependency (if unused)

Check: `grep -r "barter_integration" crates/ --include="*.rs"`. If zero hits outside comments, remove from `Cargo.toml`.

### 9c. Add `barter_data::retry` imports where needed

The pagination loops in Step 7 need:
```rust
use barter_data::retry::{RetryPolicy, retry_with_backoff, is_retriable_data_error};
```

Or use the daemon's own `RestError::classify()` as the retry predicate for richer error classification.

---

## Step 10: Verify and test

```bash
cd /home/molaco/Documents/collector-daemon

# 1. Compile
cargo check --workspace

# 2. Tests
cargo test --workspace

# 3. Clippy
cargo clippy --workspace

# 4. Verify no old API references remain
grep -rn "stream_bulk_trades\b" crates/ --include="*.rs"  # should be 0
grep -rn "\.stream_trades(" crates/ --include="*.rs"       # should be 0
grep -rn "barter_data::bulk::bulk_client" crates/          # should be 0
grep -rn "barter_data::rest::rest_client" crates/          # should be 0
grep -rn "BulkTradeFetcher\b" crates/ --include="*.rs"     # should be 0 (only BulkDayTradeFetcher)
```

**Manual smoke test:** Run a small backfill for one instrument on one exchange and verify Parquet output matches the pre-migration output.

---

## Execution Order

```
Step 1   Restore initial_cursor in barter-data         (barter-rs-correction, 1 file)
Step 2   Expose fetch_day_trades_multi for Hyperliquid  (barter-rs-correction, 1 file)
Step 3   Point daemon to corrected barter-data          (Cargo.toml, see all errors)
Step 4   Create local factory functions                 (daemon, new factory.rs)
Step 5   Update trait names + BulkConfig                (daemon, mechanical)
Step 6   Replace stream_bulk_trades                     (daemon, 1 site + helper)
Step 7   Replace stream_trades (4 sites)                (daemon, main work)
Step 8   Replace stream_bulk_trades_multi               (daemon, 1 site)
Step 9   Clean up imports + phantom deps                (daemon, mechanical)
Step 10  Verify                                         (tests + grep)
```

Steps 1-2 are barter-data changes (commit first). Step 3 surfaces all errors. Steps 4-5 are mechanical. Steps 6-8 are the real migration. Steps 9-10 are cleanup.

---

## Critical Notes (from multi-round review)

### Rate limiting is stripped from `fetch_trades()`

The correction removed `wait_for_rate_limit()` from inside all exchange `fetch_trades()` implementations. Every pagination loop in Step 7 **must** call `client.wait_for_rate_limit().await` before each `fetch_trades()` call. Missing this will hammer exchange APIs and trigger 429s.

Bulk `fetch_day_trades()` does NOT need external rate limiting — it hits CDN archive endpoints, not rate-limited REST APIs.

### Retry is stripped from `fetch_trades()`

The correction removed `retry_with_backoff()` from inside all exchange `fetch_trades()` implementations. Every pagination loop in Step 7 **must** wrap `fetch_trades()` in `retry_with_backoff()` (or the daemon's own retry).

Bulk `fetch_day_trades()` still retries internally via `download_bytes_resumable` (the Group B exception). No daemon-side retry needed for bulk.

### Use `buffered()` not `buffer_unordered()`

Steps 6 and 8 use `buffered(concurrency)` to preserve date order. `buffer_unordered` would break `MonotonicityChecker` (expects increasing IDs) and `BulkFlushState` (day-boundary detection assumes ordered dates).

### The daemon's existing two-layer rate limiting is correct

- **Coarse layer** (daemon): `RateLimiterRegistry.acquire(exchange_id).await` — gates task start
- **Fine layer** (barter-data, now explicit): `client.wait_for_rate_limit().await` — gates each HTTP request
- **Injected limiter**: The `Arc<ExchangeRateLimiter>` built by `build_rest_limiter_map()` is shared across all tasks for the same exchange

### WebSocket/Candle/subscription APIs are safe

Verified: no breaking changes. Only `#[non_exhaustive]` attributes added to `DataKind`, `SubKind`, `Interval`, `MarketInput`. All struct/enum fields identical. `recorder.rs` and `collector-candle` compile without changes.

### DataError matching is safe

The daemon never imports or matches on `DataError` directly — everything is wrapped in `anyhow::anyhow!()` or `anyhow::Error::from()`. The new variants and `#[non_exhaustive]` attribute are invisible.

---

## Files Modified

### barter-rs-correction (2 files)

| File | Change |
|------|--------|
| `barter-data/src/rest/mod.rs` | Restore `initial_cursor: Option<String>` on `TradeRequest` |
| `barter-data/src/exchange/hyperliquid/bulk/mod.rs` | Expose `fetch_day_trades_multi` as public method |

### collector-daemon (8 files)

| File | Change |
|------|--------|
| `Cargo.toml` | Point barter-data path to corrected repo |
| `crates/collector-backfill/src/factory.rs` | **New** — local `bulk_client()` + `rest_client()` |
| `crates/collector-backfill/src/lib.rs` | Declare `factory` module, update re-exports |
| `crates/collector-backfill/src/archive/download.rs` | Replace `stream_bulk_trades`, `stream_trades`, factory imports, BulkConfig, trait rename |
| `crates/collector-backfill/src/archive/hyperliquid.rs` | Replace `stream_bulk_trades_multi`, BulkConfig |
| `crates/collector-backfill/src/online.rs` | Replace `stream_trades` with pagination loop + rate limit + retry + timeout |
| `crates/collector-backfill/src/client.rs` | Replace `stream_trades` with pagination loop + ShrinkGap |
| `crates/collector/src/main.rs` | Update `rest_client` import path (1 site) |

### Not modified

- `crates/collector-common/` — no barter-data API changes affect it
- `crates/collector-storage/` — uses `Candle`, `RestTrade` types (unchanged)
- `crates/collector-candle/` — uses candle types (unchanged)
- `crates/collector-api/` — uses `Candle`, query types (unchanged)
- `crates/collector-ctl/` — IPC only, no barter-data dependency
- `crates/collector-tui/` — UI only, no barter-data dependency
- `crates/collector-tools/` — uses storage/query types (unchanged)
- `barter-collector` — not used by daemon, no changes needed
