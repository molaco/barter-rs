# Collector-Daemon Migration Plan (Option B)

## Context

collector-daemon (`../collector-daemon`) currently depends on `../barter-rs/barter-data` (original, pre-correction). After the 5-phase barter-rs correction, the barter-data API changed significantly. The daemon needs to be updated to work with the corrected barter-data.

**Key insight:** collector-daemon already has richer orchestration than barter-collector (circuit breakers, priority scheduling, chunked cursor carriage, Parquet storage). It does NOT need barter-collector as a dependency. It calls barter-data's single-batch APIs directly and manages its own loops, retry, and concurrency.

**Scope:** 32 broken call sites across 5 files, primarily in `crates/collector-backfill/`.

---

## Step 1: Point dependency to corrected barter-data

Update `collector-daemon/Cargo.toml` workspace dependency:

```toml
# Change from:
barter-data = { path = "../barter-rs/barter-data", features = ["rest", "bulk"] }
# To:
barter-data = { path = "../barter-rs-correction/barter-data", features = ["rest", "bulk"] }
```

After this step, `cargo check` will show all compilation errors — the exact inventory of what needs to change.

---

## Step 2: Restore `initial_cursor` on `TradeRequest` in barter-data

**Why:** 1 of 5 `TradeRequest` construction sites in the daemon uses `initial_cursor` (`online.rs:82`). This is for OKX chunked backward pagination with cursor carry-forward between 1-hour chunks. It's a field on the request struct, not orchestration logic — it tells the exchange "start from this trade ID."

**In `barter-rs-correction/barter-data/src/rest/mod.rs`**, add back:

```rust
pub struct TradeRequest {
    pub market: String,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
    pub initial_cursor: Option<String>,  // restore
}
```

Update the 4 daemon sites that hardcode `initial_cursor: None` — no behavioral change, just field presence.

**Alternative:** If you don't want to restore it in barter-data, the daemon can manage cursors entirely on its side by calling `fetch_trades` in a loop and advancing the cursor itself. But since the daemon's `backfill_gap()` in `online.rs` already relies on cursor carry-forward between chunks, restoring the field is simpler.

---

## Step 3: Recreate factory functions in collector-daemon

**Why:** `bulk_client()` and `rest_client()` were deleted from barter-data (they were orchestration). The daemon uses them in 9 call sites. Rather than restoring them in barter-data, recreate them locally in the daemon where they belong.

**In `collector-daemon/crates/collector-backfill/src/`**, create a `factory.rs` module (or inline in `lib.rs`):

- `pub fn bulk_client(exchange_id, config) -> Option<Box<dyn BulkDayTradeFetcher>>` — maps exchange IDs to concrete bulk clients. Same logic as the deleted `barter_data::bulk::bulk_client()`.
- `pub fn rest_client(exchange_id, rate_limiter) -> Option<Box<dyn TradeFetcher>>` — maps exchange IDs to concrete REST clients. Same logic as the deleted `barter_data::rest::rest_client()`.

Update all 9 `rest_client` and 3 `bulk_client` call sites to use the local factory. Update the re-export in `lib.rs:12`.

---

## Step 4: Update trait names and BulkConfig

**Rename references:**
- `BulkTradeFetcher` → `BulkDayTradeFetcher` (2 sites in `download.rs`)
- `BulkConfig { concurrency, verify_checksum, cache_dir }` → `BulkConfig { verify_checksum: true }` (3 sites)

**Handle removed `concurrency` field:**
The daemon passes `opts.concurrency` into `BulkConfig`. Since `BulkConfig` no longer has this field, the daemon must manage concurrency itself. Two sub-options:

**4a.** If the daemon uses `stream_bulk_trades()` (which is deleted), concurrency was internal to the stream. After migration (Step 5), the daemon's own date loop + `buffer_unordered` handles concurrency using `opts.concurrency` directly.

**4b.** If the daemon calls `fetch_day_trades()` per date, it wraps the calls in `futures::stream::iter(dates).map(...).buffer_unordered(opts.concurrency)` — same pattern barter-collector uses.

**Handle removed `cache_dir` field:** All 3 sites already set `cache_dir: None`. Just remove the field from construction. Zero behavioral impact.

---

## Step 5: Replace `stream_bulk_trades()` with day-by-day loop

**1 site:** `stream_bulk_into()` in `download.rs:18-45`

Currently:
```rust
let stream = client.stream_bulk_trades(request);
tokio::pin!(stream);
while let Some(batch_result) = stream.next().await { ... }
```

Replace with a date iteration loop using `fetch_day_trades`:

```rust
for date in dates_in_range(request.start, request.end) {
    match client.fetch_day_trades(&request.market, date).await {
        Ok(Some(batch)) => { /* same push-into-BulkFlushState logic */ }
        Ok(None) => continue,  // 404, skip
        Err(e) => return Err(anyhow!("archive fetch error: {e}")),
    }
}
```

For concurrency, wrap in `buffer_unordered`:

```rust
let stream = futures::stream::iter(dates_in_range(request.start, request.end))
    .map(|date| {
        let client = &client;
        let market = &request.market;
        async move { client.fetch_day_trades(market, date).await.map(|opt| (date, opt)) }
    })
    .buffer_unordered(concurrency);
```

The `MonotonicityChecker`, `BulkFlushState.push()`, and `finalize_bulk_state()` remain unchanged.

---

## Step 6: Replace `stream_trades()` with fetch loop

**5 sites** across `download.rs`, `online.rs`, `client.rs`.

Currently each site does:
```rust
let stream = client.stream_trades(request);
tokio::pin!(stream);
while let Some(batch) = stream.next().await { ... }
```

Replace with a pagination loop calling `fetch_trades()`:

```rust
let mut cursor_start = request.start;
loop {
    let batch_request = TradeRequest {
        market: request.market.clone(),
        start: cursor_start,
        end: request.end,
        limit: request.limit,
        initial_cursor: cursor.clone(),
    };
    let batch = client.fetch_trades(batch_request).await?;
    if batch.is_empty() { break; }

    // advance cursor from last trade
    cursor_start = Some(batch.last().unwrap().time + TimeDelta::milliseconds(1));

    // process batch (same as current while-let body)
    ...
}
```

**Per-site specifics:**

| Site | File | Cursor? | Timeout? | Special handling |
|------|------|---------|----------|-----------------|
| `stream_rest_into` | download.rs:53 | No | No | Simple: just loop + flush |
| `run_rest_backfill_parallel` | download.rs:275 | No | No | Each spawned task gets its own loop; semaphore + channel unchanged |
| `stream_backfill_batches` | online.rs:97 | Yes (`last_cursor`) | Yes (60s) | Most complex: wrap each `fetch_trades` in `tokio::time::timeout`. Track `last_cursor` from `batch.first().id`. Return `BackfillResult`. |
| `fetch_and_send` | client.rs:21 | Timestamp only | No | Track `last_sent_ts`. On error: flush partial + `ShrinkGap`. |
| (1-hour chunks) | download.rs:275 | No | No | Each chunk is independent, self-contained loop |

**Rate limiting:** The daemon's injected `ExchangeRateLimiter` (via `with_rate_limiter()`) is already used by barter-data's `fetch_trades()` internally via `wait_for_rate_limit()`. No change needed — barter-data still rate-limits per HTTP request inside `fetch_trades()`.

---

## Step 7: Replace `stream_bulk_trades_multi()` for Hyperliquid

**1 site:** `hyperliquid.rs:109`

`stream_bulk_trades_multi()` was a Hyperliquid-specific method that returned `HashMap<String, Vec<RestTrade>>` per hourly S3 file. It was deleted in Phase 3.

The Hyperliquid bulk client still has `download_and_parse_trades()` (single-market, single-day) and the internal `download_and_parse_trades_multi()` (all-coins, single-day). The daemon needs the multi-coin variant.

**Options:**
- **A:** Expose `download_and_parse_trades_multi` as a public method on `HyperliquidBulkClient` (it's currently private/dead code since `stream_bulk_trades_multi` was deleted).
- **B:** The daemon iterates dates itself and calls a per-date multi-coin method.

Option A is simpler. Add a `fetch_day_trades_multi()` method to `HyperliquidBulkClient` that exposes the existing internal logic.

---

## Step 8: Verify and test

1. `cargo check --workspace` in collector-daemon — zero errors
2. `cargo test --workspace` — all tests pass
3. Manual smoke test: run a small backfill for one instrument on one exchange and verify Parquet output is identical

---

## Execution Order

```
Step 1  Point to corrected barter-data        (Cargo.toml, see errors)
Step 2  Restore initial_cursor                 (barter-data, 1 field)
Step 3  Recreate factory functions locally      (daemon, new factory.rs)
Step 4  Update trait names + BulkConfig         (daemon, mechanical renames)
Step 5  Replace stream_bulk_trades             (daemon, 1 site)
Step 6  Replace stream_trades                  (daemon, 5 sites)
Step 7  Replace stream_bulk_trades_multi       (barter-data + daemon, 1 site)
Step 8  Verify                                 (tests)
```

Steps 2-4 are quick mechanical fixes. Steps 5-6 are the main work. Step 7 is Hyperliquid-specific.

---

## Files Modified

### barter-rs-correction (2 files)
- `barter-data/src/rest/mod.rs` — restore `initial_cursor` on `TradeRequest`
- `barter-data/src/exchange/hyperliquid/bulk/mod.rs` — expose `fetch_day_trades_multi`

### collector-daemon (5 files)
- `Cargo.toml` — update barter-data path
- `crates/collector-backfill/src/archive/download.rs` — main migration (bulk + REST)
- `crates/collector-backfill/src/archive/hyperliquid.rs` — multi-coin migration
- `crates/collector-backfill/src/online.rs` — cursor-aware REST migration
- `crates/collector-backfill/src/client.rs` — gap recovery migration
- `crates/collector-backfill/src/lib.rs` — factory re-export + `build_rest_clients` update
- `crates/collector/src/main.rs` — update `rest_client` check (1 site)

### Not modified
- barter-collector — not used by daemon, can be deleted or kept as an optional library
