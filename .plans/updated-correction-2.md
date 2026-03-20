# barter-rs Correction Plan (Updated v2)

## Context

All REST, bulk, pagination, retry, and caching code in `barter-data` was added to the fork — the upstream repo (`barter-2`) is a pure WebSocket streaming library with zero REST/bulk logic. There are no external consumers. This means we can restructure aggressively in a single pass with no deprecation or migration concerns.

## Goal

Move collector-level orchestration (pagination, stream composition, date scheduling, caching, retry ownership, concurrency) out of `barter-data` into a new `barter-collector` crate within the same workspace. `barter-data` becomes a pure single-batch fetcher. Later, `barter-collector` moves to `../collector-rs` as a separate repo.

---

## Phase 1: Error Type Overhaul

**Why first:** Every subsequent phase benefits from typed errors. Retry classification, collector error handling, and fetcher diagnostics all depend on structured errors instead of string matching.

### Steps

1.1. Add new variants to `DataError` with `#[non_exhaustive]`:
  - `Http { status: Option<u16>, url: String, message: String }` — HTTP transport errors. `Option` for status (None = no response received).
  - `ExchangeApi { exchange: String, code: String, message: String }` — API-level errors (HTTP 200 with error payload). Separate from `Http` because retry semantics differ.
  - `DataParse(String)` — CSV, JSON, timestamp, numeric conversion failures. Covers both bulk CSV and REST DTO TryFrom errors.
  - `BulkArchive(String)` — ZIP/LZ4/gzip decompression failures.
  - `ChecksumMismatch { expected: String, actual: String }`
  - `UnsupportedInterval { exchange: String, interval: String }`
  - `Io(String)` — local filesystem errors (separate from archive decompression).
  - `PaginationLimit { exchange: String, market: String, limit: usize }`

1.2. Migrate all 125 `DataError::Socket(format!(...))` call sites to the correct variant. Work exchange-by-exchange.

1.3. Rewrite `is_retriable_data_error` to match on variants + HTTP status codes. Delete all substring matching.

1.4. Fix `From<SocketError> for DataError` — match every variant explicitly (no wildcard), preserve HTTP status codes, map deserialization errors to `DataParse`.

**Validation:** Zero `DataError::Socket(format!(...))` in production code. Only variant definition, `From<SocketError>` fallback, and retry match arm remain.

---

## Phase 2: Retry & Observability

**Why before the split:** These fixes apply to `retry.rs` which both `barter-data` and `barter-collector` will share. Fixing it once avoids carrying bugs into the new crate.

### Steps

2.1. Add `tracing::warn!` inside `retry_with_backoff` — log attempt number, backoff duration, error.

2.2. Add jitter: `apply_jitter(duration)` with ±25% range. `rand` as optional dep behind `rest`/`bulk` features with `#[cfg]` no-op fallback.

2.3. Add `RetryPolicy::new()` constructor with `debug_assert!` validation. Do NOT add `#[non_exhaustive]` yet (deferred to Phase 5).

2.4. Use `saturating_mul` for backoff to prevent overflow.

2.5. Comment the HTTP 418 match (Binance-specific IP auto-ban code).

**Validation:** Retry logs appear with structured fields. Overflow test passes.

---

## Phase 3: Fetcher / Collector Split

This is the core architectural change.

### 3A. Create `barter-collector` crate

```
barter-collector/
  Cargo.toml
  src/
    lib.rs
    config.rs       # CollectorConfig
    pagination.rs   # PaginationStrategy trait + PageResult enum
    scheduling.rs   # date_range, partition_date_range, last_day_of_month
    caching.rs      # should_skip, write_verified_marker, compute_sha256, marker_path_for_url
    streams.rs      # stream_paginated, stream_bulk
    filters.rs      # filter_trades_by_time
    retry.rs        # re-exports from barter-data
    download.rs     # placeholder (see note below)
```

**Note on `barter-data` dependency:** Uses `path = "../barter-data"` with explicit `features = ["rest", "bulk"]`, NOT `workspace = true`. This avoids forcing those features on all workspace consumers.

### 3B. Move scheduling utilities

Move `date_range()`, `partition_date_range()`, `last_day_of_month()` from barter-data to `barter-collector/src/scheduling.rs`.

### 3C. Move caching logic

Move `should_skip()`, `write_verified_marker()`, `compute_sha256()`, `marker_path_for_url()` to `barter-collector/src/caching.rs`. Keep `verify_sha256()` and `parse_binance_checksum()` in barter-data (fetcher-level).

Move `BulkConfig.concurrency` and `BulkConfig.cache_dir` to `CollectorConfig`. `BulkConfig` retains only `verify_checksum`.

### 3D. Pagination design

**Deviation from original plan:** Uses a `PaginationStrategy` trait instead of a `Cursor` enum. Rationale: each exchange's pagination logic is 30-80 lines of bespoke async code. An enum would require a god-function `match` in `stream_paginated`. The trait approach follows hexagonal architecture — `PaginationStrategy` is a port, each exchange's pagination is an adapter, `stream_paginated` is the orchestrator.

`PageResult<T>` is an enum (Continue / Done / Empty) with `#[non_exhaustive]`.

Move `filter_trades_by_time` to `barter-collector/src/filters.rs`.

### 3E. Stream composition

Two generic stream composers in `streams.rs`:
- `stream_paginated(strategy, rate_limiter)` — drives a `PaginationStrategy` via `stream::unfold`
- `stream_bulk(dates, concurrency, fetch_day)` — fans out with `buffer_unordered`

### 3F. Slim `barter-data` traits

- `KlineFetcher`: delete `stream_klines`, keep `fetch_klines` + `supported_intervals`
- `TradeFetcher`: delete `stream_trades`, keep `fetch_trades`
- Delete `TradeRequest.initial_cursor`
- Replace `BulkTradeFetcher` with `BulkDayTradeFetcher` (single-day)
- Replace `BulkKlineFetcher` with `BulkDayKlineFetcher` (single-day, with `Send + Sync`)
- Delete `rest_client()` and `bulk_client()` factory functions

### 3G. Strip retry from fetcher internals

Remove `retry_with_backoff` from all REST `fetch_*` methods. Remove `wait_for_rate_limit` calls from inside fetch methods (keep the method definitions for standalone usage). Remove `RetryPolicy` from bulk client structs.

**Exception:** `download_bytes_resumable` stays in barter-data (Binance + Hyperliquid). It is HTTP transport-level retry with partial-buffer state across attempts — not collector-level orchestration. The `retry_with_backoff` calls inside it are an exception to the "no retry in fetchers" rule. The `download.rs` placeholder in barter-collector can be removed or kept as documentation.

### 3H. Retry module

`barter-collector/src/retry.rs` re-exports from `barter-data`. The retry utilities stay in barter-data since `download_bytes_resumable` uses them internally.

**Validation:** Zero `buffer_unordered`, `stream::unfold`, `PaginationState` in barter-data. Zero `stream_bulk_*` methods. `BulkConfig` has only `verify_checksum`. No `RetryPolicy` struct fields in exchange clients.

---

## Phase 4: Safety Fixes

### Steps

4.1. **Blocking I/O in async:** Wrap ZIP decompression in `tokio::task::spawn_blocking` in OKX/Kraken bulk.

4.2. **ZIP size limits:** Check `file.size()` before `read_to_end`. Cap at 2 GB.

4.3. **Hyperliquid panic on unsupported instrument:** Replace `panic!` in `market.rs:39` with error propagation or sentinel.

4.4. **Silent data degradation:**
  - `unwrap_or(0.0)` in `hyperliquid/candle.rs` → `.ok()?`
  - `unwrap_or(1)` in `kraken/mod.rs` → `expect` with invariant comment
  - `tracing::warn!` when `Interval::D3` degrades to `"D"` in Bybit

4.5. **Doc/code mismatch:** Fix Coinbase rate limiter doc (says 60, code is 10).

4.6. **`unwrap` comments:** Add `// N is non-zero` to all `NonZeroU32::new(N).unwrap()`.

4.7. **OKX kline `confirm` field:** Use it to set `is_closed` instead of hardcoding `true`.

4.8. **Eliminate `std::sync::Mutex` in `download_bytes_resumable`:** Replace `Arc<std::sync::Mutex<Vec<u8>>>` with owned `Vec<u8>` in both Binance bulk (`binance/bulk/mod.rs`) and Hyperliquid bulk (`hyperliquid/bulk/mod.rs`). The retry loop is sequential — no shared state needed.

**Validation:** `cargo test --all-features`. `cargo clippy --all-features` clean.

---

## Phase 5: API Cleanup

Final polish after everything is split and safe.

### Steps

5.1. **`#[non_exhaustive]` sweep:** Add to all public structs with fields and public enums across `barter-data`. Priority: `BulkConfig`, all DTOs. Also add to `RetryPolicy`.

5.2. **Visibility tightening:**
  - `*RestClient` / `*BulkClient` fields → `pub(crate)` with accessors
  - `*Channel(pub SmolStr)` / `*Market(pub SmolStr)` inner fields → `pub(crate)`
  - `test_utils` → `#[cfg(test)]`
  - `process_buffered_events` → `pub(crate)`
  - `binance_interval`, `okx_interval`, `kraken_interval` → `pub(crate)`

5.3. **Dead code removal:**
  - Delete `BASE_URL_OKX` (unused)
  - Delete `interval_duration` in `okx/rest/klines.rs` (use `Interval::to_ms`)

5.4. **Unify Bybit + Hyperliquid rate limiters:**
  - Replace private type aliases with shared `ExchangeRateLimiter`
  - Add `with_rate_limiter()` constructors

5.5. **Crate-root re-exports** for most-used types.

5.6. **Add `#[instrument]`** to key public functions in `barter-collector`: `stream_paginated`, `stream_bulk`, caching, scheduling.

5.7. **Doc comments on `wait_for_rate_limit()`** — note it's for standalone/direct usage, not called by collector.

5.8. **Clean up `barter-collector/src/download.rs`** — either remove the placeholder or document that Range-resume lives in barter-data as HTTP transport.

**Validation:** `cargo doc --all-features` — no broken links. Full test suite.

---

## Phase Order

```
Phase 1 (errors)
  └→ Phase 2 (retry)
       └→ Phase 3 (fetcher/collector split)  ← core change
            └→ Phase 5 (API cleanup)
       └→ Phase 4 (safety fixes)  ← parallel to Phase 3
```

Phases 1-2 are prerequisites. Phase 3 is the main event. Phases 4-5 are cleanup that can overlap.

---

## What Does NOT Change

- WebSocket streaming (`streams/`, `Connector`, `StreamSelector`) — untouched
- Exchange instrument/market/subscription types — untouched
- `barter-integration` crate — untouched (except `From<SocketError>` in Phase 1)
- All exchange server variant types (`BinanceServerSpot`, etc.) — untouched
- Pure fetcher utilities: URL construction, DTOs, `TryFrom` conversions, CSV parsers, ZIP extraction, `verify_sha256`, `parse_binance_checksum`, interval mappers
- `download_bytes_resumable` — stays in barter-data as HTTP transport optimization

## What Moves to `barter-collector`

Scheduling (`date_range`, `partition_date_range`), caching (`should_skip`, `write_verified_marker`, `marker_path_for_url`), pagination (`PaginationStrategy` trait, `PageResult`), stream composition (`stream_paginated`, `stream_bulk`), filters (`filter_trades_by_time`), config (`CollectorConfig` with concurrency, cache_dir, retry, max_trade_pages).

## Future: Move to `collector-rs`

Once `barter-collector` works within the workspace, moving it to `../collector-rs` is:
1. `git mv barter-collector/ ../collector-rs/`
2. Replace `path = "../barter-collector"` deps with git/crates.io references
3. Separate CI

This is a mechanical step with no logic changes.
