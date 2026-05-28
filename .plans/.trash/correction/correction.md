# barter-rs Correction Plan

## Diagnosis

barter-rs started as a pure data fetcher but accumulated collector-level orchestration: pagination state machines, date-range iteration, concurrency management, caching, and retry loops are embedded inside what should be thin HTTP+parse clients. Additionally, the codebase has 61 identified issues (11 high, 22 medium, 28 low) around error types, safety, observability, and API design.

This plan corrects the architecture in 8 sequential phases. Each phase is independently shippable and leaves the codebase in a working state.

---

## Phase 1: Error Type Overhaul

**Goal:** Replace the monolithic `DataError::Socket(String)` catch-all with structured, per-domain error variants.

**Why first:** Every subsequent phase benefits from typed errors. Retry classification, collector error handling, and user-facing diagnostics all depend on this.

### Steps

1.1. Add new variants to `DataError`:
  - `HttpApi { status: u16, exchange: String, message: String }` — API-level errors with status code
  - `BulkArchive(String)` — ZIP extraction, decompression failures
  - `CsvParse(String)` — CSV/data format errors
  - `ChecksumMismatch { expected: String, actual: String }` — checksum verification
  - `UnsupportedInterval { exchange: String, interval: String }` — replaces `Socket("... does not support ...")`
  - `Pagination(String)` — max-pages exceeded, cursor stall

1.2. Add `#[non_exhaustive]` to `DataError` (enables future additions without semver breaks).

1.3. Migrate all `DataError::Socket(format!(...))` call sites to the appropriate new variant. Work exchange-by-exchange: Binance → OKX → Bybit → Kraken → Coinbase → Hyperliquid.

1.4. Rewrite `is_retriable_data_error` to match on structured variants + HTTP status codes instead of substring matching. Delete all string-matching logic.

1.5. Update `From<SocketError> for DataError` to preserve structure (carry the `SocketError` variant, not `.to_string()`).

**Validation:** `cargo test --all-features` passes. Grep for `DataError::Socket` returns zero hits outside of actual socket/WebSocket paths.

---

## Phase 2: Retry & Observability Improvements

**Goal:** Make retry observable, safe, and ready to be lifted to the collector layer.

### Steps

2.1. Add `tracing::warn!` inside `retry_with_backoff` on each retry attempt — log attempt number, backoff duration, and error summary.

2.2. Add jitter to the backoff calculation: `backoff * rand(0.75..1.25)` (add `rand` as an optional dep behind the existing `rest`/`bulk` features).

2.3. Add validation to `RetryPolicy` constructor: `initial_backoff <= max_backoff`, `multiplier >= 1`. Use `debug_assert!` or return `Result` from a `new()` constructor.

2.4. Use `saturating_mul` for the backoff multiplier to prevent overflow.

2.5. Add a comment to the HTTP 418 match arm explaining it's a Binance-specific rate-limit code.

**Validation:** Run bulk/REST integration tests. Verify retry logs appear in trace output.

---

## Phase 3: Slim the Fetcher Traits

**Goal:** Remove collector-level methods from fetcher traits. Traits become single-batch-only.

### Steps

3.1. **REST traits** — In `rest/mod.rs`:
  - Remove `stream_klines` from `KlineFetcher`. Keep only `fetch_klines` + `supported_intervals`.
  - Remove `stream_trades` from `TradeFetcher`. Keep only `fetch_trades`.
  - Remove `initial_cursor` from `TradeRequest`.

3.2. **Bulk traits** — In `bulk/mod.rs`:
  - Replace `BulkTradeFetcher::stream_bulk_trades` with `fetch_day_trades(&self, market: &str, date: NaiveDate) -> Result<Option<Vec<RestTrade>>>`.
  - Replace `BulkKlineFetcher::stream_bulk_klines` with `fetch_day_klines(&self, market: &str, interval: Interval, date: NaiveDate) -> Result<Option<Vec<Candle>>>`.
  - Add `Send + Sync` bounds to `BulkKlineFetcher` (fixing the existing inconsistency).

3.3. **Move `date_range()` out of `bulk/mod.rs`** — it will move to the collector module in Phase 5.

3.4. **Update each exchange implementation** to implement the new single-batch trait methods:
  - Binance: rename `download_and_parse_trades` → impl `fetch_day_trades`
  - OKX: same, remove retry wrapper (retry moves out in Phase 6)
  - Bybit: same
  - Kraken: `parse_zip_trades` becomes the `fetch_day_trades` impl
  - Hyperliquid: single-hour download becomes the primitive; day-level composition moves out

3.5. **Do not delete** the old `stream_*` implementations yet — mark them `#[deprecated]` with a message pointing to the collector. This allows downstream migration.

**Validation:** `cargo test --all-features`. Deprecated methods still compile. New trait impls work for single-batch calls.

---

## Phase 4: Extract Pagination State to a Shared Module

**Goal:** Consolidate the 11 nearly-identical `PaginationState` structs into a generic, reusable type that the collector will own.

### Steps

4.1. Create `barter-data/src/collector/pagination.rs` with a generic cursor-based pagination state:
  - Time-cursor variant (for klines: advance by close_time + 1)
  - ID-cursor variant (for OKX/Binance trades: advance by last ID)
  - Nonce-cursor variant (for Kraken trades)
  - Backward-cursor variant (for Coinbase trades: collect-then-reverse)

4.2. Move `filter_trades_by_time` to `collector/filters.rs` as a reusable post-fetch filter.

4.3. Move `MAX_TRADE_PAGES` to collector-level config.

4.4. Delete all per-exchange `PaginationState` / `TradePaginationState` structs.

**Validation:** Compiles. The pagination module is not yet wired — that happens in Phase 5.

---

## Phase 5: Build the Collector Layer

**Goal:** Create a `collector` module that owns all orchestration: pagination streams, date iteration, concurrency, and caching.

### Steps

5.1. Create `barter-data/src/collector/mod.rs` with sub-modules:
  - `pagination` (from Phase 4)
  - `filters` (from Phase 4)
  - `streams` — generic `stream_klines` / `stream_trades` / `stream_bulk` functions
  - `scheduling` — `date_range()`, `partition_date_range()`, `last_day_of_month()`
  - `caching` — marker-file logic (`should_skip`, `write_verified_marker`, `marker_path_for_url`)

5.2. Implement generic `stream_klines<F: KlineFetcher>(fetcher, request) -> impl Stream`:
  - Uses `stream::unfold` with the generic pagination state from Phase 4
  - Calls `fetcher.fetch_klines(...)` per page
  - Handles cursor advancement, stall detection, end-of-range termination
  - Exchange-specific cursor behavior parameterized via a `CursorStrategy` trait or enum

5.3. Implement generic `stream_trades<F: TradeFetcher>(fetcher, request) -> impl Stream`:
  - Same pattern, supporting forward (Kraken), backward (OKX/Coinbase), and ID-based (Binance) strategies

5.4. Implement generic `stream_bulk<F: BulkDayFetcher>(fetcher, dates, concurrency) -> impl Stream`:
  - `stream::iter(dates).map(|d| fetcher.fetch_day_trades(market, d)).buffer_unordered(concurrency).filter_map(skip_none)`
  - Replaces all 6 exchange-specific `stream_bulk_trades` impls with one

5.5. Move `date_range()`, `partition_date_range()`, `last_day_of_month()` into `collector::scheduling`.

5.6. Move `BulkConfig.concurrency` and `BulkConfig.cache_dir` into a new `CollectorConfig` struct. `BulkConfig` retains only `verify_checksum` (if checksum stays in the fetcher as a pure function).

5.7. Feature-gate the collector module: `#[cfg(feature = "collector")]` with `collector` included in `default`.

**Validation:** Write integration tests that compose `collector::stream_klines` with each exchange's `KlineFetcher`. Verify identical output to the old `stream_klines` impls.

---

## Phase 6: Lift Retry Out of Fetchers

**Goal:** Fetcher methods make one attempt. Retry is applied by the collector.

### Steps

6.1. Create `collector::retry` module that re-exports `RetryPolicy` and `retry_with_backoff` from `retry.rs`.

6.2. Wrap retry around fetcher calls at the collector level:
  - `collector::stream_klines` calls `retry_with_backoff(|| fetcher.fetch_klines(...))` per page
  - `collector::stream_bulk` calls `retry_with_backoff(|| fetcher.fetch_day_trades(...))` per date

6.3. Strip `retry_with_backoff` from inside each exchange's `fetch_*` / `download_and_parse_*` methods. Each method becomes a single-attempt call.

6.4. Move rate-limit acquisition into the retry loop (fixes the "rate limiter checked once before retry" bug across all 5 REST clients).

6.5. Remove `RetryPolicy` field from all `*BulkClient` and `*RestClient` structs. Retry policy lives in `CollectorConfig`.

**Validation:** Integration tests still pass. Verify retry + rate-limit logs show one permit acquisition per attempt.

---

## Phase 7: Safety & Hygiene Fixes

**Goal:** Address remaining high/medium issues that don't require architectural changes.

### Steps

7.1. **Blocking I/O in async:**
  - Wrap `checksum.rs` file operations in `tokio::task::spawn_blocking`
  - Wrap ZIP decompression in OKX/Kraken bulk modules in `spawn_blocking`

7.2. **ZIP size limits:**
  - Add `const MAX_ZIP_ENTRY_SIZE: u64 = 2_147_483_648` (2 GB)
  - Check `file.size()` before `read_to_end` in all `extract_zip_csv` / ZIP-reading paths

7.3. **Binance async Mutex:**
  - Replace `std::sync::Mutex` with `tokio::sync::Mutex` in `download_bytes_resumable`
  - Or eliminate the Mutex entirely (single-task access pattern)

7.4. **Hyperliquid panic on unsupported instrument:**
  - Replace `panic!` in `hyperliquid/market.rs:39` with `return` a sentinel or propagate error

7.5. **Silent data degradation:**
  - Replace `unwrap_or(0.0)` in `hyperliquid/candle.rs` with `.ok()?` (match the trade pattern)
  - Replace `unwrap_or(1)` in `kraken/mod.rs` with `expect` + invariant comment
  - Add `tracing::warn!` when `Interval::D3` degrades to `"D"` in Bybit

7.6. **Doc/code mismatch:**
  - Fix Coinbase rate limiter doc (says 60, code is 10)

7.7. **`unwrap` comments:**
  - Add `// N is non-zero` comments to all `NonZeroU32::new(N).unwrap()` sites

7.8. **OKX kline `confirm` field:**
  - Use `confirm` value to set `is_closed` instead of hardcoding `true`

**Validation:** `cargo test --all-features`. `cargo clippy --all-features` clean.

---

## Phase 8: API Surface Cleanup

**Goal:** Tighten the public API for a library crate.

### Steps

8.1. **`#[non_exhaustive]` sweep:**
  - Add to all public structs with fields: `BulkConfig`, all `*ApiError`, all `*RestClient`, all `*BulkClient`, all request/response DTOs
  - Add to all public enums: `DataError` (done in Phase 1), `DataKind`, `MarketInput`

8.2. **Visibility tightening:**
  - Make `*RestClient` and `*BulkClient` fields `pub(crate)` (add accessors if needed externally)
  - Make `*Channel(pub SmolStr)` and `*Market(pub SmolStr)` inner fields `pub(crate)`
  - Gate `test_utils` behind `#[cfg(test)]` or `#[cfg(feature = "test-utils")]`
  - Make `process_buffered_events` `pub(crate)`
  - Scope `binance_interval`, `okx_interval`, `kraken_interval` to `pub(crate)`

8.3. **Dead code removal:**
  - Delete unused `BASE_URL_OKX` constant
  - Delete `interval_duration` in `okx/rest/klines.rs` (use `Interval::to_ms` instead)

8.4. **Unify Bybit + Hyperliquid rate limiters:**
  - Replace `BybitRateLimiter` / `HyperliquidRateLimiter` type aliases with the shared `ExchangeRateLimiter`
  - Add `with_rate_limiter(Arc<ExchangeRateLimiter>)` constructors
  - Register both in the `rest_client()` factory

8.5. **Remove deprecated `stream_*` methods** (deprecated in Phase 3, now replaced by collector).

8.6. **Add crate-root re-exports** for the most-used types: `DataError`, `MarketEvent`, key traits.

**Validation:** `cargo doc --all-features` — no broken links. `cargo semver-checks` if available. Full test suite passes.

---

## Phase Order & Dependencies

```
Phase 1 (errors)
  └─→ Phase 2 (retry/observability)
        └─→ Phase 3 (slim traits)
              ├─→ Phase 4 (extract pagination)
              │     └─→ Phase 5 (build collector)
              │           └─→ Phase 6 (lift retry)
              └─→ Phase 7 (safety fixes)  ← can run parallel to 4-6
                    └─→ Phase 8 (API cleanup)  ← final, after all others
```

Phases 4-6 are the core architectural change. Phase 7 is independent safety work that can proceed in parallel. Phase 8 is the final polish pass.

---

## What Does NOT Change

- WebSocket streaming (`streams/`, `Connector` trait, `StreamSelector`) — untouched
- Exchange instrument/market/subscription types — untouched
- `barter-integration` crate — untouched (except `From<SocketError>` in Phase 1)
- The `Connector` trait and its `SocketError` return type — untouched
- Feature flag structure (`rest`, `bulk`) — additive `collector` flag added
- All exchange server variant types (`BinanceServerSpot`, etc.) — untouched
