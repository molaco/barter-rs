# Phase 5: API Cleanup — Detailed Implementation Plan

**Prerequisite:** Phases 1-3 complete. Phase 4 (safety fixes) may run in parallel.

**Guiding principles (from Rust Architecture Guide):**
- Default private, escalate deliberately (`pub(crate)` before `pub`)
- `#[non_exhaustive]` on all public enums and structs with fields in library code
- Public API uses `&str`/`&[T]`/`&Path`, not owned types in params
- Re-export key public types from crate root (facade pattern)
- `#[instrument]` on public async entry points, not `println!`

---

## Step 5.1: `#[non_exhaustive]` Sweep

**Why:** Adding variants/fields to public types is a semver-breaking change without `#[non_exhaustive]`. Currently only `DataError` has it. Every other public struct and enum is unprotected.

### 5.1.1 Public enums (18 missing)

Add `#[non_exhaustive]` to each. Existing exhaustive `match` arms in barter-data must add wildcard fallbacks.

| Enum | File | Line |
|------|------|------|
| `Interval` | `subscription/candle.rs` | 51 |
| `SubKind` | `subscription/mod.rs` | 81 |
| `OrderBookEvent` | `subscription/book.rs` | 123 |
| `DataKind` | `event.rs` | 123 |
| `Event<Origin, T>` | `streams/reconnect/mod.rs` | 7 |
| `Command<Ch, Mkt, IK>` | `streams/handle.rs` | 22 |
| `MarketInput<'a>` | `instrument.rs` | 17 |
| `BitfinexCandlePayload` | `exchange/bitfinex/candle.rs` | 86 |
| `BitfinexPayload` | `exchange/bitfinex/message.rs` | 44 |
| `BitfinexPlatformEvent` | `exchange/bitfinex/subscription.rs` | 39 |
| `Status` (Bitfinex) | `exchange/bitfinex/subscription.rs` | 112 |
| `BybitPayloadKind` | `exchange/bybit/message.rs` | 51 |
| `BybitReturnMessage` | `exchange/bybit/subscription.rs` | 34 |
| `OkxSubResponse` | `exchange/okx/subscription.rs` | 43 |
| `CoinbaseSubResponse` | `exchange/coinbase/subscription.rs` | 26 |
| `KrakenMessage<T>` | `exchange/kraken/message.rs` | 72 |
| `KrakenEvent` | `exchange/kraken/message.rs` | 99 |
| `KrakenSubResponse` | `exchange/kraken/subscription.rs` | 35 |

**`Interval` cascading match impact (highest-risk item):**

`Interval` has 14 variants and is exhaustively matched in 10 production sites. After adding `#[non_exhaustive]`, each needs a wildcard arm. 5 other match sites already have catch-all arms and need no change.

| Match site | File | Line | Already has wildcard? |
|------------|------|------|-----------------------|
| `Interval::to_ms()` | `subscription/candle.rs` | 78 | NO — add `_ => unreachable!("all Interval variants handled")` |
| `Interval::as_str()` | `subscription/candle.rs` | 103 | NO — same |
| `Interval::description()` | `subscription/candle.rs` | 123 | NO — same |
| `binance_interval()` | `exchange/binance/mod.rs` | 70 | NO — add `_ => unreachable!(...)` |
| `bybit_interval()` | `exchange/bybit/mod.rs` | 81 | NO — add `_ => unreachable!(...)` |
| `okx_interval()` | `exchange/okx/mod.rs` | 67 | NO — add `_ => unreachable!(...)` |
| `kraken_interval()` | `exchange/kraken/mod.rs` | 67 | NO — add `_ => unreachable!(...)` |
| `hyperliquid_interval()` | `exchange/hyperliquid/mod.rs` | 59 | NO — add `_ => unreachable!(...)` |
| `interval_duration()` | `exchange/okx/rest/klines.rs` | 14 | NO — deleted in step 5.3.3 |
| `interval_duration()` | `exchange/coinbase/rest/klines.rs` | 14 | NO — deleted in step 5.3.3 |
| `Interval::from_str()` | `subscription/candle.rs` | 189 | YES — has `_` catch-all |
| `bitfinex_interval()` | `exchange/bitfinex/mod.rs` | 79 | YES — has `unsupported =>` |
| `bitmex_interval()` | `exchange/bitmex/mod.rs` | 56 | YES — has `unsupported =>` |
| `gateio_interval()` | `exchange/gateio/mod.rs` | 64 | YES — has `unsupported =>` |
| `coinbase_interval()` | `exchange/coinbase/mod.rs` | 55 | YES — has `unsupported =>` |

**Note:** The `Interval` methods (`to_ms`, `as_str`, `description`) are on the enum's own `impl` block in the same crate, so `#[non_exhaustive]` does NOT require wildcard arms there (the attribute only affects external crates). However, the `*_interval()` free functions in exchange modules DO need wildcards because they match on the enum parameter, not `self`. **Actually: `#[non_exhaustive]` only affects matches in downstream crates, not within the defining crate.** So none of these 10 sites need changes. The wildcard arms are only needed by external consumers.

**Correction:** Within the defining crate, `#[non_exhaustive]` has no effect on match exhaustiveness. All 10 sites compile as-is. Only downstream crates matching on `Interval` would need wildcards. This makes the step purely additive — no cascading match changes needed inside barter-data.

**Implementation order:** Core types first (`Interval`, `SubKind`, `DataKind`, `OrderBookEvent`), then exchange-specific types alphabetically. After adding `#[non_exhaustive]`, run `cargo build --all-features` to verify no regressions.

### 5.1.2 Public structs with fields (33+ missing)

Add `#[non_exhaustive]` to all public structs that have `pub` fields. This is a larger set. Group by category:

**Core config structs:**

| Struct | File | Line | Fields |
|--------|------|------|--------|
| `RetryPolicy` | `retry.rs` | 8 | `initial_backoff`, `max_backoff`, `multiplier`, `max_retries` |
| `BulkConfig` | `bulk/mod.rs` | 38 | `verify_checksum` |

**REST client structs (6):**

| Struct | File | Line |
|--------|------|------|
| `BinanceRestClient<S>` | `exchange/binance/rest/mod.rs` | 62 |
| `OkxRestClient` | `exchange/okx/rest/mod.rs` | 69 |
| `KrakenRestClient` | `exchange/kraken/rest/mod.rs` | 62 |
| `CoinbaseRestClient` | `exchange/coinbase/rest/mod.rs` | 62 |
| `BybitRestClient<S>` | `exchange/bybit/rest/mod.rs` | 83 |
| `HyperliquidRestClient` | `exchange/hyperliquid/rest/mod.rs` | 77 |

**Bulk client structs (4):**

| Struct | File | Line |
|--------|------|------|
| `BinanceBulkClient<S>` | `exchange/binance/bulk/mod.rs` | 77 |
| `OkxBulkClient` | `exchange/okx/bulk/mod.rs` | 19 |
| `BybitBulkClient<S>` | `exchange/bybit/bulk/mod.rs` | 61 |
| `HyperliquidBulkClient` | `exchange/hyperliquid/bulk/mod.rs` | 27 |

**API error structs (6):**

| Struct | File | Line |
|--------|------|------|
| `BinanceApiError` | `exchange/binance/rest/mod.rs` | 31 |
| `OkxApiError` | `exchange/okx/rest/mod.rs` | 33 |
| `KrakenApiError` | `exchange/kraken/rest/mod.rs` | 34 |
| `CoinbaseApiError` | `exchange/coinbase/rest/mod.rs` | 30 |
| `BybitApiError` | `exchange/bybit/rest/mod.rs` | 31 |
| `HyperliquidApiError` | `exchange/hyperliquid/rest/mod.rs` | 27 |

**REST/Bulk DTO structs (12+):**

| Struct | File |
|--------|------|
| `BinanceKlineRaw` | `exchange/binance/rest/klines.rs` |
| `OkxKlinesResponse` | `exchange/okx/rest/klines.rs` |
| `OkxKlineRaw` | `exchange/okx/rest/klines.rs` |
| `KrakenOhlcResponse` | `exchange/kraken/rest/klines.rs` |
| `CoinbaseKlinesResponse` | `exchange/coinbase/rest/klines.rs` |
| `BybitKlinesResponse` | `exchange/bybit/rest/klines.rs` |
| `BinanceBulkAggTrade` | `exchange/binance/bulk/trades.rs` |
| `BinanceBulkKline` | `exchange/binance/bulk/klines.rs` |
| `OkxBulkTrade` | `exchange/okx/bulk/trades.rs` |
| `BybitBulkTrade` | `exchange/bybit/bulk/trades.rs` |
| `BybitSpotBulkTrade` | `exchange/bybit/bulk/trades.rs` |
| `KrakenBulkTrade` | `exchange/kraken/bulk/trades.rs` |
| `HyperliquidFillEvent` | `exchange/hyperliquid/bulk/trades.rs` |

**Other structs:**

| Struct | File |
|--------|------|
| `AwsCredentials` | `exchange/hyperliquid/bulk/s3_signer.rs` |
| `SignedHeaders` | `exchange/hyperliquid/bulk/s3_signer.rs` |

**Implementation note:** Adding `#[non_exhaustive]` to structs with `pub` fields prevents external construction via struct literal syntax. Consumers must use constructors or `Default`. For each struct, verify a constructor (`new()`, `Default`, or builder) already exists. If not, add one alongside the `#[non_exhaustive]` attribute.

**Structs that need constructors added:**
- `RetryPolicy` — already has `RetryPolicy::new()` from Phase 2
- `BulkConfig` — add `BulkConfig::new(verify_checksum: bool)` + `Default`
- API error structs — typically constructed only inside HTTP parsers (internal), so `pub(crate)` construction is fine
- DTO structs — constructed only by deserialization (`Deserialize`), no manual constructor needed
- Client structs — already have `new()` / `with_rate_limiter()` constructors

---

## Step 5.2: Visibility Tightening

### 5.2.1 RestClient / BulkClient fields → `pub(crate)` with accessors

**Pattern for REST clients (6 structs):**

```rust
// Before:
pub struct OkxRestClient {
    pub client: Arc<RestClient<'static, PublicNoHeaders, OkxHttpParser>>,
    pub rate_limiter: Arc<ExchangeRateLimiter>,
}

// After:
#[non_exhaustive]
pub struct OkxRestClient {
    pub(crate) client: Arc<RestClient<'static, PublicNoHeaders, OkxHttpParser>>,
    pub(crate) rate_limiter: Arc<ExchangeRateLimiter>,
}

impl OkxRestClient {
    pub fn http_client(&self) -> &RestClient<'static, PublicNoHeaders, OkxHttpParser> {
        &self.client
    }
}
```

Apply to all 6 REST clients:
1. `BinanceRestClient<S>` — `client`, `rate_limiter` → `pub(crate)`
2. `OkxRestClient` — `client`, `rate_limiter` → `pub(crate)`
3. `KrakenRestClient` — `client`, `rate_limiter` → `pub(crate)`
4. `CoinbaseRestClient` — `client`, `rate_limiter` → `pub(crate)`
5. `BybitRestClient<S>` — `client`, `rate_limiter` → `pub(crate)`
6. `HyperliquidRestClient` — `client`, `rate_limiter` → `pub(crate)`

**Pattern for Bulk clients (4 structs):**

```rust
// After:
#[non_exhaustive]
pub struct OkxBulkClient {
    pub(crate) client: reqwest::Client,
    pub(crate) config: BulkConfig,
}

impl OkxBulkClient {
    pub fn config(&self) -> &BulkConfig { &self.config }
}
```

Apply to all 4 bulk clients:
1. `BinanceBulkClient<S>` — `client`, `config` → `pub(crate)`
2. `OkxBulkClient` — `client`, `config` → `pub(crate)`
3. `BybitBulkClient<S>` — `client`, `config` → `pub(crate)`
4. `HyperliquidBulkClient` — `client`, `config` → `pub(crate)` (credentials already private)

**After each change:** `cargo build --all-features` to catch any external field access. Fix test code that accesses `client.config.verify_checksum` directly — use the accessor instead.

### 5.2.2 Channel / Market inner fields → `pub(crate)`

16 tuple structs wrap `pub SmolStr`. Change inner field to `pub(crate)`.

**All 16 already implement `AsRef<str>`** — most external uses can switch to `.as_ref()`.

**External `.0` access sites that must be updated (only 4 call sites):**

| Struct | File | Line | Current Usage | Fix |
|--------|------|------|---------------|-----|
| `BinanceChannel` | `binance/futures/liquidation.rs` | 114 | `BinanceChannel::LIQUIDATIONS.0` in `format!` | Use `.as_ref()` |
| `BinanceMarket` | `binance/spot/l2.rs` | 55 | `market.0` in URL format | Use `.as_ref()` |
| `BybitChannel` | `bybit/message.rs` | 72,76,80 | `BybitChannel::TRADES.0` etc in `format!` | Use `.as_ref()` |

**Implementation for each of the 16 structs:**

Exchanges: Binance, Bitfinex, Bitmex, Bybit, Coinbase, Gateio, Hyperliquid, Kraken, OKX (Channel + Market each = 18 total, but only 16 have SmolStr — verify at implementation time).

```rust
// Before:
pub struct BinanceChannel(pub SmolStr);

// After:
pub struct BinanceChannel(pub(crate) SmolStr);
```

No additional accessor method needed — `AsRef<str>` already provides read access. If `Display` is missing, add it for formatting convenience:

```rust
impl fmt::Display for BinanceChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0.as_str())
    }
}
```

### 5.2.3 `test_utils` → `#[cfg(test)]`

**File:** `barter-data/src/lib.rs` line 324

**Current:** `pub mod test_utils` — completely unused (zero call sites in entire workspace).

**Change to:** `#[cfg(test)] mod test_utils`

If any future need arises for cross-crate test utilities, add a `test-utils` feature flag at that point.

### 5.2.4 `process_buffered_events` → `pub(crate)`

**File:** `barter-data/src/lib.rs` line 242

Only used internally in `streams/task.rs`. Change `pub fn` → `pub(crate) fn`.

### 5.2.5 Other internal-only items → `pub(crate)`

All of these are defined in `barter-data/src/lib.rs` and only used in `streams/task.rs`:

| Item | Line | Current | Change To |
|------|------|---------|-----------|
| `WsSendRateLimiter` | 118 | `pub type` | `pub(crate) type` |
| `build_send_rate_limiter` | 127 | `pub fn` | `pub(crate) fn` |
| `distribute_messages_to_exchange` | 272 | `pub async fn` | `pub(crate) async fn` |
| `schedule_pings_to_exchange` | 305 | `pub async fn` | `pub(crate) async fn` |

### 5.2.6 Interval functions → `pub(crate)` where safe

**WebSocket-only exchanges (no `rest/klines.rs`, no public re-export) — make `pub(crate)`:**

| Function | File | Used From |
|----------|------|-----------|
| `bitmex_interval` | `exchange/bitmex/mod.rs:55` | `bitmex/channel.rs` via `use super::` |
| `gateio_interval` | `exchange/gateio/mod.rs:63` | `gateio/channel.rs` via `use super::` |
| `bitfinex_interval` | `exchange/bitfinex/mod.rs:78` | `bitfinex/mod.rs` in `resolve_market()` |

These 3 have no REST module and are only used within their own exchange module. Change `pub fn` → `pub(crate) fn`.

**REST-capable exchanges (have `rest/klines.rs` with `pub use` re-export) — keep `pub`:**

All 6 REST exchanges have a `rest/klines.rs` file that publicly re-exports the interval function:

| Function | Re-export Location |
|----------|--------------------|
| `binance_interval` | `exchange/binance/rest/klines.rs` — `pub use crate::exchange::binance::binance_interval;` |
| `bybit_interval` | `exchange/bybit/rest/klines.rs` — `pub use crate::exchange::bybit::bybit_interval;` |
| `coinbase_interval` | `exchange/coinbase/rest/klines.rs` — `pub use crate::exchange::coinbase::coinbase_interval;` |
| `hyperliquid_interval` | `exchange/hyperliquid/rest/klines.rs` — `pub use crate::exchange::hyperliquid::hyperliquid_interval;` |
| `kraken_interval` | `exchange/kraken/rest/klines.rs` — `pub use crate::exchange::kraken::kraken_interval;` |
| `okx_interval` | `exchange/okx/rest/klines.rs` — `pub use crate::exchange::okx::okx_interval;` |

These must remain `pub` because the re-export makes them reachable to downstream crates. Reducing to `pub(crate)` would break the `pub use` re-export path.

---

## Step 5.3: Dead Code Removal

### 5.3.1 Delete `BASE_URL_OKX`

**File:** `exchange/okx/mod.rs:88`
```rust
pub const BASE_URL_OKX: &str = "wss://ws.okx.com:8443/ws/v5/public";
```
Confirmed unused. The actual WebSocket URLs are `WEBSOCKET_BASE_URL_OKX_SPOT` and `WEBSOCKET_BASE_URL_OKX_PERPETUALS_USD` in their respective server modules.

### 5.3.2 Delete `OKX_KLINES_PATH`

**File:** `exchange/okx/rest/klines.rs:267`
```rust
pub const OKX_KLINES_PATH: &str = "/api/v5/market/candles";
```
Confirmed unused. Only `OKX_HISTORY_KLINES_PATH` is actually referenced.

### 5.3.3 Replace `interval_duration` with `Interval::to_ms`

**File:** `exchange/okx/rest/klines.rs:13`

This function IS used (in `try_into_candle` at line 160), but duplicates logic already in `Interval::to_ms()` (defined in `subscription/candle.rs:77`).

**Before:**
```rust
fn interval_duration(interval: Interval) -> TimeDelta { ... }

// Usage in try_into_candle:
let close_time = open_time + interval_duration(interval) - TimeDelta::milliseconds(1);
```

**After:**
```rust
// Delete interval_duration function. Replace usage:
let close_time = open_time + TimeDelta::milliseconds(interval.to_ms()) - TimeDelta::milliseconds(1);
```

Verify `Interval::to_ms()` returns equivalent values for all intervals before deleting.

**Coinbase has the same duplication:** `exchange/coinbase/rest/klines.rs:14` also defines a private `interval_duration()` with an exhaustive 14-arm match. Delete it and replace with `TimeDelta::milliseconds(interval.to_ms())` in the same way.

---

## Step 5.4: Unify Bybit + Hyperliquid Rate Limiters

### 5.4.1 Delete private type aliases

**Delete:**
- `BybitRateLimiter` in `exchange/bybit/rest/mod.rs:58-63`
- `HyperliquidRateLimiter` in `exchange/hyperliquid/rest/mod.rs:52-57`

Both are identical to the shared `ExchangeRateLimiter` in `rest/mod.rs:56-65`.

**Replace all usages:**
- `BybitRestClient.rate_limiter: Arc<BybitRateLimiter>` → `Arc<ExchangeRateLimiter>`
- `HyperliquidRestClient.rate_limiter: Arc<HyperliquidRateLimiter>` → `Arc<ExchangeRateLimiter>`

Add `use crate::rest::ExchangeRateLimiter;` to both modules.

### 5.4.2 Add `with_rate_limiter()` to Bybit and Hyperliquid

**Bybit** (`exchange/bybit/rest/mod.rs`) — add alongside existing `new()`:
```rust
pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
    let client = RestClient::new(
        Server::rest_base_url().to_owned(),
        PublicNoHeaders,
        BybitHttpParser,
    );
    Self {
        client: Arc::new(client),
        rate_limiter,
        _server: PhantomData,
    }
}
```

**Hyperliquid** (`exchange/hyperliquid/rest/mod.rs`) — add alongside existing `new()`:
```rust
pub fn with_rate_limiter(rate_limiter: Arc<ExchangeRateLimiter>) -> Self {
    let client = RestClient::new(
        HYPERLIQUID_REST_BASE_URL.to_owned(),
        PublicNoHeaders,
        HyperliquidHttpParser,
    );
    Self {
        client: Arc::new(client),
        rate_limiter,
    }
}
```

This gives all 6 REST clients a consistent `with_rate_limiter()` constructor.

### 5.4.3 Summary of rate limiter state after this step

| Exchange | Type | `new()` | `with_rate_limiter()` | `wait_for_rate_limit()` |
|----------|------|---------|-----------------------|------------------------|
| Binance | `ExchangeRateLimiter` | yes | yes | yes |
| OKX | `ExchangeRateLimiter` | yes | yes | yes |
| Kraken | `ExchangeRateLimiter` | yes | yes | yes |
| Coinbase | `ExchangeRateLimiter` | yes | yes | yes |
| Bybit | `ExchangeRateLimiter` | yes | **yes (new)** | yes |
| Hyperliquid | `ExchangeRateLimiter` | yes | **yes (new)** | yes |

---

## Step 5.5: Crate-Root Re-exports

**File:** `barter-data/src/lib.rs`

Currently there are zero `pub use` re-exports. Add them in tiers, organized by domain.

### 5.5.1 Core types (always available)

```rust
// Error
pub use error::DataError;

// Events
pub use event::{DataKind, MarketEvent, MarketIter};

// Subscriptions
pub use subscription::{Subscription, SubKind, SubscriptionKind};
pub use subscription::trade::{PublicTrade, PublicTrades};
pub use subscription::book::{
    OrderBook as OrderBookSnapshot, OrderBookEvent, OrderBookL1,
    OrderBooksL1, OrderBooksL2, OrderBooksL3,
};
pub use subscription::candle::{Candle, Candles, Interval};
pub use subscription::liquidation::{Liquidation, Liquidations};

// Order books
pub use books::{OrderBook, Level};

// Instruments
pub use instrument::InstrumentData;

// Streams
pub use streams::Streams;
pub use streams::builder::StreamBuilder;
```

### 5.5.2 REST types (feature-gated)

```rust
#[cfg(feature = "rest")]
pub use rest::{ExchangeRateLimiter, KlineFetcher, KlineRequest, TradeFetcher, TradeRequest};

#[cfg(feature = "rest")]
pub use trade::RestTrade;
```

### 5.5.3 Bulk types (feature-gated)

```rust
#[cfg(feature = "bulk")]
pub use bulk::{BulkConfig, BulkDayKlineFetcher, BulkDayTradeFetcher, BulkKlineRequest, BulkTradeRequest};
```

### 5.5.4 Shared types (available when either REST or bulk is enabled)

```rust
#[cfg(any(feature = "rest", feature = "bulk"))]
pub use retry::RetryPolicy;
```

**Why `any(rest, bulk)`:** `RetryPolicy` is used by both REST retry loops and bulk `download_bytes_resumable`. Gating under only `rest` would hide it from bulk-only consumers.

**Do NOT re-export at root:**
- Exchange-specific types (channels, markets, clients) — too many, use `exchange::binance::*` paths
- Internal types (parsers, HTTP parsers, server variants)
- DTO types (raw kline/trade structs per exchange)

**Naming conflict check:** `OrderBook` exists in both `books` and `subscription::book`. If both are re-exported, use an alias for one (e.g., `OrderBookSnapshot` for the subscription type, or just don't re-export the subscription one since `OrderBookEvent` wraps it).

---

## Step 5.6: Add `#[instrument]` to Key Functions

**Dependency:** `tracing` is already in `Cargo.toml`.

### 5.6.1 Retry module

**File:** `retry.rs`

```rust
#[tracing::instrument(skip(f), fields(max_retries = policy.max_retries))]
pub async fn retry_with_backoff<F, Fut, T, E>(...)

#[tracing::instrument(fields(attempt, sleep_ms))]
pub async fn apply_backoff(...)
```

### 5.6.2 Bulk download entry points

Add `#[instrument]` to public async methods in bulk client implementations. Skip large arguments (client, config), keep identifying fields (exchange, market, date):

```rust
#[tracing::instrument(skip(self), fields(exchange = "binance", %date))]
async fn fetch_bulk_trades_day(...)
```

Apply to all `BulkDayTradeFetcher` and `BulkDayKlineFetcher` trait implementations across:
- `exchange/binance/bulk/mod.rs`
- `exchange/okx/bulk/mod.rs`
- `exchange/bybit/bulk/mod.rs`
- `exchange/hyperliquid/bulk/mod.rs`
- `exchange/kraken/bulk/mod.rs`

### 5.6.3 REST fetch entry points

There are 11 existing manual `.instrument(span)` sites across the 6 REST exchange modules (6 `fetch_klines` + 5 `fetch_trades` — Hyperliquid has no `fetch_trades`). All use `tracing::info_span!` with consistent naming:

```rust
// Current pattern (all 11 sites):
let span = tracing::info_span!(
    "fetch_klines",                    // span name = function name
    exchange = "binance",              // literal string
    market = %request.market,
    interval = %request.interval,      // only on fetch_klines
);
async { ... }.instrument(span).await
```

**Span names already match function names** (`fetch_klines`, `fetch_trades`), so replacing with `#[instrument]` produces identical span names. Safe to replace:

```rust
// Replacement:
#[tracing::instrument(
    skip(self),
    fields(exchange = "binance", market = %request.market, interval = %request.interval)
)]
async fn fetch_klines(...) { ... }
```

**Key detail:** Use `level = "info"` in the `#[instrument]` attribute since the manual spans use `info_span!` (the default for `#[instrument]` is `info`, so this matches).

Apply across all 6 REST client `KlineFetcher` and `TradeFetcher` implementations:
- `exchange/binance/rest/mod.rs` — 2 sites (fetch_klines, fetch_trades)
- `exchange/okx/rest/mod.rs` — 2 sites
- `exchange/kraken/rest/mod.rs` — 2 sites
- `exchange/coinbase/rest/mod.rs` — 2 sites
- `exchange/bybit/rest/mod.rs` — 2 sites
- `exchange/hyperliquid/rest/mod.rs` — 1 site (fetch_klines only)

### 5.6.4 Stream initialization

```rust
// streams/consumer.rs
#[tracing::instrument(skip_all)]
pub async fn init_market_stream(...)

// streams/builder/mod.rs
#[tracing::instrument(skip_all)]
pub async fn init(...)

// streams/reconnect/stream.rs
#[tracing::instrument(skip_all)]
pub async fn init_reconnecting_stream(...)
```

---

## Step 5.7: Doc Comments on `wait_for_rate_limit()`

All 6 exchange REST clients have identical doc comments. Update them to clarify standalone/direct usage:

```rust
/// Wait until the rate limiter permits the next request.
///
/// This method is for **standalone/direct usage** — call it before each
/// REST API request when driving the client yourself (outside of a
/// `barter-collector` pipeline). When using `barter-collector`, rate
/// limiting is managed by the collector's orchestration layer.
///
/// Blocks asynchronously until a permit is available.
pub async fn wait_for_rate_limit(&self) {
    debug!("waiting for rate limit permit");
    self.rate_limiter.until_ready().await;
}
```

Apply to all 6 files:
1. `exchange/binance/rest/mod.rs:151`
2. `exchange/okx/rest/mod.rs:128`
3. `exchange/kraken/rest/mod.rs:138`
4. `exchange/coinbase/rest/mod.rs:123`
5. `exchange/bybit/rest/mod.rs:151`
6. `exchange/hyperliquid/rest/mod.rs:121`

### 5.7.1 Coinbase rate limiter doc mismatch

**File:** `exchange/coinbase/rest/mod.rs`

**Already addressed in Phase 4 step 4.5.** Verify the fix landed — the doc should say 10 req/s, matching the code. If Phase 4 hasn't run yet, fix it here.

---

## Step 5.8: Clean Up `barter-collector/src/download.rs`

**Context:** Phase 3 creates a `download.rs` placeholder in `barter-collector`. After Phase 3 is complete:

**Option A (preferred):** Delete the file entirely. `download_bytes_resumable` lives in `barter-data` as HTTP transport. No placeholder needed.

**Option B:** If kept, add a module-level doc comment explaining the architecture decision:

```rust
//! # Download — Architecture Note
//!
//! HTTP Range-resume download (`download_bytes_resumable`) lives in
//! `barter-data` as HTTP transport-level retry with partial-buffer
//! state. It is NOT collector-level orchestration.
//!
//! See `barter-data::exchange::binance::bulk` and
//! `barter-data::exchange::hyperliquid::bulk` for the implementations.
```

---

## Execution Order

```
5.3  Dead code removal                           (smallest diff, unblocks nothing but reduces noise)
5.4  Unify rate limiters                          (isolated, no deps on other steps)
5.1  #[non_exhaustive] sweep                      (large but mechanical)
5.2  Visibility tightening                        (depends on 5.1 for struct attrs, do together)
5.7  Doc comments on wait_for_rate_limit          (quick, do alongside 5.4)
5.5  Crate-root re-exports                        (depends on 5.2 visibility changes being settled)
5.6  #[instrument] addition                       (last — touches many files, should be final layer)
5.8  download.rs cleanup                          (trivial, do whenever Phase 3 is done)
```

**Parallelizable pairs:**
- 5.3 + 5.4 (independent)
- 5.1 + 5.7 (independent)

---

## Validation

After all steps:

```bash
cargo build --all-features          # no compile errors
cargo test --all-features           # all tests pass
cargo clippy --all-features         # no warnings
cargo doc --all-features            # no broken links, re-exports visible in rustdoc
```

Spot checks:
- [ ] Zero `pub` fields on RestClient/BulkClient structs (grep for `pub client:` and `pub rate_limiter:` and `pub config:`)
- [ ] Zero `(pub SmolStr)` in Channel/Market structs
- [ ] Zero private rate limiter type aliases (`BybitRateLimiter`, `HyperliquidRateLimiter`)
- [ ] `#[non_exhaustive]` on every public enum and every public struct with fields
- [ ] `#[instrument]` on retry, bulk fetch, REST fetch, and stream init functions
- [ ] Crate root re-exports appear in `cargo doc` output
- [ ] `BASE_URL_OKX` and `OKX_KLINES_PATH` are gone
- [ ] `interval_duration` in OKX klines is gone (replaced by `Interval::to_ms()`)
