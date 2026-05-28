# Phase 1: Error Type Overhaul — Detailed Implementation Plan

## Objective

Replace all stringly-typed `DataError::Socket(String)` usage in non-WebSocket paths with structured error variants. After this phase, `DataError::Socket` is used **only** for actual WebSocket/transport errors originating from `SocketError`. Retry classification uses variant matching, not substring search.

## Constraints (from Rusty guides)

- **Arch §4**: Error types per operation, not per crate. `DataError` is the crate boundary error — new variants give callers structure to branch on failure mode.
- **Rusty §7**: `unwrap`/`expect` only with invariant comments. New error conversions must use `?` or explicit mapping.
- **Arch §2**: `#[non_exhaustive]` on public enums so adding variants is not semver-breaking.
- **Rusty §14**: Existing tests must pass. New variants need their own unit tests.

## Pre-flight Check

```bash
cargo test --all-features -p barter-data   # must pass before starting
cargo clippy --all-features -p barter-data  # note any existing warnings
```

---

## Step 1.1 — Add new `DataError` variants

**File:** `barter-data/src/error.rs`

Add `#[non_exhaustive]` to the enum, then add these variants:

```rust
#[non_exhaustive]
pub enum DataError {
    // ... existing variants unchanged ...

    /// HTTP API error from an exchange REST endpoint.
    ///
    /// Captures the HTTP status code and exchange-specific error details.
    /// Retry decisions branch on `status` (e.g., 429/5xx = retry, 4xx = don't).
    #[error("HTTP API error (status {status}, exchange: {exchange}): {message}")]
    HttpApi {
        status: u16,
        exchange: String,
        message: String,
    },

    /// Bulk archive download/decompression failure.
    ///
    /// ZIP extraction, LZ4 decompression, gzip streaming, or archive I/O errors.
    #[error("bulk archive error: {0}")]
    BulkArchive(String),

    /// CSV or data format parsing error.
    ///
    /// Invalid timestamps, prices, sizes, sides, or malformed CSV rows
    /// from bulk archive or REST response parsing.
    #[error("data parse error: {0}")]
    DataParse(String),

    /// SHA-256 checksum mismatch on a downloaded archive.
    #[error("checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    /// Interval not supported by this exchange.
    #[error("unsupported interval for {exchange}: {interval}")]
    UnsupportedInterval { exchange: String, interval: String },

    /// Pagination error: max pages exceeded, cursor stall, or cursor parse failure.
    #[error("pagination error: {0}")]
    Pagination(String),
}
```

**Why `DataParse` instead of `CsvParse`:** The same variant covers REST DTO conversion errors (`.map_err(DataError::Socket)` on `TryFrom` failures), not just CSV. "DataParse" is more accurate.

**Derive implications:** `DataError` currently derives `Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize`. The new variants use only `String` and `u16`, so all derives remain satisfied. No changes needed.

**Checkpoint:** `cargo check -p barter-data --all-features` — new variants compile, existing code unchanged.

---

## Step 1.2 — Migrate `DataError::Socket` call sites to correct variants

Work file-by-file. Each sub-step is independently committable.

### 1.2a — `bulk/checksum.rs` (3 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 17 | `DataError::Socket(format!("checksum mismatch: ..."))` | `DataError::ChecksumMismatch { expected, actual }` |
| 38 | `DataError::Socket("empty checksum file".into())` | `DataError::DataParse("empty checksum file".into())` |
| 41 | `DataError::Socket(format!("invalid checksum length: ..."))` | `DataError::DataParse(format!("invalid checksum length: ..."))` |

### 1.2b — `exchange/binance/bulk/mod.rs` (17 sites)

**HTTP/download errors → `HttpApi` or `BulkArchive`:**

| Line | Category | New variant |
|------|----------|-------------|
| 181 | HTTP request failed | `HttpApi { status: 0, exchange: "binance".into(), message }` (no status available from reqwest send error — use 0 as sentinel) |
| 192 | HTTP 416 | `HttpApi { status: 416, exchange: "binance".into(), message }` |
| 196 | HTTP {status} | `HttpApi { status: status.as_u16(), exchange: "binance".into(), message }` |
| 212 | Response body read failure | `BulkArchive(format!("failed to read response body for {url}: {e}"))` |
| 238 | Checksum UTF-8 | `DataParse(format!("checksum file is not UTF-8: {e}"))` |
| 495 | S3 listing request | `HttpApi { status: 0, exchange: "binance".into(), message }` |
| 498 | S3 listing body read | `BulkArchive(format!("S3 listing read body failed: {e}"))` |
| 551 | S3 XML unescape | `DataParse(format!("S3 XML unescape error: {err}"))` |
| 575 | S3 XML parse | `DataParse(format!("S3 XML parse error: {e}"))` |

**ZIP extraction errors → `BulkArchive`:**

| Line | Current message | New variant |
|------|----------------|-------------|
| 256 | "failed to open ZIP archive" | `BulkArchive(...)` |
| 259 | "ZIP archive contains no files" | `BulkArchive(...)` |
| 266 | "failed to read first ZIP entry" | `BulkArchive(...)` |
| 270 | "failed to decompress ZIP entry" | `BulkArchive(...)` |

### 1.2c — `exchange/binance/bulk/trades.rs` (4 sites) → `DataParse`

All are CSV/data parsing: invalid timestamp, price, quantity, CSV row parse.

### 1.2d — `exchange/binance/bulk/klines.rs` (8 sites) → `DataParse`

All are CSV/data parsing: invalid open_time, close_time, open, high, low, close, volume, quote_volume, CSV row parse.

### 1.2e — `exchange/binance/rest/mod.rs` (4 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 48 | `parse_api_error` → `DataError::Socket(format!("Binance API error (code {}): {}", ...))` | `DataError::HttpApi { status: status.as_u16(), exchange: "binance".into(), message }` — **Note:** `parse_api_error` receives `StatusCode` already. Pipe it through. |
| 252, 418, 563 | `.map_err(DataError::Socket)` on `TryFrom` conversion | `.map_err(DataError::DataParse)` |

### 1.2f — `exchange/okx/rest/mod.rs` (9 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 49 | `parse_api_error` | `HttpApi { status, exchange: "okx", message }` |
| 175 | OKX non-zero code response | `HttpApi { status: 0, exchange: "okx", message }` (application-level error, no HTTP status) |
| 275 | OKX non-zero code (klines) | Same as above |
| 286, 483 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |
| 542 | Pagination max pages exceeded | `DataError::Pagination(format!(...))` |
| 601 | Trade timestamp parse failure | `DataError::DataParse(format!(...))` |
| 625 | TryFrom batch conversion | `.map_err(DataError::DataParse)` |

### 1.2g — `exchange/okx/bulk/mod.rs` (12 sites)

**HTTP errors → `HttpApi`:**

| Lines | Current | New |
|-------|---------|-----|
| 62, 72, 78 | OKX bulk HTTP errors | `HttpApi { status, exchange: "okx", message }` |
| 142, 151, 159 | OKX monthly HTTP errors | Same |

**ZIP errors → `BulkArchive`:**

| Lines | Current | New |
|-------|---------|-----|
| 94, 104, 106 | OKX bulk ZIP errors | `BulkArchive(...)` |
| 173, 183, 185 | OKX monthly ZIP errors | `BulkArchive(...)` |

### 1.2h — `exchange/okx/bulk/trades.rs` (6 sites) → `DataParse`

All are CSV/data parsing: timestamp, side, price, size, CSV row.

### 1.2i — `exchange/bybit/rest/mod.rs` (5 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 50 | `parse_api_error` | `HttpApi { status, exchange: "bybit", message }` |
| 258 | Bybit retCode error (klines) | `HttpApi { status: 0, exchange: "bybit", message }` |
| 270 | TryFrom conversion | `DataParse` |
| 425 | Bybit retCode error (trades) | `HttpApi { status: 0, exchange: "bybit", message }` |
| 437 | TryFrom conversion | `DataParse` |

### 1.2j — `exchange/bybit/bulk/mod.rs` (3 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 127 | HTTP request failed | `HttpApi { status: 0, exchange: "bybit", ... }` |
| 137 | HTTP {status} error | `HttpApi { status: status.as_u16(), exchange: "bybit", ... }` |
| 169 | Streaming decompress failed | `BulkArchive(...)` |

### 1.2k — `exchange/bybit/bulk/trades.rs` (10 sites) → `DataParse`

All are CSV/data parsing: timestamp, side, price, size/volume, CSV row.

### 1.2l — `exchange/coinbase/rest/mod.rs` (3 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 47 | `parse_api_error` | `HttpApi { status, exchange: "coinbase", message }` |
| 226, 385 | TryFrom conversion | `DataParse` |

### 1.2m — `exchange/kraken/rest/mod.rs` (7 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 50 | `parse_api_error` | `HttpApi { status, exchange: "kraken", message }` |
| 191, 252 | Kraken API error string | `HttpApi { status: 0, exchange: "kraken", message }` |
| 200, 261 | TryFrom conversion | `DataParse` |
| 445, 497 | Timestamp nanosecond overflow | `DataParse(format!("timestamp out of nanosecond range: {dt}"))` |

### 1.2n — `exchange/kraken/rest/klines.rs` (2 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 41 | "not an object" | `DataParse(...)` |
| 53 | Kline array parse | `DataParse(...)` |

### 1.2o — `exchange/kraken/rest/trades.rs` (2 sites) → `DataParse`

### 1.2p — `exchange/kraken/bulk/mod.rs` (5 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 25 | File open failed | `BulkArchive(...)` |
| 32 | ZIP read failed | `BulkArchive(...)` |
| 49 | Pair not found | `BulkArchive(...)` |
| 57 | ZIP entry read | `BulkArchive(...)` |
| 61 | CSV data read | `BulkArchive(...)` |

### 1.2q — `exchange/kraken/bulk/trades.rs` (5 sites) → `DataParse`

### 1.2r — `exchange/hyperliquid/rest/mod.rs` (2 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 43 | `parse_api_error` | `HttpApi { status, exchange: "hyperliquid", message }` |
| 239 | TryFrom conversion | `DataParse` |

### 1.2s — `exchange/hyperliquid/bulk/mod.rs` (6 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 117 | HTTP request failed | `HttpApi { status: 0, exchange: "hyperliquid", ... }` |
| 128 | HTTP 416 | `HttpApi { status: 416, exchange: "hyperliquid", ... }` |
| 132 | HTTP {status} | `HttpApi { status: status.as_u16(), exchange: "hyperliquid", ... }` |
| 149 | Response body read | `BulkArchive(format!("failed to read response: {e}"))` |
| 162 | LZ4 decompression | `BulkArchive(format!("LZ4 decompression failed: {e}"))` |
| 166 | LZ4 task panic | `BulkArchive(format!("LZ4 task panicked: {e}"))` |

### 1.2t — `exchange/hyperliquid/bulk/trades.rs` (6 sites) → `DataParse`

### 1.2u — Unsupported interval sites → `UnsupportedInterval`

| File | Lines | Exchange |
|------|-------|----------|
| `exchange/kraken/mod.rs` | 76-93 | `UnsupportedInterval { exchange: "kraken", interval: format!("{interval}") }` |
| `exchange/coinbase/mod.rs` | 64-66 | `UnsupportedInterval { exchange: "coinbase", interval: format!("{unsupported}") }` |
| `exchange/gateio/mod.rs` | 74 | `UnsupportedInterval { exchange: "gateio", interval: ... }` |
| `exchange/bitfinex/mod.rs` | 90 | `UnsupportedInterval { exchange: "bitfinex", interval: ... }` |
| `exchange/bitmex/mod.rs` | 61 | `UnsupportedInterval { exchange: "bitmex", interval: ... }` |

### 1.2v — `exchange/okx/rest/mod.rs` `filter_trades_by_time` and `MAX_TRADE_PAGES`

The `MAX_TRADE_PAGES` exceeded error (line 542) → `DataError::Pagination(...)` (covered in 1.2f).

**Checkpoint after all 1.2 sub-steps:**
```bash
cargo test --all-features -p barter-data   # must pass
grep -rn 'DataError::Socket' barter-data/src/ | grep -v '// ' | grep -v 'test'
# Should only show: error.rs (variant definition), retry.rs (match arm), error.rs (From<SocketError>)
```

---

## Step 1.3 — Rewrite `is_retriable_data_error`

**File:** `barter-data/src/retry.rs`

Replace the substring-matching implementation with variant-based matching:

```rust
pub fn is_retriable_data_error(error: &DataError) -> bool {
    match error {
        DataError::HttpApi { status, .. } => matches!(
            status,
            429 | 418  // 418 = Binance IP ban rate-limit code
                | 500 | 502 | 503 | 504
                | 0  // status 0 = transport-level failure (no HTTP response received)
        ),
        DataError::Socket(msg) => {
            // Actual WebSocket/transport errors: retry on transient network issues
            let lower = msg.to_lowercase();
            lower.contains("timeout")
                || lower.contains("connection")
                || lower.contains("reset by peer")
                || lower.contains("broken pipe")
                || lower.contains("error sending request")
        }
        DataError::BulkArchive(_) => false,
        DataError::DataParse(_) => false,
        DataError::ChecksumMismatch { .. } => false,
        DataError::UnsupportedInterval { .. } => false,
        DataError::Pagination(_) => false,
        _ => false,
    }
}
```

**Key changes:**
- `HttpApi` with 429/418/5xx → retriable. Status 0 (transport failure) → retriable.
- `HttpApi` with 400/401/403/404 → **not** retriable (falls through `matches!`).
- `Socket` only matches actual transport strings now — no more HTTP status substring matching.
- All new variants explicitly listed (not caught by `_ => false`) so the compiler warns if future variants are added without updating this function. Use `#[deny(non_exhaustive_omitted_patterns)]` or just list them explicitly.

**Update tests:** All existing `is_retriable_data_error` tests in `retry.rs` (lines 189-253) must be rewritten to use the new variants instead of `DataError::Socket("HTTP 429 ...")`. Example:

```rust
#[test]
fn test_is_retriable_http_429() {
    let err = DataError::HttpApi { status: 429, exchange: "test".into(), message: "rate limit".into() };
    assert!(is_retriable_data_error(&err));
}

#[test]
fn test_is_not_retriable_http_400() {
    let err = DataError::HttpApi { status: 400, exchange: "test".into(), message: "bad request".into() };
    assert!(!is_retriable_data_error(&err));
}
```

---

## Step 1.4 — Fix `From<SocketError> for DataError`

**File:** `barter-data/src/error.rs`

Current implementation loses all structure:
```rust
impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        Self::Socket(value.to_string())
    }
}
```

New implementation preserves HTTP status information:
```rust
impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        match value {
            SocketError::HttpResponse(status, body) => DataError::HttpApi {
                status: status.as_u16(),
                exchange: String::new(),
                message: body,
            },
            SocketError::HttpTimeout(e) => DataError::HttpApi {
                status: 0,
                exchange: String::new(),
                message: format!("request timeout: {e}"),
            },
            SocketError::Http(e) => DataError::HttpApi {
                status: 0,
                exchange: String::new(),
                message: format!("HTTP error: {e}"),
            },
            other => DataError::Socket(other.to_string()),
        }
    }
}
```

**Note:** `exchange` is empty string because `SocketError` doesn't carry exchange context — the caller can wrap it with context if needed. The important thing is that HTTP status codes are preserved for retry classification.

**Update test:** The existing test `test_data_error_is_terminal` (line 96) constructs `DataError::from(SocketError::Sink)` — this still maps to `DataError::Socket`, so the test remains valid.

---

## Step 1.5 — Update `HttpParser` implementations

Each exchange's `parse_api_error` receives `StatusCode` but currently discards it. Update all 6:

| File | Exchange | Change |
|------|----------|--------|
| `exchange/binance/rest/mod.rs:47` | Binance | Use `status.as_u16()`, `"binance"` |
| `exchange/okx/rest/mod.rs:48` | OKX | Use `status.as_u16()`, `"okx"` |
| `exchange/coinbase/rest/mod.rs:46` | Coinbase | Use `status.as_u16()`, `"coinbase"` |
| `exchange/kraken/rest/mod.rs:49` | Kraken | Use `status.as_u16()`, `"kraken"` |
| `exchange/bybit/rest/mod.rs:49` | Bybit | Use `status.as_u16()`, `"bybit"` |
| `exchange/hyperliquid/rest/mod.rs:42` | Hyperliquid | Use `status.as_u16()`, `"hyperliquid"` |

Example (Binance):
```rust
fn parse_api_error(&self, status: StatusCode, error: Self::ApiError) -> Self::OutputError {
    DataError::HttpApi {
        status: status.as_u16(),
        exchange: "binance".into(),
        message: format!("code {}: {}", error.code, error.msg),
    }
}
```

---

## Execution Order

```
1.1  Add variants to DataError          (error.rs only, no breakage)
1.4  Fix From<SocketError>              (error.rs only, no breakage — new variants are additive)
1.5  Update HttpParser impls            (6 files, changes Socket→HttpApi at API boundaries)
1.2a Migrate checksum.rs               (3 sites)
1.2b-1.2t Migrate exchange files        (exchange-by-exchange, ~85 sites total)
1.2u Migrate unsupported intervals      (5 files)
1.3  Rewrite is_retriable_data_error    (retry.rs — must be LAST since tests depend on variants existing)
```

Each step compiles independently. Run `cargo test --all-features -p barter-data` after each sub-step.

---

## Validation Criteria

1. `cargo test --all-features -p barter-data` — all tests pass
2. `cargo clippy --all-features -p barter-data` — no new warnings
3. `grep -rn 'DataError::Socket' barter-data/src/ | grep -v test | grep -v '#\[error'` shows ONLY:
   - `error.rs` — the `Socket(String)` variant definition
   - `error.rs` — the `From<SocketError>` fallback arm (`other => DataError::Socket(...)`)
   - `retry.rs` — the `DataError::Socket(msg)` match arm in `is_retriable_data_error`
4. No `DataError::Socket` in any bulk, REST, exchange, or checksum production code paths

---

## Site Count Summary

| New Variant | Call Sites | Files |
|-------------|-----------|-------|
| `HttpApi` | ~30 | 12 (6 HttpParser + 6 bulk/rest) |
| `BulkArchive` | ~25 | 6 (binance, okx, bybit, kraken, hyperliquid bulk) |
| `DataParse` | ~40 | 12 (all bulk/trades.rs, bulk/klines.rs, rest TryFrom sites) |
| `ChecksumMismatch` | 1 | 1 (checksum.rs) |
| `UnsupportedInterval` | ~10 | 5 (kraken, coinbase, gateio, bitfinex, bitmex mod.rs) |
| `Pagination` | 1 | 1 (okx/rest/mod.rs) |
| **Total** | **~107** | |
