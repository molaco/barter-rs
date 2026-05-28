# Phase 1: Error Type Overhaul — Merged Plan

## Objective

Replace all 125 stringly-typed `DataError::Socket(String)` call sites in non-WebSocket paths with structured error variants. After this phase, `DataError::Socket` is used **only** for actual WebSocket/transport errors originating from `SocketError`. Retry classification uses variant matching, not substring search.

## Constraints (from Rusty guides)

- **Arch §4**: Error types per operation, not per crate. New variants give callers structure to branch on failure mode.
- **Arch §2**: `#[non_exhaustive]` on public enums so adding variants is not semver-breaking.
- **Rusty §7**: `unwrap`/`expect` only with invariant comments. New error conversions must use `?` or explicit mapping.
- **Rusty §14**: Existing tests must pass. New variants need their own unit tests.

## Pre-flight Check

```bash
cargo test --all-features -p barter-data   # must pass before starting
cargo clippy --all-features -p barter-data  # note any existing warnings
```

---

## Step 1.1 — Add new `DataError` variants

**File:** `barter-data/src/error.rs`

Add `#[non_exhaustive]` to the enum, then add 8 new variants:

```rust
#[non_exhaustive]
pub enum DataError {
    // --- existing variants unchanged ---
    Socket(String),           // now ONLY for actual socket/WS errors
    Index(String),
    SubscriptionsEmpty,
    ConnectionTaskTerminated,
    UnsupportedSubKind { .. },
    InitialSnapshotMissing { .. },
    InitialSnapshotInvalid(String),
    CommandChannelClosed(String),
    Unsupported(String),
    NoConnection,
    SubscriptionMismatch,
    InvalidSequence { .. },

    // --- new variants ---

    /// HTTP transport error (request send failure, body read failure, non-2xx status).
    ///
    /// `status` is `Some` when an HTTP response was received; `None` for
    /// connection-level failures where no response arrived.
    /// `url` captures which endpoint failed — useful for bulk downloads with many URLs in flight.
    #[error("HTTP error (status={status:?}) for {url}: {message}")]
    Http {
        status: Option<u16>,
        url: String,
        message: String,
    },

    /// Exchange API returned a structured error response (HTTP 200 with error payload).
    ///
    /// Separated from `Http` because the HTTP request itself succeeded —
    /// the exchange application layer rejected the request. `code` is String
    /// because OKX uses `"51001"`, Binance uses numeric, Kraken uses string arrays.
    #[error("{exchange} API error (code {code}): {message}")]
    ExchangeApi {
        exchange: String,
        code: String,
        message: String,
    },

    /// Data parsing failure (CSV, JSON, timestamp, numeric conversion).
    ///
    /// Covers both bulk CSV parsing and REST DTO `TryFrom` conversion errors.
    /// Callers will not branch on "was it a timestamp or a price?" — they report and fail.
    #[error("data parse error: {0}")]
    DataParse(String),

    /// Archive decompression or extraction failure (ZIP, LZ4, gzip).
    #[error("bulk archive error: {0}")]
    BulkArchive(String),

    /// SHA-256 checksum mismatch on a downloaded archive.
    #[error("checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch {
        expected: String,
        actual: String,
    },

    /// Exchange does not support the requested interval.
    #[error("{exchange} does not support interval: {interval}")]
    UnsupportedInterval {
        exchange: String,
        interval: String,
    },

    /// Local filesystem I/O error (distinct from archive decompression).
    #[error("I/O error: {0}")]
    Io(String),

    /// Pagination safety limit exceeded.
    #[error("{exchange} pagination limit exceeded ({limit}) for {market}")]
    PaginationLimit {
        exchange: String,
        market: String,
        limit: usize,
    },
}
```

**Design decisions:**
- `Http.status` is `Option<u16>` not `StatusCode` — avoids leaking `reqwest`/`http` types into the public API. `None` = no HTTP response received (connection failure). No sentinel values.
- `Http` vs `ExchangeApi` are separate — "server returned HTTP 500" and "server returned HTTP 200 with OKX code 51001" are fundamentally different failure modes with different retry semantics.
- `Http.url` included — when bulk downloading hundreds of archive files concurrently, knowing which URL failed is essential for debugging.
- `ExchangeApi.code` is `String` — OKX uses `"51001"`, Binance uses numeric strings, Kraken uses string arrays. String is the lowest common denominator.
- `DataParse(String)` is opaque — callers won't branch on timestamp vs price parse failures.
- `BulkArchive(String)` is opaque — ZIP vs LZ4 distinction doesn't change caller behavior.
- `Io(String)` is separate from `BulkArchive` — Kraken local file open is genuinely different from decompression.
- `PaginationLimit` has structured fields — exchange/market/limit are useful for diagnostics without parsing strings.

**Derive compatibility:** `DataError` currently derives `Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize`. All new variants use only `String`, `Option<u16>`, and `usize` — all derives remain satisfied.

**Checkpoint:** `cargo check -p barter-data --all-features` — new variants compile, existing code unchanged.

---

## Step 1.2 — Fix `From<SocketError> for DataError`

**File:** `barter-data/src/error.rs`

Replace the lossy `.to_string()` conversion with a structured mapping. Read `barter-integration/src/error.rs` first and match every `SocketError` variant explicitly — no wildcard — so the compiler catches additions.

```rust
impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        match value {
            SocketError::Http(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("HTTP error: {e}"),
            },
            SocketError::HttpTimeout(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("request timeout: {e}"),
            },
            SocketError::HttpResponse { status, body } => DataError::Http {
                status: Some(status.as_u16()),
                url: String::new(),
                message: body,
            },
            SocketError::DeserialiseBinary { error, .. } => DataError::DataParse(error),
            SocketError::Deserialise { error, .. } => DataError::DataParse(error.to_string()),
            SocketError::DeserialiseProtobuf(e) => DataError::DataParse(e.to_string()),
            // Everything else → Socket (actual socket/WS errors)
            other => DataError::Socket(other.to_string()),
        }
    }
}
```

**Note:** `url` is empty and `exchange` is absent because `SocketError` doesn't carry that context. The important thing is HTTP status codes are preserved for retry classification.

**Checkpoint:** `cargo check -p barter-data --all-features`

---

## Step 1.3 — Update `HttpParser` implementations

Each exchange's `parse_api_error` receives `StatusCode` but currently discards it into a flat string. Update all 6 to use `ExchangeApi`:

| File | Exchange | Status source |
|------|----------|---------------|
| `exchange/binance/rest/mod.rs:47` | `"binance"` | `status.as_u16()` available but this is an API-level error |
| `exchange/okx/rest/mod.rs:48` | `"okx"` | same |
| `exchange/coinbase/rest/mod.rs:46` | `"coinbase"` | same |
| `exchange/kraken/rest/mod.rs:49` | `"kraken"` | same |
| `exchange/bybit/rest/mod.rs:49` | `"bybit"` | same |
| `exchange/hyperliquid/rest/mod.rs:42` | `"hyperliquid"` | same |

Example (Binance):
```rust
fn parse_api_error(&self, _status: StatusCode, error: Self::ApiError) -> Self::OutputError {
    DataError::ExchangeApi {
        exchange: "binance".into(),
        code: error.code.to_string(),
        message: error.msg,
    }
}
```

**Note:** `parse_api_error` is called when the HTTP response body contains an exchange error payload. The HTTP status is available but the error is application-level, so we use `ExchangeApi` not `Http`. The HTTP status can be included in the `message` if needed for debugging.

**Checkpoint:** `cargo check -p barter-data --all-features`

---

## Step 1.4 — Migrate call sites file-by-file

Work file-by-file. Each sub-step is independently committable. Run `cargo check` after each.

### 1.4a — `bulk/checksum.rs` (3 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 17 | `DataError::Socket(format!("checksum mismatch: ..."))` | `DataError::ChecksumMismatch { expected, actual }` |
| 38 | `DataError::Socket("empty checksum file".into())` | `DataError::DataParse("empty checksum file".into())` |
| 41 | `DataError::Socket(format!("invalid checksum length: ..."))` | `DataError::DataParse(format!("invalid checksum length: ..."))` |

### 1.4b — `exchange/binance/bulk/mod.rs` (17 sites)

**HTTP errors → `Http`:**

| Line | Category | New variant |
|------|----------|-------------|
| 181 | reqwest send failure | `Http { status: None, url: url.clone(), message: format!("request failed: {e}") }` |
| 192 | HTTP 416 | `Http { status: Some(416), url: url.clone(), message: "Range Not Satisfiable".into() }` |
| 196 | HTTP {status} | `Http { status: Some(status.as_u16()), url: url.clone(), message: format!("HTTP {status}") }` |
| 212 | body read failure | `Http { status: None, url: url.clone(), message: format!("body read failed: {e}") }` |
| 495 | S3 listing send | `Http { status: None, url: url.clone(), message: format!("S3 listing failed: {e}") }` |
| 498 | S3 listing body read | `Http { status: None, url: url.clone(), message: format!("S3 listing body read failed: {e}") }` |

**ZIP errors → `BulkArchive`:**

| Line | Message | New variant |
|------|---------|-------------|
| 256 | "failed to open ZIP archive" | `BulkArchive(...)` |
| 259 | "ZIP archive contains no files" | `BulkArchive(...)` |
| 266 | "failed to read first ZIP entry" | `BulkArchive(...)` |
| 270 | "failed to decompress ZIP entry" | `BulkArchive(...)` |

**Parse errors → `DataParse`:**

| Line | Message | New variant |
|------|---------|-------------|
| 238 | "checksum file is not UTF-8" | `DataParse(...)` |
| 551 | "S3 XML unescape error" | `DataParse(...)` |
| 575 | "S3 XML parse error" | `DataParse(...)` |

### 1.4c — `exchange/binance/bulk/trades.rs` (4 sites) → all `DataParse`

Invalid timestamp, price, quantity, CSV row parse.

### 1.4d — `exchange/binance/bulk/klines.rs` (8–9 sites) → all `DataParse`

Invalid open_time, close_time, open, high, low, close, volume, quote_volume, CSV row.

### 1.4e — `exchange/binance/rest/mod.rs` (3 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 252, 418, 563 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |

(The `parse_api_error` site was already handled in Step 1.3.)

### 1.4f — `exchange/okx/rest/mod.rs` (8 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 175 | OKX non-zero code response | `ExchangeApi { exchange: "okx", code, message }` |
| 275 | OKX non-zero code (klines) | `ExchangeApi { exchange: "okx", code, message }` |
| 286, 483, 625 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |
| 542 | Pagination max pages | `PaginationLimit { exchange: "okx", market, limit: MAX_TRADE_PAGES }` |
| 601 | Timestamp parse failure | `DataParse(...)` |

(The `parse_api_error` site was already handled in Step 1.3.)

### 1.4g — `exchange/okx/bulk/mod.rs` (12 sites)

**HTTP errors → `Http`:**

| Lines | Current | New |
|-------|---------|-----|
| 62, 72, 78 | OKX bulk HTTP errors | `Http { status, url, message }` |
| 142, 151, 159 | OKX monthly HTTP errors | `Http { status, url, message }` |

**ZIP errors → `BulkArchive`:**

| Lines | Current | New |
|-------|---------|-----|
| 94, 104, 106 | OKX bulk ZIP errors | `BulkArchive(...)` |
| 173, 183, 185 | OKX monthly ZIP errors | `BulkArchive(...)` |

### 1.4h — `exchange/okx/bulk/trades.rs` (6 sites) → all `DataParse`

Timestamp, side, price, size, CSV row.

### 1.4i — `exchange/bybit/rest/mod.rs` (4 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 258 | Bybit retCode error (klines) | `ExchangeApi { exchange: "bybit", code, message }` |
| 270 | TryFrom conversion | `DataParse` |
| 425 | Bybit retCode error (trades) | `ExchangeApi { exchange: "bybit", code, message }` |
| 437 | TryFrom conversion | `DataParse` |

(The `parse_api_error` site was already handled in Step 1.3.)

### 1.4j — `exchange/bybit/bulk/mod.rs` (3 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 127 | HTTP request failed | `Http { status: None, url, message }` |
| 137 | HTTP {status} error | `Http { status: Some(status.as_u16()), url, message }` |
| 169 | Streaming decompress failed | `BulkArchive(...)` |

### 1.4k — `exchange/bybit/bulk/trades.rs` (10 sites) → all `DataParse`

Timestamp, side, price, size/volume, CSV row (both spot and perps parsers).

### 1.4l — `exchange/coinbase/rest/mod.rs` (2 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 226, 385 | TryFrom conversion | `DataParse` |

(The `parse_api_error` site was already handled in Step 1.3.)

### 1.4m — `exchange/kraken/rest/mod.rs` (6 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 191, 252 | Kraken API error string | `ExchangeApi { exchange: "kraken", code: "".into(), message }` |
| 200, 261 | TryFrom conversion | `DataParse` |
| 445, 497 | Timestamp nanosecond overflow | `DataParse(format!("timestamp out of nanosecond range: {dt}"))` |

(The `parse_api_error` site was already handled in Step 1.3.)

### 1.4n — `exchange/kraken/rest/klines.rs` (2 sites) → `DataParse`

"not an object", kline array parse failure.

### 1.4o — `exchange/kraken/rest/trades.rs` (2 sites) → `DataParse`

### 1.4p — `exchange/kraken/bulk/mod.rs` (5 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 25 | File open failed | `Io(format!("failed to open ZIP file '{}': {e}", path.display()))` |
| 32 | ZIP read failed | `Io(format!("failed to read ZIP archive '{}': {e}", path.display()))` |
| 49 | Pair not found | `BulkArchive(format!("no CSV file found for pair '{}' in '{}'", ...))` |
| 57 | ZIP entry read | `BulkArchive(...)` |
| 61 | CSV data read | `BulkArchive(...)` |

### 1.4q — `exchange/kraken/bulk/trades.rs` (5 sites) → all `DataParse`

Timestamp, price, volume, side, CSV row.

### 1.4r — `exchange/hyperliquid/rest/mod.rs` (1 site)

| Line | Current | New variant |
|------|---------|-------------|
| 239 | TryFrom conversion | `DataParse` |

(The `parse_api_error` site was already handled in Step 1.3.)

### 1.4s — `exchange/hyperliquid/bulk/mod.rs` (6 sites)

| Line | Current | New variant |
|------|---------|-------------|
| 117 | HTTP request failed | `Http { status: None, url, message }` |
| 128 | HTTP 416 | `Http { status: Some(416), url, message }` |
| 132 | HTTP {status} | `Http { status: Some(status.as_u16()), url, message }` |
| 149 | Response body read | `Http { status: None, url, message: format!("body read failed: {e}") }` |
| 162 | LZ4 decompression | `BulkArchive(format!("LZ4 decompression failed: {e}"))` |
| 166 | LZ4 task panic | `BulkArchive(format!("LZ4 task panicked: {e}"))` |

### 1.4t — `exchange/hyperliquid/bulk/trades.rs` (6 sites) → all `DataParse`

Timestamp, price, size, side, UTF-8 decode.

### 1.4u — Unsupported interval sites → `UnsupportedInterval` (10 sites)

| File | Lines | Exchange |
|------|-------|----------|
| `exchange/kraken/mod.rs` | 76-93 (6 sites) | `"kraken"` |
| `exchange/coinbase/mod.rs` | 64 | `"coinbase"` |
| `exchange/gateio/mod.rs` | 74 | `"gateio"` |
| `exchange/bitfinex/mod.rs` | 90 | `"bitfinex"` |
| `exchange/bitmex/mod.rs` | 61 | `"bitmex"` |

Replace pattern:
```rust
// BEFORE
DataError::Socket(format!("Kraken does not support 3m interval"))
// AFTER
DataError::UnsupportedInterval { exchange: "kraken".into(), interval: "3m".into() }
```

**Checkpoint after all 1.4 sub-steps:**
```bash
cargo test --all-features -p barter-data
grep -rn 'DataError::Socket' barter-data/src/ | grep -v '// ' | grep -v 'test'
# Should only show: error.rs (variant def + From<SocketError> fallback), retry.rs (match arm)
```

---

## Step 1.5 — Rewrite `is_retriable_data_error`

**File:** `barter-data/src/retry.rs`

This step is **last** because the retry tests must use the new variants.

```rust
pub fn is_retriable_data_error(error: &DataError) -> bool {
    match error {
        DataError::Http { status, message, .. } => match status {
            Some(429) => true,   // Too Many Requests
            Some(418) => true,   // Binance IP ban / rate limit
            Some(500) => true,   // Internal Server Error
            Some(502) => true,   // Bad Gateway
            Some(503) => true,   // Service Unavailable
            Some(504) => true,   // Gateway Timeout
            Some(416) => true,   // Range Not Satisfiable (retry from scratch)
            None => {
                // Connection-level failures (no HTTP response received)
                let msg = message.to_lowercase();
                msg.contains("timeout")
                    || msg.contains("connection")
                    || msg.contains("reset by peer")
                    || msg.contains("broken pipe")
                    || msg.contains("error sending request")
            }
            _ => false,
        },
        DataError::ExchangeApi { .. } => false,
        DataError::DataParse(_) => false,
        DataError::BulkArchive(_) => false,
        DataError::ChecksumMismatch { .. } => false,
        DataError::UnsupportedInterval { .. } => false,
        DataError::Io(_) => false,
        DataError::PaginationLimit { .. } => false,
        // Legacy socket errors from WS layer
        DataError::Socket(msg) => {
            let msg = msg.to_lowercase();
            msg.contains("timeout")
                || msg.contains("connection")
                || msg.contains("reset by peer")
                || msg.contains("broken pipe")
        }
        _ => false,
    }
}
```

**Key changes vs current:**
- `Http` with 429/418/5xx → retriable. `None` status (connection failure) → retriable based on message.
- `Http` with 400/401/403/404 → not retriable.
- `ExchangeApi` → not retriable (exchange said no; retrying won't help).
- `Socket` retains string matching only for actual WS transport errors — no more HTTP status substring matching.
- All new variants listed explicitly. `_ => false` catches existing variants (`Index`, `SubscriptionsEmpty`, etc.) and future `#[non_exhaustive]` additions.

**Rewrite tests:** All existing `is_retriable_data_error` tests must use new variants:

```rust
#[test]
fn test_retriable_http_429() {
    let err = DataError::Http { status: Some(429), url: "test".into(), message: "rate limit".into() };
    assert!(is_retriable_data_error(&err));
}

#[test]
fn test_not_retriable_http_400() {
    let err = DataError::Http { status: Some(400), url: "test".into(), message: "bad request".into() };
    assert!(!is_retriable_data_error(&err));
}

#[test]
fn test_retriable_connection_failure() {
    let err = DataError::Http { status: None, url: "test".into(), message: "connection reset by peer".into() };
    assert!(is_retriable_data_error(&err));
}

#[test]
fn test_not_retriable_parse() {
    let err = DataError::DataParse("invalid timestamp".into());
    assert!(!is_retriable_data_error(&err));
}

#[test]
fn test_not_retriable_exchange_api() {
    let err = DataError::ExchangeApi { exchange: "okx".into(), code: "51001".into(), message: "not found".into() };
    assert!(!is_retriable_data_error(&err));
}
```

---

## Step 1.6 — Update remaining tests

Search for tests that match on `DataError::Socket` or assert on error message strings:

```bash
grep -rn "DataError::Socket" barter-data/src/ --include="*.rs" | grep -E "test|assert"
```

Update each to match on the new variant. Example:

```rust
// BEFORE
assert!(matches!(result, Err(DataError::Socket(msg)) if msg.contains("checksum")));
// AFTER
assert!(matches!(result, Err(DataError::ChecksumMismatch { .. })));
```

---

## Step 1.7 — Final verification

1. `cargo test --workspace --all-features` — all tests pass
2. `cargo clippy --workspace --all-features` — no new warnings
3. Verify grep counts:
```bash
# Should return ONLY actual WS/socket error paths
grep -rn "DataError::Socket(" barter-data/src/ --include="*.rs" | wc -l
# Target: < 10 (variant def, From<SocketError> fallback, retry match arm, maybe a few WS paths)

# Should return zero — no more format! into Socket
grep -rn 'DataError::Socket(format!' barter-data/src/ --include="*.rs" | wc -l
# Target: 0
```
4. Verify `is_retriable_data_error` has zero substring matching for HTTP status codes.

---

## Execution Order Summary

```
1.1  Add 8 variants to DataError        (error.rs — additive, no breakage)
1.2  Fix From<SocketError>              (error.rs — preserves structure)
1.3  Update 6 HttpParser impls          (6 rest/mod.rs files — Socket → ExchangeApi)
1.4a checksum.rs                        (3 sites)
1.4b binance/bulk/mod.rs                (17 sites)
1.4c binance/bulk/trades.rs             (4 sites)
1.4d binance/bulk/klines.rs             (8-9 sites)
1.4e binance/rest/mod.rs                (3 sites)
1.4f okx/rest/mod.rs                    (8 sites)
1.4g okx/bulk/mod.rs                    (12 sites)
1.4h okx/bulk/trades.rs                (6 sites)
1.4i bybit/rest/mod.rs                 (4 sites)
1.4j bybit/bulk/mod.rs                 (3 sites)
1.4k bybit/bulk/trades.rs              (10 sites)
1.4l coinbase/rest/mod.rs              (2 sites)
1.4m kraken/rest/mod.rs                (6 sites)
1.4n kraken/rest/klines.rs             (2 sites)
1.4o kraken/rest/trades.rs             (2 sites)
1.4p kraken/bulk/mod.rs                (5 sites)
1.4q kraken/bulk/trades.rs             (5 sites)
1.4r hyperliquid/rest/mod.rs           (1 site)
1.4s hyperliquid/bulk/mod.rs           (6 sites)
1.4t hyperliquid/bulk/trades.rs        (6 sites)
1.4u unsupported intervals             (10 sites across 5 files)
1.5  Rewrite is_retriable_data_error   (retry.rs — LAST, depends on all variants populated)
1.6  Update remaining tests            (test files)
1.7  Final verification                (grep counts, clippy, full test suite)
```

Steps 1.1-1.3 are prerequisites. Steps 1.4a-1.4u are independent and can go in any order. Step 1.5 must be last among code changes.

Each step compiles independently. Run `cargo check -p barter-data --all-features` after each.

---

## Site Count Summary

| New Variant | Call Sites | Files |
|-------------|-----------|-------|
| `Http` | ~18 | 5 (bulk download files) |
| `ExchangeApi` | ~14 | 6 (HttpParser impls + inline retCode checks) |
| `DataParse` | ~62 | 12 (bulk/trades, bulk/klines, rest TryFrom, rest parse) |
| `BulkArchive` | ~15 | 6 (ZIP, LZ4, gzip across all bulk modules) |
| `ChecksumMismatch` | 1 | 1 (checksum.rs) |
| `UnsupportedInterval` | 10 | 5 (kraken, coinbase, gateio, bitfinex, bitmex) |
| `Io` | 2 | 1 (kraken/bulk local file open) |
| `PaginationLimit` | 1 | 1 (okx/rest) |
| **Total** | **~123** | |

Remaining `DataError::Socket` after migration: ~2 (variant definition + `From<SocketError>` fallback arm for non-HTTP `SocketError` variants).
