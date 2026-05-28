# Phase 1: Error Type Overhaul — Merged Implementation Plan

## Objective

Replace all stringly-typed `DataError::Socket(String)` usage in non-WebSocket paths with structured error variants. After this phase, `DataError::Socket` is used **only** for actual WebSocket/transport errors originating from `SocketError`. Retry classification uses variant matching, not substring search.

## Constraints

- **Arch §4**: Error types per operation, not per crate. `DataError` is the crate boundary error — new variants give callers structure to branch on failure mode.
- **Arch §2**: `#[non_exhaustive]` on public enums so adding variants is not semver-breaking.
- **Rusty §7**: `unwrap`/`expect` only with invariant comments. New error conversions must use `?` or explicit mapping.
- Existing tests must pass at every sub-step. New variants need their own unit tests.

## Pre-flight

```bash
cargo test --all-features -p barter-data
cargo clippy --all-features -p barter-data
```

---

## Step 1 — Add new `DataError` variants

**File:** `barter-data/src/error.rs`

Add `#[non_exhaustive]` to the enum, then append these variants:

```rust
#[non_exhaustive]
pub enum DataError {
    // --- existing variants unchanged ---

    /// HTTP transport error (request send failure, body read failure, non-2xx status).
    /// `status` is `Some` when an HTTP response was received; `None` for connection-level failures.
    #[error("HTTP error (status={status:?}) for {url}: {message}")]
    Http {
        status: Option<u16>,
        url: String,
        message: String,
    },

    /// Exchange API returned a structured error response (HTTP 200 with error payload,
    /// or a parsed error body from a non-200 response).
    #[error("{exchange} API error (code {code}): {message}")]
    ExchangeApi {
        exchange: String,
        code: String,
        message: String,
    },

    /// Data parsing failure (CSV, JSON, timestamp, numeric conversion, TryFrom).
    #[error("data parse error: {0}")]
    DataParse(String),

    /// Archive decompression or extraction failure (ZIP, LZ4, gzip).
    #[error("bulk archive error: {0}")]
    BulkArchive(String),

    /// SHA-256 checksum mismatch on a downloaded archive.
    #[error("checksum mismatch: expected {expected}, got {actual}")]
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

    /// Local filesystem I/O error (e.g. opening a local archive file).
    #[error("I/O error: {0}")]
    Io(String),

    /// Pagination safety limit exceeded.
    #[error("{exchange} pagination limit exceeded ({limit}) for {market}")]
    PaginationLimit {
        exchange: String,
        market: String,
        limit: u32,
    },
}
```

**Design rationale:**
- `Http` vs `ExchangeApi` are separate because retry semantics differ: `Http` retries on status codes (429/5xx), `ExchangeApi` can retry on exchange-specific rate-limit codes (e.g. OKX `"50011"`).
- `Http.status` is `Option<u16>` not `u16` — `None` means no HTTP response was received (connection failure, DNS failure, timeout before response).
- `Http.url` captures the URL for diagnostics. `ExchangeApi` does not need it — the exchange name + code is sufficient.
- `DataParse(String)` covers both CSV bulk parsing AND REST DTO `TryFrom` conversion errors. Callers never branch on "was it a timestamp or a price" — they report and move on.
- `BulkArchive(String)` is opaque — ZIP vs LZ4 vs gzip distinction doesn't change caller behavior.
- `Io(String)` for local filesystem errors (Kraken local ZIP). Separate from `BulkArchive` because it's not a download issue.
- `PaginationLimit` is structured so callers can log the exchange/market/limit without parsing a string.

**Derive compatibility:** `DataError` derives `Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize`. All new fields are `String`, `Option<u16>`, or `u32` — all derives remain satisfied.

**Checkpoint:** `cargo check -p barter-data --all-features`

---

## Step 2 — Fix `From<SocketError> for DataError`

**File:** `barter-data/src/error.rs`

Replace the lossy `.to_string()` conversion with explicit matching on every `SocketError` variant (no wildcard — compiler catches future additions):

```rust
impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        match value {
            // HTTP errors → preserve status for retry classification
            SocketError::HttpResponse(status, body) => DataError::Http {
                status: Some(status.as_u16()),
                url: String::new(),
                message: body,
            },
            SocketError::HttpTimeout(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("timeout: {e}"),
            },
            SocketError::Http(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("HTTP error: {e}"),
            },
            // Deserialization errors → DataParse (data format failure, not a socket issue)
            SocketError::Deserialise { error, .. } => DataError::DataParse(error.to_string()),
            SocketError::DeserialiseBinary { error, .. } => DataError::DataParse(error.to_string()),
            SocketError::DeserialiseProtobuf { error, .. } => DataError::DataParse(error.to_string()),
            // Everything else → Socket (actual WebSocket/transport errors)
            SocketError::Sink => DataError::Socket("sink error".to_string()),
            SocketError::Serialise(e) => DataError::Socket(format!("serialise: {e}")),
            SocketError::QueryParams(e) => DataError::Socket(format!("query params: {e}")),
            SocketError::UrlEncoded(e) => DataError::Socket(format!("url encoded: {e}")),
            SocketError::UrlParse(e) => DataError::Socket(format!("url parse: {e}")),
            SocketError::Subscribe(msg) => DataError::Socket(format!("subscribe: {msg}")),
            SocketError::Terminated(msg) => DataError::Socket(format!("terminated: {msg}")),
            SocketError::Unsupported { entity, item } => {
                DataError::Socket(format!("unsupported {entity}: {item}"))
            }
            SocketError::WebSocket(e) => DataError::Socket(format!("websocket: {e}")),
            SocketError::Unidentifiable(id) => {
                DataError::Socket(format!("unidentifiable: {id}"))
            }
            SocketError::Exchange(msg) => DataError::Socket(format!("exchange: {msg}")),
        }
    }
}
```

**Note:** `SocketError::HttpResponse` is a tuple variant `HttpResponse(StatusCode, String)` — verify exact shape in `barter-integration/src/error.rs` before implementing. If `DeserialiseProtobuf` uses a different structure (e.g. `DeserialiseProtobuf(DecodeError)` vs struct), adjust accordingly.

**Checkpoint:** `cargo check -p barter-data --all-features`. Existing test `test_data_error_is_terminal` uses `DataError::from(SocketError::Sink)` → still maps to `Socket`, still works.

---

## Step 3 — Update `HttpParser` implementations

Each exchange's `parse_api_error` receives `StatusCode` but currently discards it. Update all 6 to produce `ExchangeApi`:

| File | Exchange | Status passed? |
|------|----------|----------------|
| `exchange/binance/rest/mod.rs:47` | binance | Yes — `_status` param |
| `exchange/okx/rest/mod.rs:48` | okx | Yes — `_status` param |
| `exchange/coinbase/rest/mod.rs:46` | coinbase | Yes — `_status` param |
| `exchange/kraken/rest/mod.rs:49` | kraken | Yes — `_status` param |
| `exchange/bybit/rest/mod.rs:49` | bybit | Yes — `_status` param |
| `exchange/hyperliquid/rest/mod.rs:42` | hyperliquid | Yes — `status` param |

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

**Note:** These `HttpParser` impls are called by the `RestClient` framework when the server returns an error response body. The `StatusCode` is available but belongs in the transport layer — the `ExchangeApi` variant captures the exchange's own error code/message, which is what callers care about. The HTTP status is separately available via the `Http` variant if the `RestClient` framework also produces one.

**Checkpoint:** `cargo check -p barter-data --all-features`

---

## Step 4 — Migrate call sites (file-by-file)

Each sub-step is independently committable. Work one file at a time.

### 4a — `bulk/checksum.rs` (3 sites)

| Line | Current | New |
|------|---------|-----|
| 17 | `Socket(format!("checksum mismatch: expected {expected}, got {actual}"))` | `ChecksumMismatch { expected: expected.to_owned(), actual }` |
| 38 | `Socket("empty checksum file".into())` | `DataParse("empty checksum file".into())` |
| 41 | `Socket(format!("invalid checksum length: ..."))` | `DataParse(format!("invalid checksum length: ..."))` |

### 4b — `exchange/binance/bulk/mod.rs` (17 sites)

**HTTP errors → `Http`:**

| Line | Description | New |
|------|-------------|-----|
| 181 | Request send failure | `Http { status: None, url: url.to_owned(), message: format!("request failed: {e}") }` |
| 192 | HTTP 416 | `Http { status: Some(416), url: url.to_owned(), message: "Range Not Satisfiable, restarting".into() }` |
| 196 | HTTP {status} | `Http { status: Some(status.as_u16()), url: url.to_owned(), message: format!("HTTP {status}") }` |
| 212 | Body read failure | `Http { status: None, url: url.to_owned(), message: format!("body read failed: {e}") }` |
| 495 | S3 listing request failure | `Http { status: None, url: url.to_owned(), message: format!("S3 listing request failed: {e}") }` |
| 498 | S3 listing body read | `Http { status: None, url: url.to_owned(), message: format!("S3 listing body read failed: {e}") }` |

**ZIP errors → `BulkArchive`:**

| Line | Description |
|------|-------------|
| 256 | "failed to open ZIP archive" |
| 259 | "ZIP archive contains no files" |
| 266 | "failed to read first ZIP entry" |
| 270 | "failed to decompress ZIP entry" |

**Parse errors → `DataParse`:**

| Line | Description |
|------|-------------|
| 238 | "checksum file is not UTF-8" |
| 551 | "S3 XML unescape error" |
| 575 | "S3 XML parse error" |

### 4c — `exchange/binance/bulk/trades.rs` (4 sites) → `DataParse`

Invalid timestamp, price, quantity, CSV row parse.

### 4d — `exchange/binance/bulk/klines.rs` (8 sites) → `DataParse`

Invalid open_time, close_time, open, high, low, close, volume, quote_volume, CSV row parse.

### 4e — `exchange/binance/rest/mod.rs` (3 sites)

| Line | Current | New |
|------|---------|-----|
| 252, 418, 563 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |

(The `parse_api_error` site at line 48 is already covered by Step 3.)

### 4f — `exchange/okx/rest/mod.rs` (8 sites)

| Line | Current | New |
|------|---------|-----|
| 175 | OKX non-zero code (trades) | `ExchangeApi { exchange: "okx".into(), code: response.code.clone(), message: response.msg.clone() }` |
| 275 | OKX non-zero code (klines) | Same pattern |
| 286, 483 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |
| 542 | MAX_TRADE_PAGES exceeded | `PaginationLimit { exchange: "okx".into(), market: state.market.clone(), limit: MAX_TRADE_PAGES }` |
| 601 | Trade timestamp parse failure | `DataParse(format!(...))` |
| 625 | TryFrom batch conversion | `.map_err(DataError::DataParse)` |

(The `parse_api_error` site at line 49 is already covered by Step 3.)

### 4g — `exchange/okx/bulk/mod.rs` (12 sites)

**HTTP errors → `Http`:**

| Lines | Description | `url` available? |
|-------|-------------|------------------|
| 62, 78 | Request failure / body read | Yes (`url` in scope) |
| 72 | HTTP {status} | Yes |
| 142, 159 | Monthly request failure / body read | Yes |
| 151 | Monthly HTTP {status} | Yes |

**ZIP errors → `BulkArchive`:** Lines 94, 104, 106, 173, 183, 185.

### 4h — `exchange/okx/bulk/trades.rs` (6 sites) → `DataParse`

Timestamp, side, price, size, CSV row.

### 4i — `exchange/bybit/rest/mod.rs` (4 sites)

| Line | Current | New |
|------|---------|-----|
| 258 | Bybit retCode error (klines) | `ExchangeApi { exchange: "bybit".into(), code: response.ret_code.to_string(), message: response.ret_msg.clone() }` |
| 270 | TryFrom conversion | `DataParse` |
| 425 | Bybit retCode error (trades) | Same `ExchangeApi` pattern |
| 437 | TryFrom conversion | `DataParse` |

(The `parse_api_error` site at line 50 is already covered by Step 3.)

### 4j — `exchange/bybit/bulk/mod.rs` (3 sites)

| Line | Current | New |
|------|---------|-----|
| 127 | HTTP request failed | `Http { status: None, url: url.to_owned(), ... }` |
| 137 | HTTP {status} error | `Http { status: Some(status.as_u16()), url: url.to_owned(), ... }` |
| 169 | Streaming decompress failed | `BulkArchive(...)` |

### 4k — `exchange/bybit/bulk/trades.rs` (10 sites) → `DataParse`

Timestamp, side, price, size/volume, CSV row (both spot and perpetuals parsers).

### 4l — `exchange/coinbase/rest/mod.rs` (2 sites)

| Line | Current | New |
|------|---------|-----|
| 226, 385 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |

(The `parse_api_error` site at line 47 is already covered by Step 3.)

### 4m — `exchange/kraken/rest/mod.rs` (6 sites)

| Line | Current | New |
|------|---------|-----|
| 191 | Kraken API error string (klines) | `ExchangeApi { exchange: "kraken".into(), code: String::new(), message: msg }` |
| 252 | Kraken API error string (trades) | Same |
| 200, 261 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |
| 445, 497 | Timestamp nanosecond overflow | `DataParse(format!("timestamp out of nanosecond range: {dt}"))` |

(The `parse_api_error` site at line 50 is already covered by Step 3.)

### 4n — `exchange/kraken/rest/klines.rs` (2 sites) → `DataParse`

"not an object", kline array parse.

### 4o — `exchange/kraken/rest/trades.rs` (2 sites) → `DataParse`

"not an object", trades array parse.

### 4p — `exchange/kraken/bulk/mod.rs` (5 sites)

| Line | Current | New |
|------|---------|-----|
| 25 | File open failed | `Io(format!("failed to open ZIP file '{}': {e}", path.display()))` |
| 32 | ZIP archive read failed | `Io(format!("failed to read ZIP archive '{}': {e}", path.display()))` |
| 49 | Pair not found in archive | `BulkArchive(format!("no CSV file found for pair '{pair}' in '{}'", path.display()))` |
| 57 | ZIP entry read failed | `BulkArchive(format!("failed to read ZIP entry: {e}"))` |
| 61 | CSV data read from ZIP failed | `BulkArchive(format!("failed to read CSV data from ZIP: {e}"))` |

### 4q — `exchange/kraken/bulk/trades.rs` (5 sites) → `DataParse`

Timestamp, price, volume, side, CSV row.

### 4r — `exchange/hyperliquid/rest/mod.rs` (1 site)

| Line | Current | New |
|------|---------|-----|
| 239 | `.map_err(DataError::Socket)` on TryFrom | `.map_err(DataError::DataParse)` |

(The `parse_api_error` site at line 43 is already covered by Step 3.)

### 4s — `exchange/hyperliquid/bulk/mod.rs` (6 sites)

| Line | Current | New |
|------|---------|-----|
| 117 | HTTP request failed | `Http { status: None, url: url.clone(), ... }` |
| 128 | HTTP 416 | `Http { status: Some(416), url: url.clone(), ... }` |
| 132 | HTTP {status} | `Http { status: Some(status.as_u16()), url: url.clone(), ... }` |
| 149 | Response body read failure | `Http { status: None, url: url.clone(), message: format!("body read failed: {e}") }` |
| 162 | LZ4 decompression failed | `BulkArchive(format!("LZ4 decompression failed: {e}"))` |
| 166 | LZ4 task panicked | `BulkArchive(format!("LZ4 task panicked: {e}"))` |

### 4t — `exchange/hyperliquid/bulk/trades.rs` (6 sites) → `DataParse`

Timestamp, price, size, side, UTF-8.

### 4u — Unsupported interval sites → `UnsupportedInterval`

| File | Lines | Exchange |
|------|-------|----------|
| `exchange/kraken/mod.rs` | 76-93 (6 arms) | `"kraken"` |
| `exchange/coinbase/mod.rs` | 64-66 | `"coinbase"` |
| `exchange/gateio/mod.rs` | 74 | `"gateio"` |
| `exchange/bitfinex/mod.rs` | 90 | `"bitfinex"` |
| `exchange/bitmex/mod.rs` | 61 | `"bitmex"` |

Pattern:
```rust
// BEFORE
Err(DataError::Socket("Kraken does not support 3m interval".into()))

// AFTER
Err(DataError::UnsupportedInterval {
    exchange: "kraken".into(),
    interval: format!("{interval}"),
})
```

**Checkpoint after all Step 4 sub-steps:**
```bash
cargo test --all-features -p barter-data
grep -rn 'DataError::Socket' barter-data/src/ | grep -v test | grep -v '#\[error'
# Should only show: error.rs (variant def), error.rs (From<SocketError> fallback), retry.rs (match arm)
```

---

## Step 5 — Rewrite `is_retriable_data_error`

**File:** `barter-data/src/retry.rs`

Replace substring matching with variant-based matching:

```rust
pub fn is_retriable_data_error(error: &DataError) -> bool {
    match error {
        DataError::Http { status, message, .. } => match status {
            Some(429) => true,  // Too Many Requests
            Some(418) => true,  // Binance IP ban / rate limit
            Some(500) => true,  // Internal Server Error
            Some(502) => true,  // Bad Gateway
            Some(503) => true,  // Service Unavailable
            Some(504) => true,  // Gateway Timeout
            Some(416) => true,  // Range Not Satisfiable (retry from scratch)
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
        DataError::ExchangeApi { code, .. } => {
            // Exchange-specific retriable codes can be added here.
            // e.g., OKX "50011" (rate limit), etc.
            let _ = code;
            false
        }
        // Actual WebSocket/transport errors — retry on transient network issues
        DataError::Socket(msg) => {
            let msg = msg.to_lowercase();
            msg.contains("timeout")
                || msg.contains("connection")
                || msg.contains("reset by peer")
                || msg.contains("broken pipe")
        }
        // These are never retriable
        DataError::DataParse(_) => false,
        DataError::BulkArchive(_) => false,
        DataError::ChecksumMismatch { .. } => false,
        DataError::UnsupportedInterval { .. } => false,
        DataError::Io(_) => false,
        DataError::PaginationLimit { .. } => false,
        // All other existing variants
        _ => false,
    }
}
```

**Rewrite all tests** in the `#[cfg(test)] mod tests` block (lines 86-253). Replace `DataError::Socket("HTTP 429 ...")` constructions with the correct new variants:

```rust
#[test]
fn test_is_retriable_http_429() {
    let err = DataError::Http {
        status: Some(429),
        url: "https://example.com".into(),
        message: "Too Many Requests".into(),
    };
    assert!(is_retriable_data_error(&err));
}

#[test]
fn test_is_not_retriable_http_400() {
    let err = DataError::Http {
        status: Some(400),
        url: "https://example.com".into(),
        message: "Bad Request".into(),
    };
    assert!(!is_retriable_data_error(&err));
}

#[test]
fn test_is_retriable_connection_failure() {
    let err = DataError::Http {
        status: None,
        url: "https://example.com".into(),
        message: "connection reset by peer".into(),
    };
    assert!(is_retriable_data_error(&err));
}

#[test]
fn test_is_not_retriable_parse_error() {
    let err = DataError::DataParse("invalid timestamp".into());
    assert!(!is_retriable_data_error(&err));
}

#[test]
fn test_is_not_retriable_exchange_api() {
    let err = DataError::ExchangeApi {
        exchange: "binance".into(),
        code: "-1121".into(),
        message: "Invalid symbol".into(),
    };
    assert!(!is_retriable_data_error(&err));
}
```

Also update `test_data_error_is_terminal` in `error.rs` if it references `DataError::Socket` for non-socket scenarios.

**Checkpoint:** `cargo test --all-features -p barter-data`

---

## Execution Order Summary

```
Step 1   Add variants to DataError             (error.rs — additive, no breakage)
Step 2   Fix From<SocketError>                 (error.rs — additive, no breakage)
Step 3   Update HttpParser impls               (6 files — Socket→ExchangeApi at API boundary)
Step 4a  Migrate bulk/checksum.rs              (3 sites)
Step 4b  Migrate binance/bulk/mod.rs           (17 sites)
Step 4c  Migrate binance/bulk/trades.rs        (4 sites)
Step 4d  Migrate binance/bulk/klines.rs        (8 sites)
Step 4e  Migrate binance/rest/mod.rs           (3 sites)
Step 4f  Migrate okx/rest/mod.rs               (8 sites)
Step 4g  Migrate okx/bulk/mod.rs               (12 sites)
Step 4h  Migrate okx/bulk/trades.rs            (6 sites)
Step 4i  Migrate bybit/rest/mod.rs             (4 sites)
Step 4j  Migrate bybit/bulk/mod.rs             (3 sites)
Step 4k  Migrate bybit/bulk/trades.rs          (10 sites)
Step 4l  Migrate coinbase/rest/mod.rs          (2 sites)
Step 4m  Migrate kraken/rest/mod.rs            (6 sites)
Step 4n  Migrate kraken/rest/klines.rs         (2 sites)
Step 4o  Migrate kraken/rest/trades.rs         (2 sites)
Step 4p  Migrate kraken/bulk/mod.rs            (5 sites)
Step 4q  Migrate kraken/bulk/trades.rs         (5 sites)
Step 4r  Migrate hyperliquid/rest/mod.rs       (1 site)
Step 4s  Migrate hyperliquid/bulk/mod.rs       (6 sites)
Step 4t  Migrate hyperliquid/bulk/trades.rs    (6 sites)
Step 4u  Migrate unsupported intervals         (5 files, ~10 sites)
Step 5   Rewrite is_retriable_data_error       (retry.rs — LAST, after all sites migrated)
```

Each step compiles independently. Run `cargo test --all-features -p barter-data` after each.

---

## Validation Criteria

1. `cargo test --all-features -p barter-data` — all tests pass
2. `cargo clippy --all-features -p barter-data` — no new warnings
3. `grep -rn 'DataError::Socket' barter-data/src/ | grep -v test | grep -v '#\[error'` shows ONLY:
   - `error.rs` — the `Socket(String)` variant definition
   - `error.rs` — the `From<SocketError>` fallback arm (`other => DataError::Socket(...)` for actual WS errors)
   - `retry.rs` — the `DataError::Socket(msg)` match arm in `is_retriable_data_error`
4. Zero `DataError::Socket(format!(` in any production code
5. No substring matching on HTTP status codes anywhere

---

## Site Count Summary

| New Variant | Sites | Files |
|-------------|-------|-------|
| `Http` | ~24 | 5 bulk download files |
| `ExchangeApi` | ~12 | 6 HttpParser + inline retCode/code checks |
| `DataParse` | ~52 | 12 (bulk/trades.rs, bulk/klines.rs, rest TryFrom, rest/klines.rs, rest/trades.rs) |
| `BulkArchive` | ~20 | 5 (binance, okx, bybit, kraken, hyperliquid bulk) |
| `ChecksumMismatch` | 1 | 1 (checksum.rs) |
| `UnsupportedInterval` | ~10 | 5 (kraken, coinbase, gateio, bitfinex, bitmex) |
| `Io` | 2 | 1 (kraken/bulk/mod.rs) |
| `PaginationLimit` | 1 | 1 (okx/rest/mod.rs) |
| **Total** | **~122** | |
