# Phase 1: Error Type Overhaul — Detailed Plan

## Current State

`DataError::Socket(String)` is used **125 times** across the codebase. In zero cases does it represent an actual socket error. It is a stringly-typed catch-all covering 9 distinct failure categories. The retry classifier (`is_retriable_data_error`) does substring matching on 13 patterns inside these strings, which is fragile and can false-positive.

### Call-site breakdown

| Category | Count | What it actually is |
|----------|-------|---------------------|
| C. Parse failures (CSV, JSON, timestamp, numeric) | 49 | Data format errors |
| A. HTTP request/transport errors | 18 | Network/HTTP failures |
| D. Decompression / archive failures | 15 | ZIP, LZ4, gzip I/O |
| I. Indirect `.map_err(DataError::Socket)` wrapping | 13 | Parse failures via String TryFrom |
| B. Exchange API error responses | 12 | API-level errors with codes |
| F. Unsupported interval | 10 | Validation errors |
| E. Checksum failures | 4 | Integrity verification |
| G. Local file I/O | 3 | Filesystem errors |
| H. Pagination safety limit | 1 | Resource limit exceeded |

### Current `From<SocketError>` impl

```rust
impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        Self::Socket(value.to_string())  // destroys all type information
    }
}
```

---

## Target State

`DataError::Socket(String)` retained **only** for actual WebSocket/socket errors propagated from `SocketError` in the streaming layer. All other uses replaced with structured, matchable variants. `is_retriable_data_error` matches on variants and HTTP status codes, not substrings.

---

## Step 1: Define new `DataError` variants

**File:** `barter-data/src/error.rs`

Add these variants to the existing `DataError` enum and add `#[non_exhaustive]` to the enum:

```rust
#[non_exhaustive]
pub enum DataError {
    // --- existing variants (unchanged) ---
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
    /// `status` is `Some` when an HTTP response was received; `None` for connection-level failures.
    Http {
        status: Option<u16>,
        url: String,
        message: String,
    },

    /// Exchange API returned a structured error response (HTTP 200 with error payload).
    ExchangeApi {
        exchange: String,
        code: String,
        message: String,
    },

    /// Data parsing failure (CSV, JSON, timestamp, numeric conversion).
    Parse(String),

    /// Archive decompression or extraction failure (ZIP, LZ4, gzip).
    Archive(String),

    /// SHA-256 checksum mismatch.
    ChecksumMismatch {
        expected: String,
        actual: String,
    },

    /// Exchange does not support the requested interval.
    UnsupportedInterval {
        exchange: String,
        interval: String,
    },

    /// Local filesystem I/O error.
    Io(String),

    /// Pagination safety limit exceeded.
    PaginationLimit {
        exchange: String,
        market: String,
        limit: usize,
    },
}
```

**Design decisions (per Arch Guide §4):**
- `Http.status` is `Option<u16>` not `StatusCode` — avoids leaking `reqwest`/`http` types into the public error API.
- `ExchangeApi.code` is `String` not `u32` — OKX uses string codes like `"51001"`, Binance uses numeric, Kraken uses string arrays.
- `Parse(String)` is a single opaque variant — callers will not branch on "was it a timestamp or a price?", they will just report.
- `Archive(String)` is opaque — same reasoning: ZIP vs LZ4 distinction doesn't change caller behavior.
- `#[non_exhaustive]` allows adding variants later without semver break.

**Constraint (Arch Guide §4):** "Will the caller behave differently based on the failure mode?" → `Http` (retry), `ExchangeApi` (maybe retry on rate limit), `ChecksumMismatch` (re-download), `UnsupportedInterval` (fail fast). The rest are opaque report-and-fail.

---

## Step 2: Fix `From<SocketError> for DataError`

**File:** `barter-data/src/error.rs`

Replace the lossy `.to_string()` conversion with a structured mapping:

```rust
impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        match value {
            SocketError::Http(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: e.to_string(),
            },
            SocketError::HttpTimeout(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("timeout: {e}"),
            },
            SocketError::HttpResponse { status, body } => DataError::Http {
                status: Some(status.as_u16()),
                url: String::new(),
                message: body,
            },
            // DeserialiseBinary / Deserialise / DeserialiseProtobuf → Parse
            SocketError::DeserialiseBinary { error, .. } => DataError::Parse(error),
            SocketError::Deserialise { error, .. } => DataError::Parse(error.to_string()),
            SocketError::DeserialiseProtobuf(e) => DataError::Parse(e.to_string()),
            // Everything else → Socket (actual socket/WS errors)
            other => DataError::Socket(other.to_string()),
        }
    }
}
```

Check exact `SocketError` variants by reading `barter-integration/src/error.rs` before implementing. Match every variant explicitly — no wildcard — so the compiler catches additions.

---

## Step 3: Rewrite `is_retriable_data_error`

**File:** `barter-data/src/retry.rs`

Replace substring matching with variant matching:

```rust
pub fn is_retriable_data_error(error: &DataError) -> bool {
    match error {
        DataError::Http { status, message, .. } => {
            match status {
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
            }
        }
        DataError::ExchangeApi { code, .. } => {
            // Exchange-specific retriable codes can be added here
            // e.g., OKX "50011" (rate limit), Binance error codes, etc.
            let _ = code;
            false
        }
        // These are never retriable:
        DataError::Parse(_) => false,
        DataError::Archive(_) => false,
        DataError::ChecksumMismatch { .. } => false,
        DataError::UnsupportedInterval { .. } => false,
        DataError::Io(_) => false,
        DataError::PaginationLimit { .. } => false,
        // Legacy socket errors from WS layer — retry on transport failures
        DataError::Socket(msg) => {
            let msg = msg.to_lowercase();
            msg.contains("timeout")
                || msg.contains("connection")
                || msg.contains("reset by peer")
                || msg.contains("broken pipe")
        }
        // All other existing variants are not retriable
        _ => false,
    }
}
```

Delete the old function body entirely. The `_ => false` wildcard at the end is safe because `DataError` is `#[non_exhaustive]` — new variants default to non-retriable.

---

## Step 4: Migrate call sites — Category F (Unsupported Interval)

**10 sites. Easiest — mechanical find-and-replace.**

Files:
- `exchange/kraken/mod.rs` — 6 sites (lines 76, 79, 82, 85, 88, 91)
- `exchange/coinbase/mod.rs` — 1 site (line 64)
- `exchange/bitmex/mod.rs` — 1 site (line 61)
- `exchange/gateio/mod.rs` — 1 site (line 74)
- `exchange/bitfinex/mod.rs` — 1 site (line 90)

Replace pattern:
```rust
// BEFORE
DataError::Socket(format!("Kraken does not support 3m interval"))

// AFTER
DataError::UnsupportedInterval {
    exchange: "Kraken".to_owned(),
    interval: "3m".to_owned(),
}
```

**Validation:** `cargo check`. Grep for `"does not support"` in DataError contexts returns zero.

---

## Step 5: Migrate call sites — Category E (Checksum)

**4 sites. Small and contained in 2 files.**

Files:
- `bulk/checksum.rs` — 3 sites (lines 17, 38, 41)
- `exchange/binance/bulk/mod.rs` — 1 site (line 238)

Replace pattern:
```rust
// BEFORE (checksum mismatch)
DataError::Socket(format!("checksum mismatch: expected {expected}, got {actual}"))

// AFTER
DataError::ChecksumMismatch {
    expected: expected.to_owned(),
    actual: actual.to_owned(),
}

// BEFORE (empty/invalid checksum file)
DataError::Socket(format!("empty checksum file"))

// AFTER
DataError::Parse("empty checksum file".to_owned())

// BEFORE (invalid checksum length)
DataError::Socket(format!("invalid checksum length: expected 64 hex chars, got {len}"))

// AFTER
DataError::Parse(format!("invalid checksum length: expected 64 hex chars, got {len}"))

// BEFORE (checksum not UTF-8)
DataError::Socket(format!("checksum file is not UTF-8: {e}"))

// AFTER
DataError::Parse(format!("checksum file is not UTF-8: {e}"))
```

**Validation:** `cargo check`. Grep for `"checksum mismatch"` in DataError contexts returns zero.

---

## Step 6: Migrate call sites — Category D (Archive/Decompression)

**15 sites across 6 files.**

Files:
- `exchange/binance/bulk/mod.rs` — 4 sites (ZIP)
- `exchange/okx/bulk/mod.rs` — 6 sites (ZIP)
- `exchange/kraken/bulk/mod.rs` — 2 sites (ZIP)
- `exchange/bybit/bulk/mod.rs` — 1 site (gzip)
- `exchange/hyperliquid/bulk/mod.rs` — 2 sites (LZ4)

Replace pattern:
```rust
// BEFORE
DataError::Socket(format!("failed to open ZIP archive: {e}"))

// AFTER
DataError::Archive(format!("failed to open ZIP archive: {e}"))
```

Mechanical: change `DataError::Socket(` to `DataError::Archive(` for all ZIP, LZ4, and gzip error messages. Keep the format strings as-is.

**Validation:** `cargo check`. All archive-related error strings now in `Archive` variant.

---

## Step 7: Migrate call sites — Category G (Local File I/O)

**3 sites in 1 file: `exchange/kraken/bulk/mod.rs`**

Replace pattern:
```rust
// BEFORE
DataError::Socket(format!("failed to open ZIP file '{}': {e}", path.display()))

// AFTER
DataError::Io(format!("failed to open ZIP file '{}': {e}", path.display()))
```

---

## Step 8: Migrate call sites — Category A (HTTP Transport)

**18 sites across 6 files.**

Files:
- `exchange/binance/bulk/mod.rs` — 5 sites
- `exchange/okx/bulk/mod.rs` — 6 sites
- `exchange/bybit/bulk/mod.rs` — 2 sites
- `exchange/hyperliquid/bulk/mod.rs` — 4 sites
- `exchange/binance/bulk/mod.rs` (S3) — 2 sites (not really in a separate exchange directory, just separate logical group)

Replace patterns:

```rust
// BEFORE — request send failure (no HTTP response)
DataError::Socket(format!("HTTP request failed for {url}: {e}"))

// AFTER
DataError::Http {
    status: None,
    url: url.to_owned(),
    message: format!("request failed: {e}"),
}

// BEFORE — non-success HTTP status
DataError::Socket(format!("HTTP {status} for {url}"))

// AFTER
DataError::Http {
    status: Some(status.as_u16()),
    url: url.to_owned(),
    message: format!("HTTP {status}"),
}

// BEFORE — HTTP 416
DataError::Socket(format!("HTTP 416 for {url}, restarting"))

// AFTER
DataError::Http {
    status: Some(416),
    url: url.to_owned(),
    message: "Range Not Satisfiable, restarting".to_owned(),
}

// BEFORE — body read failure
DataError::Socket(format!("failed to read response body for {url}: {e}"))

// AFTER
DataError::Http {
    status: None,
    url: url.to_owned(),
    message: format!("body read failed: {e}"),
}
```

**Validation:** `cargo check`. Grep for `"HTTP request failed"`, `"HTTP {"`, `"read body"` in `DataError::Socket` returns zero.

---

## Step 9: Migrate call sites — Category B (Exchange API Errors)

**12 sites across 6 files.**

Files:
- `exchange/binance/rest/mod.rs` — 1 site (HttpParser impl)
- `exchange/okx/rest/mod.rs` — 3 sites (HttpParser + 2 code checks)
- `exchange/bybit/rest/mod.rs` — 3 sites (HttpParser + 2 retCode checks)
- `exchange/kraken/rest/mod.rs` — 3 sites (HttpParser + 2 error checks)
- `exchange/coinbase/rest/mod.rs` — 1 site (HttpParser)
- `exchange/hyperliquid/rest/mod.rs` — 1 site (HttpParser)

Replace pattern:
```rust
// BEFORE (HttpParser::parse_api_error)
DataError::Socket(format!("OKX API error (code {code}): {msg}"))

// AFTER
DataError::ExchangeApi {
    exchange: "OKX".to_owned(),
    code: code.clone(),
    message: msg.clone(),
}

// BEFORE (inline retCode check)
DataError::Socket(format!("Bybit API error (code {}): {}", ret_code, ret_msg))

// AFTER
DataError::ExchangeApi {
    exchange: "Bybit".to_owned(),
    code: ret_code.to_string(),
    message: ret_msg.clone(),
}
```

**Validation:** `cargo check`. Grep for `"API error"` in `DataError::Socket` returns zero.

---

## Step 10: Migrate call sites — Category C + I (Parse Failures)

**49 + 13 = 62 sites. Largest batch — do exchange-by-exchange.**

### 10a. Bulk CSV/JSON parse errors (40 sites)

Files:
- `exchange/binance/bulk/trades.rs` — 4 sites
- `exchange/binance/bulk/klines.rs` — 9 sites
- `exchange/okx/bulk/trades.rs` — 6 sites
- `exchange/bybit/bulk/trades.rs` — 10 sites
- `exchange/kraken/bulk/trades.rs` — 5 sites
- `exchange/hyperliquid/bulk/trades.rs` — 6 sites

Replace pattern:
```rust
// BEFORE
DataError::Socket(format!("invalid trade timestamp: {ts}"))

// AFTER
DataError::Parse(format!("invalid trade timestamp: {ts}"))
```

Mechanical: change `DataError::Socket(` to `DataError::Parse(` for all parse-error messages. Keep format strings as-is.

### 10b. REST parse errors (9 sites)

Files:
- `exchange/kraken/rest/trades.rs` — 2 sites
- `exchange/kraken/rest/klines.rs` — 2 sites
- `exchange/kraken/rest/mod.rs` — 2 sites (timestamp nanos)
- `exchange/okx/rest/mod.rs` — 1 site (timestamp parse in pagination)
- `exchange/binance/bulk/mod.rs` — 2 sites (S3 XML parse)

Same mechanical replacement.

### 10c. Indirect `.map_err(DataError::Socket)` (13 sites)

Files: All 6 exchange `rest/mod.rs` files

Replace pattern:
```rust
// BEFORE
.map_err(DataError::Socket)?

// AFTER
.map_err(DataError::Parse)?
```

This works because `DataError::Parse` is `Parse(String)` which accepts `String` just like `Socket(String)` did.

**Validation:** `cargo check`. Grep for `DataError::Socket` across `bulk/trades.rs`, `bulk/klines.rs`, `rest/trades.rs`, `rest/klines.rs` returns zero.

---

## Step 11: Migrate Category H (Pagination Limit)

**1 site: `exchange/okx/rest/mod.rs`**

```rust
// BEFORE
DataError::Socket(format!(
    "OKX trades pagination exceeded max page limit ({MAX_TRADE_PAGES}) for {market}"
))

// AFTER
DataError::PaginationLimit {
    exchange: "OKX".to_owned(),
    market: market.to_owned(),
    limit: MAX_TRADE_PAGES,
}
```

---

## Step 12: Update `Display` impl for `DataError`

**File:** `barter-data/src/error.rs`

Add `#[error(...)]` attributes for the new variants (assuming `thiserror` is used; otherwise hand-write `Display`):

```rust
#[error("HTTP error (status={status:?}) for {url}: {message}")]
Http { status: Option<u16>, url: String, message: String },

#[error("{exchange} API error (code {code}): {message}")]
ExchangeApi { exchange: String, code: String, message: String },

#[error("parse error: {0}")]
Parse(String),

#[error("archive error: {0}")]
Archive(String),

#[error("checksum mismatch: expected {expected}, got {actual}")]
ChecksumMismatch { expected: String, actual: String },

#[error("{exchange} does not support interval: {interval}")]
UnsupportedInterval { exchange: String, interval: String },

#[error("I/O error: {0}")]
Io(String),

#[error("{exchange} pagination limit exceeded ({limit}) for {market}")]
PaginationLimit { exchange: String, market: String, limit: usize },
```

Check if `DataError` already derives `thiserror::Error`. If not, either add the derive or implement `Display` + `Error` manually for all variants.

---

## Step 13: Update tests

Search for tests that match on `DataError::Socket` or assert on error message strings:

```bash
grep -rn "DataError::Socket" barter-data/src/ --include="*.rs" | grep "#\[test\]\|assert"
```

Update each test to match on the new variant instead. For example:

```rust
// BEFORE
assert!(matches!(result, Err(DataError::Socket(msg)) if msg.contains("checksum")));

// AFTER
assert!(matches!(result, Err(DataError::ChecksumMismatch { .. })));
```

---

## Step 14: Final verification

14.1. `cargo test --workspace --all-features` — all tests pass.

14.2. `cargo clippy --workspace --all-features` — no new warnings.

14.3. Verify grep counts:
```bash
# Should return ONLY actual WebSocket/socket errors (streams layer)
grep -rn "DataError::Socket(" barter-data/src/ --include="*.rs" | wc -l
# Target: < 10 (only From<SocketError> conversion and WS error paths)

# Should return zero
grep -rn 'DataError::Socket(format!' barter-data/src/ --include="*.rs" | wc -l
# Target: 0
```

14.4. Verify `is_retriable_data_error` has no substring matching on `"500"`, `"429"`, `"timeout"`, etc. — all retry decisions are based on `DataError::Http { status, .. }` matching.

---

## Execution Order

| Step | Files touched | Sites migrated | Cumulative |
|------|---------------|----------------|------------|
| 1 | error.rs | 0 (new variants) | 0/125 |
| 2 | error.rs | 0 (From impl) | 0/125 |
| 3 | retry.rs | 0 (rewrite classifier) | 0/125 |
| 4 | 5 exchange mod.rs | 10 | 10/125 |
| 5 | checksum.rs, binance/bulk | 4 | 14/125 |
| 6 | 6 bulk files | 15 | 29/125 |
| 7 | kraken/bulk | 3 | 32/125 |
| 8 | 5 bulk files | 18 | 50/125 |
| 9 | 6 rest files | 12 | 62/125 |
| 10 | 12 files (bulk+rest) | 62 | 124/125 |
| 11 | okx/rest | 1 | 125/125 |
| 12 | error.rs | 0 (Display) | — |
| 13 | test files | 0 (test updates) | — |
| 14 | — | 0 (verification) | — |

Steps 4-11 can be done in any order. The recommended order above goes from smallest/simplest categories to largest, building confidence incrementally. Each step is independently compilable after steps 1-3 are done.
