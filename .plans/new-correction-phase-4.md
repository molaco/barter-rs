# Phase 4: Safety Fixes — Detailed Implementation Plan

## Prerequisites

- Phase 1 (Error Type Overhaul) complete — `DataError` variants (`Http`, `BulkArchive`, `Io`, `DataParse`, `UnsupportedInterval`) exist and are in use.
- Phase 2 (Retry & Observability) complete — `retry_with_backoff` has tracing, jitter, `saturating_mul`.
- `tracing` is a workspace dependency and already imported in all exchange modules.
- `zip` crate is a dependency (used by Binance, OKX, Kraken).
- `lz4_flex` is a dependency (used by Hyperliquid).
- `tokio` with `rt` feature available for `spawn_blocking`.

---

## 4.1 — Blocking I/O in Async: Wrap ZIP Decompression in `spawn_blocking`

### Problem

Synchronous ZIP/archive decompression runs on the async executor, blocking the Tokio runtime thread. Hyperliquid already wraps LZ4 in `spawn_blocking` — OKX, Kraken, and Binance do not.

### Affected Locations

| Exchange | File | Function | Lines | Format | Currently Safe? |
|----------|------|----------|-------|--------|-----------------|
| OKX | `exchange/okx/bulk/mod.rs` | `download_and_parse_trades` | 78–93 | ZIP | **NO** |
| OKX | `exchange/okx/bulk/mod.rs` | `download_monthly_trades` | 150–164 | ZIP | **NO** |
| Kraken | `exchange/kraken/bulk/mod.rs` | `parse_zip_trades` | 23–64 | ZIP | **NO** (sync fn called from async) |
| Binance | `exchange/binance/bulk/mod.rs` | `extract_zip_csv` | 256–276 | ZIP | **NO** (sync helper called from async) |
| Hyperliquid | `exchange/hyperliquid/bulk/mod.rs` | `download_and_decompress_hour` | 154–163 | LZ4 | YES (already correct) |

### Steps

**4.1.1 — Binance `extract_zip_csv`**

File: `barter-data/src/exchange/binance/bulk/mod.rs`

The sync helper `extract_zip_csv(zip_bytes: &[u8]) -> Result<Vec<u8>, DataError>` is called from two async functions: `download_and_parse_trades` (line 297) and `download_and_parse_klines` (line 323).

**Action:** At each call site, wrap with `spawn_blocking`. The helper takes `&[u8]`, so the bytes must be moved into an owned `Vec<u8>` for the `'static` closure:

```rust
// Before (line ~297):
let csv_bytes = extract_zip_csv(&zip_bytes)?;

// After:
let csv_bytes = tokio::task::spawn_blocking(move || extract_zip_csv(&zip_bytes))
    .await
    .map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")))??;
```

Apply at both call sites (trades ~297, klines ~323). The `zip_bytes` variable is already an owned `Vec<u8>`/`Bytes` at these points, so the `move` closure captures it by value. Verify `zip_bytes` is not used after the call (it isn't — both sites immediately parse the CSV result).

**4.1.2 — OKX daily `download_and_parse_trades`**

File: `barter-data/src/exchange/okx/bulk/mod.rs`, lines 78–93

The ZIP decompression block (create `Cursor`, open `ZipArchive`, `read_to_end`) is inline in the async method.

**Action:** Extract the ZIP block into a closure and wrap with `spawn_blocking`:

```rust
// Before (lines 80–93):
let cursor = std::io::Cursor::new(&bytes);
let mut archive = zip::ZipArchive::new(cursor)...;
// ...
archive.by_index(0)...read_to_end(&mut csv_data)...;

// After:
let csv_data = tokio::task::spawn_blocking(move || {
    let cursor = std::io::Cursor::new(bytes);  // move owned bytes
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP open failed: {e}")))?;
    if archive.len() == 0 {
        return Ok(Vec::new());
    }
    let mut csv_data = Vec::new();
    archive.by_index(0)
        .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP read failed: {e}")))?
        .read_to_end(&mut csv_data)
        .map_err(|e| DataError::BulkArchive(format!("OKX bulk ZIP extract failed: {e}")))?;
    Ok::<_, DataError>(csv_data)
})
.await
.map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")))??;
```

Note: `bytes` is `Bytes` from `resp.bytes().await` — it's `Send + 'static`. The empty-archive tracing warn for `market`/`date` must move outside the closure (log before or after, since `market`/`date` are `&str` and cannot enter the `'static` closure). Move the `archive.len() == 0` warning to after the `spawn_blocking` returns an empty vec.

**4.1.3 — OKX monthly `download_monthly_trades`**

File: `barter-data/src/exchange/okx/bulk/mod.rs`, lines 150–164

Identical pattern to 4.1.2. Same refactor: move ZIP block into `spawn_blocking`, handle empty-archive warning outside.

**4.1.4 — Kraken `parse_zip_trades`**

File: `barter-data/src/exchange/kraken/bulk/mod.rs`, lines 23–64

`parse_zip_trades` is already a sync function. It does `std::fs::File::open` + ZIP extraction. The caller is `stream_trades` (line 70–87) which builds a `Stream`.

**Caller assessment:** `stream_trades` has exactly **1 caller** in the entire codebase — the internal test `test_stream_trades_batching` (line 161–178, `#[tokio::test]`). It is not part of any trait and has no external consumers. Making it async is safe; the only test already runs in an async context and just needs `.await` added.

**Action:** Make `stream_trades` async and wrap the sync `parse_zip_trades` call in `spawn_blocking`:

```rust
pub async fn stream_trades(
    zip_path: &Path,
    pair: &str,
    batch_size: usize,
) -> impl Stream<Item = Result<Vec<RestTrade>, DataError>> + Send {
    let path = zip_path.to_path_buf();
    let pair = pair.to_string();
    let result = tokio::task::spawn_blocking(move || {
        Self::parse_zip_trades(&path, &pair)
    })
    .await
    .map_err(|e| DataError::BulkArchive(format!("ZIP task panicked: {e}")));

    match result {
        Ok(Ok(trades)) => {
            let batches: Vec<Result<Vec<RestTrade>, DataError>> = trades
                .chunks(batch_size.max(1))
                .map(|chunk| Ok(chunk.to_vec()))
                .collect();
            futures::stream::iter(batches)
        }
        Ok(Err(e)) => futures::stream::iter(vec![Err(e)]),
        Err(e) => futures::stream::iter(vec![Err(e)]),
    }
}
```

Update the test call site (line ~170) to add `.await`:
```rust
let batches: Vec<_> = KrakenArchiveParser::stream_trades(&path, "XBTUSD", 2)
    .await  // ← added
    .collect()
    .await;
```

### Send Requirements

- `zip::ZipArchive<std::io::Cursor<Vec<u8>>>` — `Cursor<Vec<u8>>` is `Send`, `ZipArchive` is `Send` when the reader is `Send`. Verified safe.
- `zip::ZipArchive<std::fs::File>` — `File` is `Send`. Verified safe.
- All `Vec<u8>` buffers are `Send`.

### Validation

- `cargo clippy --all-features` — no warnings about blocking in async.
- Confirm no `zip::ZipArchive::new` or `read_to_end` calls remain outside `spawn_blocking` in async contexts (grep for the pattern).
- Hyperliquid's LZ4 `spawn_blocking` (already correct) serves as the reference pattern.

---

## 4.2 — ZIP Size Limits: Check `file.size()` Before `read_to_end`

### Problem

All ZIP extraction sites call `read_to_end` without checking the uncompressed entry size. A malicious or corrupted archive could cause OOM. The LZ4 and gzip streaming paths also accumulate without bounds.

### Affected Locations

| Exchange | File | Lines | Format | Current Size Check |
|----------|------|-------|--------|-------------------|
| Binance | `exchange/binance/bulk/mod.rs` | 256–276 | ZIP | None |
| OKX (daily) | `exchange/okx/bulk/mod.rs` | 78–93 | ZIP | None |
| OKX (monthly) | `exchange/okx/bulk/mod.rs` | 150–164 | ZIP | None |
| Kraken | `exchange/kraken/bulk/mod.rs` | 55–61 | ZIP | None |
| Hyperliquid | `exchange/hyperliquid/bulk/mod.rs` | 154–160 | LZ4 | None |
| Bybit | `exchange/bybit/bulk/mod.rs` | 143–160 | Gzip | None |

### Steps

**4.2.1 — Define the constant**

Add to a shared location (e.g., `barter-data/src/exchange/mod.rs` or each bulk module locally):

```rust
/// Maximum allowed uncompressed archive entry size (2 GiB).
const MAX_DECOMPRESSED_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2 GiB
```

Prefer placing it in each bulk module locally (avoids cross-module coupling for a simple constant).

**4.2.2 — ZIP entries (Binance, OKX, Kraken)**

The `zip` crate's `ZipFile::size()` returns the uncompressed size as `u64`. Insert a check between `archive.by_index(N)` and `read_to_end`:

```rust
let file = archive.by_index(0)
    .map_err(|e| DataError::BulkArchive(format!("failed to read ZIP entry: {e}")))?;

if file.size() > MAX_DECOMPRESSED_SIZE {
    return Err(DataError::BulkArchive(format!(
        "ZIP entry too large: {} bytes (max {} bytes)",
        file.size(),
        MAX_DECOMPRESSED_SIZE,
    )));
}

let mut buf = Vec::with_capacity(file.size() as usize);  // pre-allocate
file.read_to_end(&mut buf)?;
```

Apply to:
- `binance/bulk/mod.rs` `extract_zip_csv` — before line 272
- `okx/bulk/mod.rs` `download_and_parse_trades` — before line 92
- `okx/bulk/mod.rs` `download_monthly_trades` — before line 163
- `kraken/bulk/mod.rs` `parse_zip_trades` — before line 60

Bonus: use `Vec::with_capacity(file.size() as usize)` to pre-allocate the buffer (avoid realloc on large files). Guard the cast: if `file.size() > usize::MAX as u64`, it's already over 2 GiB and will be rejected.

**4.2.3 — LZ4 stream (Hyperliquid)**

LZ4 `FrameDecoder` does not expose decompressed size upfront. Two options:

Option A (recommended): Use a size-limited reader wrapper:
```rust
let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed.as_slice());
let mut buf = Vec::new();
let read = std::io::Read::take(&mut decoder, MAX_DECOMPRESSED_SIZE + 1)
    .read_to_end(&mut buf)
    .map_err(|e| DataError::BulkArchive(format!("LZ4 decompression failed: {e}")))?;

if read as u64 > MAX_DECOMPRESSED_SIZE {
    return Err(DataError::BulkArchive(format!(
        "LZ4 decompressed data too large: >{} bytes",
        MAX_DECOMPRESSED_SIZE,
    )));
}
```

Option B: Track buffer size during read (less clean).

**4.2.4 — Gzip stream (Bybit)**

File: `barter-data/src/exchange/bybit/bulk/mod.rs`, lines 143–160

The Bybit bulk client uses async streaming gzip decompression (`GzipDecoder` + `lines()`), accumulating into `csv_buf`. Add a running size check inside the `while let Some(line)` loop:

```rust
let mut csv_buf = Vec::new();
while let Some(line) = lines.next_line().await.map_err(...)? {
    csv_buf.extend_from_slice(line.as_bytes());
    csv_buf.push(b'\n');
    if csv_buf.len() as u64 > MAX_DECOMPRESSED_SIZE {
        return Err(DataError::BulkArchive(format!(
            "Bybit decompressed data too large: >{} bytes",
            MAX_DECOMPRESSED_SIZE,
        )));
    }
}
```

### Validation

- Unit test: create a ZIP with a declared `size()` > 2 GiB and verify `BulkArchive` error is returned.
- For LZ4/gzip: test with a buffer that exceeds the limit.
- `cargo test --all-features` passes.

---

## 4.3 — Hyperliquid Panic on Unsupported Instrument

### Problem

`hyperliquid_market()` panics on unsupported `MarketDataInstrumentKind` variants (e.g., `Future`, `Option`). This crashes the process at subscription time.

### Location

File: `barter-data/src/exchange/hyperliquid/market.rs`, line 39

```rust
other => panic!("Hyperliquid does not support {other} instruments"),
```

### Call Chain

```
Subscription::new() → ExchangeSub::new() → Hyperliquid::resolve_market()
  → hyperliquid_market(base, quote, kind) → panic!
```

`Connector::resolve_market()` returns `Self::Market` (infallible). Changing it to `Result<Self::Market, DataError>` would cascade to all 9+ exchange implementations and `ExchangeSub::new()`.

### Steps

**4.3.1 — Choose approach: error propagation (recommended)**

The plan says "error propagation or sentinel." Error propagation is safer — it fails loudly at the right moment rather than deferring detection.

Change `hyperliquid_market` to return `Result<HyperliquidMarket, DataError>`:

```rust
pub(in crate::exchange::hyperliquid) fn hyperliquid_market(
    base: &AssetNameInternal,
    quote: &AssetNameInternal,
    kind: &MarketDataInstrumentKind,
) -> Result<HyperliquidMarket, DataError> {
    match kind {
        MarketDataInstrumentKind::Perpetual => {
            Ok(HyperliquidMarket(base.as_ref().to_uppercase_smolstr()))
        }
        MarketDataInstrumentKind::Spot => Ok(HyperliquidMarket(format_smolstr!(
            "{}/{}",
            base.as_ref().to_uppercase_smolstr(),
            quote.as_ref().to_uppercase_smolstr()
        ))),
        other => Err(DataError::UnsupportedInterval {
            exchange: "hyperliquid".into(),
            interval: format!("{other:?} instrument"),
        }),
    }
}
```

**Decision:** Add a new `DataError::UnsupportedInstrument` variant rather than reusing `UnsupportedInterval`. `DataError` is `#[non_exhaustive]` (confirmed in `barter-data/src/error.rs:8`), so adding a variant is non-breaking. Stuffing `"Future instrument"` into an `interval` field is semantically misleading and would confuse anyone matching on the error.

Add to `barter-data/src/error.rs` alongside the existing variants:

```rust
#[error("unsupported instrument kind '{instrument}' for exchange '{exchange}'")]
UnsupportedInstrument {
    exchange: String,
    instrument: String,
},
```

Then use it in the match arm:

```rust
other => Err(DataError::UnsupportedInstrument {
    exchange: "hyperliquid".into(),
    instrument: format!("{other:?}"),
}),
```

**4.3.2 — Update `Hyperliquid::resolve_market()`**

File: `barter-data/src/exchange/hyperliquid/mod.rs`, lines 168–177

Two options depending on whether we refactor the `Connector` trait:

**Option A — Minimal (no trait change):** Unwrap with a descriptive error at the trait boundary. This is still better than the raw panic because the error message is structured:

```rust
fn resolve_market(input: MarketInput<'_>, _sub_kind: &SubKind) -> Self::Market {
    match input {
        MarketInput::Components { base, quote, instrument_kind } => {
            hyperliquid_market(base, quote, instrument_kind)
                .expect("unsupported instrument kind for Hyperliquid")
        }
        MarketInput::ExchangeName(name) => HyperliquidMarket(name.name().clone()),
    }
}
```

This converts the panic from an unstructured `panic!()` to a structured `expect()` with a clear message. The `hyperliquid_market` function itself now returns `Result` and can be used safely by callers that handle errors.

**Option B — Full (trait change):** Change `Connector::resolve_market` to return `Result`. This is a larger refactor and may belong in Phase 5 (API Cleanup). Defer unless explicitly required.

**4.3.3 — Update tests**

File: `barter-data/src/exchange/hyperliquid/market.rs`, tests at lines 51–77

Update tests to call `.unwrap()` on the `Result` for valid cases. Add a test for the unsupported instrument case:

```rust
#[test]
fn test_unsupported_instrument_returns_error() {
    let result = hyperliquid_market(
        &AssetNameInternal::new("BTC"),
        &AssetNameInternal::new("USD"),
        &MarketDataInstrumentKind::Future(/* ... */),
    );
    assert!(result.is_err());
}
```

**4.3.4 — Audit other panics in Hyperliquid**

Additional panics found (for reference — only `market.rs:39` is in scope for 4.3):

| Location | Pattern | Severity | Action |
|----------|---------|----------|--------|
| `mod.rs:135,153` | `serde_json::to_value().expect()` | Low (infallible for known types) | No change |
| `channel.rs:32` | `.expect("validated")` | Low (exhaustive match covers all variants) | No change |
| `bulk/mod.rs:45` | `reqwest::Client::builder().build().expect()` | Low (extremely rare) | No change |
| `bulk/s3_signer.rs:99,100,160` | `.expect()` on internal URL/HMAC | Low (internal invariants) | No change |

### Validation

- `cargo test --all-features` passes.
- `grep -rn 'panic!' barter-data/src/exchange/hyperliquid/` returns zero hits in non-test code (only the macro definition and test assertions remain).

---

## 4.4 — Silent Data Degradation

Three sub-items addressing code that silently produces wrong data.

### 4.4a — `unwrap_or(0.0)` in Hyperliquid Candle

**Problem:** WebSocket candle conversion silently replaces unparseable prices with `0.0`. A price of `0.0` is semantically valid, making corruption undetectable downstream.

**File:** `barter-data/src/exchange/hyperliquid/candle.rs`, lines 123–127

```rust
let open: f64 = candle.open.parse().unwrap_or(0.0);
let high: f64 = candle.high.parse().unwrap_or(0.0);
let low: f64 = candle.low.parse().unwrap_or(0.0);
let close: f64 = candle.close.parse().unwrap_or(0.0);
let volume: f64 = candle.volume.parse().unwrap_or(0.0);
```

**Context:** The `From` impl returns `MarketIter<InstrumentKey, Candle>` which wraps `Vec<Result<MarketEvent, _>>`. The existing trade conversion in `trade.rs:113-114` correctly uses `.ok()?` inside `filter_map`. The REST kline conversion in `rest/klines.rs:54-77` correctly uses `.map_err()?`.

**Steps:**

**4.4a.1** — Refactor the `From` impl to use `filter_map` with `.ok()?`, matching the pattern from `trade.rs`:

```rust
impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, HyperliquidKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from(
        (exchange_id, instrument, kline): (ExchangeId, InstrumentKey, HyperliquidKline),
    ) -> Self {
        let candle = kline.candle;

        let result = (|| {
            let open: f64 = candle.open.parse().ok()?;
            let high: f64 = candle.high.parse().ok()?;
            let low: f64 = candle.low.parse().ok()?;
            let close: f64 = candle.close.parse().ok()?;
            let volume: f64 = candle.volume.parse().ok()?;

            let open_time = datetime_utc_from_epoch_duration(
                Duration::from_millis(candle.open_time),
            );
            let close_time = datetime_utc_from_epoch_duration(
                Duration::from_millis(candle.close_time),
            );

            Some(Ok(MarketEvent {
                time_exchange: close_time,
                time_received: Utc::now(),
                exchange: exchange_id,
                instrument,
                kind: Candle {
                    open_time,
                    close_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    quote_volume: None,
                    trade_count: candle.trade_count,
                    is_closed: true,
                },
            }))
        })();

        Self(result.into_iter().collect())
    }
}
```

If a parse fails, the candle is dropped entirely rather than emitted with `0.0` values. This matches the trade module's behavior.

**4.4a.2** — Update tests to verify that unparseable candles produce an empty `MarketIter`.

### 4.4b — `unwrap_or(1)` in Kraken

**Problem:** Three `unwrap_or(1)` calls silently degrade unsupported intervals to 1-minute.

**Locations:**

| File | Line | Context | Fix |
|------|------|---------|-----|
| `exchange/kraken/channel.rs` | 44 | `kraken_interval(self.kind.0).unwrap_or(1)` | See 4.4b.1 |
| `exchange/kraken/mod.rs` | 149 | `interval_str.parse().unwrap_or(1)` | See 4.4b.2 |
| `exchange/kraken/mod.rs` | 181 | `interval_str.parse().unwrap_or(1)` | See 4.4b.2 |

**4.4b.1 — `channel.rs:44` (interval degradation)**

This is the upstream source. `kraken_interval()` returns `Err` for unsupported intervals (D3, M3, H2, H6, H12, Month1). The current code silently maps them to `1`.

The `Identifier::id()` method returns `KrakenChannel` (infallible). Changing it to `Result` would require trait changes. Use `expect` with a clear message — callers should validate interval support before subscribing:

```rust
fn id(&self) -> KrakenChannel {
    KrakenChannel(format_smolstr!(
        "ohlc-{}",
        kraken_interval(self.kind.0)
            .expect("caller must validate interval is supported by Kraken")
    ))
}
```

The test `test_candles_channel_unsupported_falls_back_to_1` (line 113–121) should be updated to test that unsupported intervals panic (or deleted if panic-on-invalid is the desired behavior).

**4.4b.2 — `mod.rs:149` and `mod.rs:181` (string parse of channel interval)**

These parse the interval number from the channel name string (e.g., `"5"` from `"ohlc-5"`). The channel name is always constructed by the `Identifier` impl above, which guarantees the interval portion is a valid `u32`.

Replace with `expect` and invariant comment:

```rust
// Channel names are constructed by Identifier<KrakenChannel>::id(), which
// guarantees the interval portion is a valid u32.
let interval: u32 = interval_str.parse().expect("interval is a valid u32 from channel name");
```

Apply at both lines 149 and 181.

**4.4b.3** — Update or remove test `test_candles_channel_unsupported_falls_back_to_1`.

### 4.4c — `Interval::D3` Degrades to `"D"` in Bybit

**Problem:** `bybit_interval(Interval::D3)` returns `"D"` (daily) without any warning. Users requesting 3-day candles silently get daily candles.

**File:** `barter-data/src/exchange/bybit/mod.rs`, line 93

```rust
Interval::D3 => "D",  // silent degradation
```

**Context:** `tracing` is already imported and used extensively in the Bybit module. Kraken properly returns `Err(DataError::UnsupportedInterval)` for D3 — Bybit should at minimum warn.

**Steps:**

**4.4c.1** — Add `tracing::warn!` in `bybit_interval` for the D3 case:

```rust
Interval::D3 => {
    tracing::warn!(
        exchange = "bybit",
        requested = "D3",
        actual = "D",
        "Bybit does not support 3-day interval, falling back to daily"
    );
    "D"
}
```

**4.4c.2** — Consider whether this should be an error instead. The plan explicitly says `tracing::warn!`, so keep it as a warning. The `supported_intervals()` method (line 165–180) already excludes D3, so callers using that method won't hit this path. The warning catches callers that bypass the check.

### Validation

- `cargo test --all-features` passes.
- For 4.4a: test with unparseable price string → empty `MarketIter`.
- For 4.4b: test that supported intervals produce correct channels, unsupported intervals panic.
- For 4.4c: test that `bybit_interval(Interval::D3)` returns `"D"` (functional behavior unchanged, warning is observational).

---

## 4.5 — Doc/Code Mismatch: Coinbase Rate Limiter

### Problem

Documentation says 60 requests/second, code sets 10 requests/second.

### Location

File: `barter-data/src/exchange/coinbase/rest/mod.rs`

| Line | Content | Issue |
|------|---------|-------|
| 60 | `/// Includes a rate limiter configured for 60 requests per second.` | Wrong |
| 86 | `/// Initialises a rate limiter with a quota of 60 requests per second.` | Wrong |
| 114 | `Quota::per_second(NonZeroU32::new(10).unwrap()).allow_burst(NonZeroU32::new(10).unwrap())` | Correct |

### Steps

**4.5.1** — Update line 60: change `60` to `10`.

**4.5.2** — Update line 86: change `60` to `10`.

**4.5.3** — Verify no other rate limiter doc/code mismatches exist. Research confirmed all other exchanges match:

| Exchange | Doc | Code | Match |
|----------|-----|------|-------|
| Binance | 1200/min | 1200/min | Yes |
| Bybit | 10/sec | 10/sec | Yes |
| Kraken | 1/sec | 1/sec | Yes |
| OKX | 600/min | 600/min | Yes |
| Hyperliquid | 600/min | 600/min | Yes |
| **Coinbase** | **60/sec** | **10/sec** | **NO** |

### Validation

- `cargo doc --all-features` — verify doc comments are correct.
- Two-line change, no logic impact.

---

## 4.6 — `// N is non-zero` Comments on `NonZeroU32::new(N).unwrap()`

### Problem

Bare `.unwrap()` on `NonZeroU32::new(literal)` is safe (literal is provably non-zero) but looks like a potential panic site without a comment.

### All Locations (11 `.unwrap()` sites, all with literal values)

| # | File | Line | Value(s) |
|---|------|------|----------|
| 1 | `barter-collector/src/streams.rs` | 170 | `100` |
| 2 | `exchange/binance/rest/mod.rs` | 102 | `1200`, `20` |
| 3 | `exchange/binance/rest/mod.rs` | 140 | `1200`, `20` |
| 4 | `exchange/kraken/rest/mod.rs` | 94 | `1`, `1` |
| 5 | `exchange/kraken/rest/mod.rs` | 128 | `1`, `1` |
| 6 | `exchange/coinbase/rest/mod.rs` | 114 | `10`, `10` |
| 7 | `exchange/bybit/rest/mod.rs` | 123 | `10` |
| 8 | `exchange/bybit/rest/mod.rs` | 142 | `10` |
| 9 | `exchange/hyperliquid/rest/mod.rs` | 112 | `600` |
| 10 | `exchange/okx/rest/mod.rs` | 118 | `600`, `10` |

(3 additional sites use `.expect("message")` with variable values — `barter-data/src/lib.rs:134`, `subscriber/mod.rs:88` — those already have descriptive messages and are out of scope.)

### Steps

**4.6.1** — For each `.unwrap()` call, add an inline comment:

```rust
// Before:
let quota = Quota::per_second(NonZeroU32::new(10).unwrap());

// After:
let quota = Quota::per_second(NonZeroU32::new(10).unwrap()); // 10 is non-zero
```

For lines with two calls (e.g., `.allow_burst()`), add one comment covering the full expression:

```rust
// 1200 and 20 are non-zero
let quota = Quota::per_minute(NonZeroU32::new(1200).unwrap())
    .allow_burst(NonZeroU32::new(20).unwrap());
```

**4.6.2** — Apply across all 10 locations listed above.

### Validation

- Mechanical change, no logic impact.
- `cargo clippy --all-features` clean.

---

## 4.7 — OKX Kline `confirm` Field

### Problem

The REST kline conversion hardcodes `is_closed: true` despite having the `confirm` field deserialized. The WebSocket kline conversion correctly uses `fields[8] == "1"`.

### Locations

| Path | Context | Current | Correct |
|------|---------|---------|---------|
| `exchange/okx/rest/klines.rs:205` | REST `try_into_candle` | `is_closed: true` | `is_closed: self.confirm == "1"` |
| `exchange/okx/candle.rs:78` | WebSocket `From` impl | `is_closed: fields[8] == "1"` | Already correct |

### Steps

**4.7.1** — Change line 205 in `exchange/okx/rest/klines.rs`:

```rust
// Before:
is_closed: true,

// After:
is_closed: self.confirm == "1",
```

The `confirm` field is already deserialized (line 119 in the `Deserialize` impl) and stored in `OkxKlineRaw.confirm` (line 76). This is a one-line change.

**4.7.2** — Add a test for incomplete candle:

```rust
#[test]
fn test_okx_kline_incomplete_candle() {
    let raw = OkxKlineRaw {
        ts: "1672502400000".into(),
        open: "16850".into(),
        high: "16860".into(),
        low: "16845".into(),
        close: "16855.5".into(),
        volume: "12.345".into(),
        vol_ccy_quote: "208000".into(),
        confirm: "0".into(),  // incomplete
    };
    let candle = raw.try_into_candle(Interval::M1).unwrap();
    assert!(!candle.is_closed);
}

#[test]
fn test_okx_kline_confirmed_candle() {
    let raw = OkxKlineRaw {
        ts: "1672502400000".into(),
        // ... same fields ...
        confirm: "1".into(),  // confirmed
    };
    let candle = raw.try_into_candle(Interval::M1).unwrap();
    assert!(candle.is_closed);
}
```

### Validation

- `cargo test --all-features` passes.
- WebSocket test `test_okx_kline_incomplete_candle` (candle.rs:180–198) already verifies the WS path handles `confirm="0"`.

---

## 4.8 — Eliminate `std::sync::Mutex` in `download_bytes_resumable`

### Problem

Both Binance and Hyperliquid bulk download functions use `Arc<std::sync::Mutex<Vec<u8>>>` to accumulate bytes across retry attempts. The retry loop is sequential — there is never concurrent access. The Mutex adds unnecessary complexity, lock overhead, and poison-panic risk.

### Locations

| File | Function | Lines | Lock Sites |
|------|----------|-------|------------|
| `exchange/binance/bulk/mod.rs` | `download_bytes_resumable` | 146–234 | 7 lock calls |
| `exchange/hyperliquid/bulk/mod.rs` | `download_and_decompress_hour` | 67–169 | 7 lock calls |

Note: `streams/builder/mod.rs` also uses `Arc<Mutex<>>` (line 52) but that is legitimate concurrent access and must NOT be changed.

### Steps

**4.8.1 — Understand the constraint**

`retry_with_backoff` takes a closure that returns a `Future`. The closure is called multiple times (once per retry). The `partial` buffer must persist across retries. Currently this is done via `Arc<Mutex<>>` — the Arc is cloned into each closure invocation.

The key insight: `retry_with_backoff` is sequential. It `await`s each future before calling the closure again. So we need shared ownership (`Arc`) to satisfy the `'static` bound on the future, but NOT mutual exclusion (`Mutex`).

**4.8.2 — Refactor approach**

Replace `Arc<Mutex<Vec<u8>>>` with `Arc<std::cell::UnsafeCell<Vec<u8>>>` wrapped in a newtype — but this is unnecessarily unsafe.

Better approach: use `Arc<tokio::sync::Mutex<Vec<u8>>>` (async Mutex, no poison). But this still has a Mutex.

Simplest correct approach: restructure to avoid shared state entirely. Move the retry loop into a manual loop instead of using `retry_with_backoff`, keeping `partial` as a local `mut Vec<u8>`:

```rust
async fn download_bytes_resumable(
    client: &reqwest::Client,
    retry: &RetryPolicy,
    url: &str,
) -> Result<Option<Vec<u8>>, DataError> {
    let mut partial = Vec::new();
    let mut attempts = 0u32;

    loop {
        attempts += 1;
        let existing_len = partial.len();

        let mut request = client.get(url);
        if existing_len >= RANGE_RESUME_THRESHOLD {
            request = request.header("Range", format!("bytes={existing_len}-"));
            tracing::debug!(url = %url, resume_from = existing_len, "resuming download");
        }

        let response = match request.send().await {
            Ok(r) => r,
            Err(e) => {
                let err = DataError::Http {
                    status: None,
                    url: url.to_string(),
                    message: format!("request failed: {e}"),
                };
                if attempts > retry.max_retries && is_retriable_data_error(&err) {
                    return Err(err);
                }
                // backoff and continue (use retry utilities)
                continue;
            }
        };

        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if status == reqwest::StatusCode::RANGE_NOT_SATISFIABLE {
            partial.clear();
            // retry...
            continue;
        }
        if !status.is_success() && status != reqwest::StatusCode::PARTIAL_CONTENT {
            // handle error + retry...
            continue;
        }
        if status == reqwest::StatusCode::OK && existing_len > 0 {
            partial.clear();
        }

        let mut byte_stream = response.bytes_stream();
        let mut stream_err = None;
        while let Some(chunk_result) = byte_stream.next().await {
            match chunk_result {
                Ok(chunk) => partial.extend_from_slice(&chunk),
                Err(e) => {
                    stream_err = Some(e);
                    break;
                }
            }
        }

        if let Some(e) = stream_err {
            // retriable — partial buffer retained for Range resume
            if attempts > retry.max_retries {
                return Err(DataError::Http {
                    status: None,
                    url: url.to_string(),
                    message: format!("body read failed: {e}"),
                });
            }
            continue;
        }

        return Ok(Some(std::mem::take(&mut partial)));
    }
}
```

This eliminates `Arc`, `Mutex`, `.lock().unwrap()`, and poison risk entirely. The `partial` buffer is an ordinary `mut Vec<u8>` on the stack frame.

**4.8.3 — Apply to Binance**

File: `barter-data/src/exchange/binance/bulk/mod.rs`, lines 146–234

Rewrite `download_bytes_resumable` using the manual loop pattern above. Preserve:
- Range resume logic (threshold, headers)
- 404 → `Ok(None)`
- 416 → clear and retry
- 200 with existing data → clear (server doesn't support Range)
- Streaming body accumulation
- Retry backoff timing (reuse `apply_backoff` or manual `tokio::time::sleep`)
- Tracing debug logs

**4.8.4 — Apply to Hyperliquid**

File: `barter-data/src/exchange/hyperliquid/bulk/mod.rs`, lines 67–169

Same refactor. Additional considerations:
- S3 signing (`s3_signer::sign_s3_get`) must be called per attempt (credentials may rotate)
- LZ4 `spawn_blocking` decompression happens after download completes — keep that after the loop
- The `credentials` clone happens once outside the loop

**4.8.5 — Verify `retry_with_backoff` is not needed**

After refactoring, `download_bytes_resumable` no longer uses `retry_with_backoff`. Verify it is still used elsewhere (it is — REST fetch methods in the collector). No removal needed.

**4.8.6 — Remove `std::sync::Mutex` import**

After refactoring both files, remove unused `use std::sync::{Arc, Mutex}` from both bulk modules. Verify no other code in those files uses them.

### Validation

- `cargo test --all-features` passes.
- `cargo clippy --all-features` clean.
- `grep -rn 'std::sync::Mutex' barter-data/src/exchange/binance/bulk/` returns zero.
- `grep -rn 'std::sync::Mutex' barter-data/src/exchange/hyperliquid/bulk/` returns zero.
- `streams/builder/mod.rs` still correctly uses `Arc<Mutex<>>`.

---

## Implementation Order

```
4.5  (doc fix)                            ← trivial, do first
4.6  (unwrap comments)                    ← trivial, do second
4.7  (OKX confirm field)                  ← one-line fix + tests
4.4c (Bybit D3 warning)                   ← small, isolated
4.4b (Kraken unwrap_or(1))                ← small, 3 sites
4.4a (Hyperliquid candle 0.0)             ← moderate refactor of From impl
4.3  (Hyperliquid panic + new variant)    ← moderate, adds UnsupportedInstrument variant
4.2  (ZIP size limits)                    ← moderate, 6 sites
4.1  (OKX + Kraken spawn_blocking)        ← OKX (2 sites) + Kraken (1 site) only
4.1+4.8 combined (Binance bulk rewrite)   ← single pass: manual retry loop + spawn_blocking + size limit
4.1+4.8 combined (Hyperliquid bulk rewrite) ← single pass: manual retry loop + spawn_blocking already present + size limit
```

**Key change from v1:** Steps 4.1 and 4.8 are **combined** for Binance and Hyperliquid. Both steps touch `download_bytes_resumable` (Binance) and `download_and_decompress_hour` (Hyperliquid). Doing 4.1 (add `spawn_blocking`) and then 4.8 (rewrite to eliminate Mutex) would edit the same functions twice. Instead, rewrite each function once in a single pass that:
1. Replaces `Arc<Mutex<Vec<u8>>>` with owned `mut Vec<u8>` (4.8)
2. Replaces `retry_with_backoff` closure with a manual retry loop (4.8)
3. Wraps ZIP decompression in `spawn_blocking` where needed (4.1 — Binance only, Hyperliquid already has it)
4. Adds size limit checks (4.2 — folded in since we're rewriting anyway)

OKX and Kraken `spawn_blocking` changes (4.1.2, 4.1.3, 4.1.4) remain separate since those functions are not affected by 4.8.

---

## Final Validation Checklist

- [ ] `cargo build --all-features` — zero errors
- [ ] `cargo test --all-features` — all pass
- [ ] `cargo clippy --all-features` — zero warnings
- [ ] `cargo doc --all-features` — no broken links
- [ ] `grep -rn 'unwrap_or(0.0)' barter-data/src/exchange/hyperliquid/candle.rs` — zero hits
- [ ] `grep -rn 'unwrap_or(1)' barter-data/src/exchange/kraken/` — zero hits
- [ ] `grep -rn 'panic!' barter-data/src/exchange/hyperliquid/market.rs` — zero hits
- [ ] `grep -rn 'std::sync::Mutex' barter-data/src/exchange/binance/bulk/` — zero hits
- [ ] `grep -rn 'std::sync::Mutex' barter-data/src/exchange/hyperliquid/bulk/` — zero hits
- [ ] All `NonZeroU32::new(N).unwrap()` have `// N is non-zero` comments
- [ ] Coinbase rate limiter docs say 10, not 60
- [ ] OKX REST klines use `self.confirm == "1"` for `is_closed`
- [ ] No `read_to_end` without prior size check in ZIP/LZ4/gzip paths
- [ ] No `ZipArchive` operations outside `spawn_blocking` in async contexts
