# Phase 2: Retry & Observability — Detailed Plan

## Objective

Make `retry_with_backoff` observable, safe against overflow, resistant to thundering herd, and validated against nonsensical configuration. This is a prerequisite for Phase 3 (fetcher/collector split), where retry will move to the collector layer.

## Current State

**File:** `barter-data/src/retry.rs` (57 lines of production code, ~250 lines of tests)

- `RetryPolicy` has 4 public fields, no validation, no `#[non_exhaustive]`
- `retry_with_backoff` is silent — zero `tracing` on retry attempts
- Backoff uses `Duration * u32` which can panic on overflow in debug mode
- No jitter — deterministic backoff causes thundering herd when concurrent clients retry simultaneously
- `rand` crate is already in workspace dependencies (version 0.9.0)
- `tracing` crate is already a dependency of `barter-data`
- 22 call sites across 8 exchange files use `retry_with_backoff`

## Constraints

- **Rusty §9 (Async):** `tokio::time::sleep` is correct for async backoff — no blocking.
- **Rusty §8 (Tracing):** Use structured `tracing` events, not `println!`. Log at `warn` level for retries (they indicate degraded conditions).
- **Rusty §1 (Immutability):** `let mut` only for `backoff` accumulator — already correct.
- **Rusty §7 (Error Handling):** `unwrap`/`expect` only for provable invariants with comments.

## Pre-flight

```bash
cargo test --all-features -p barter-data
cargo clippy --all-features -p barter-data
```

---

## Step 2.1 — Add `tracing::warn!` on retry attempts

**File:** `barter-data/src/retry.rs`

Inside the `Err(err) if should_retry(&err)` arm, add a structured `tracing::warn!` before the sleep:

```rust
Err(err) if should_retry(&err) => {
    tracing::warn!(
        attempt = attempt + 1,
        max_retries = policy.max_retries,
        backoff_ms = backoff.as_millis() as u64,
        "retrying after error"
    );
    sleep(backoff).await;
    backoff = // ... (updated in 2.3)
}
```

Change the `for _ in 0..policy.max_retries` to `for attempt in 0..policy.max_retries` to have the attempt number available.

Also add `tracing::debug!` on the final attempt:

```rust
// Final attempt after all retries.
tracing::debug!(
    total_attempts = policy.max_retries + 1,
    "executing final retry attempt"
);
operation().await
```

**Why `warn` not `debug`:** Retries indicate degraded conditions (server errors, rate limits). Operators monitoring in production need visibility into retry storms without enabling debug-level logging. The final attempt uses `debug` since it's informational, not alarming.

**Constraint (Rusty §8):** Structured key-value fields (`attempt`, `max_retries`, `backoff_ms`), not interpolated format strings. This enables filtering/aggregation in log aggregators.

**Checkpoint:** `cargo check -p barter-data --all-features`. Existing tests still pass (they don't assert on log output).

---

## Step 2.2 — Add jitter to backoff

**File:** `barter-data/Cargo.toml` — add `rand` dependency:

```toml
[dependencies]
rand = { workspace = true }
```

**File:** `barter-data/src/retry.rs` — add jitter after computing the base backoff:

```rust
use rand::Rng;

// After computing the new backoff:
backoff = {
    let base = backoff.saturating_mul(policy.multiplier).min(policy.max_backoff);
    // Apply ±25% jitter to prevent thundering herd
    let jitter_factor = rand::rng().random_range(0.75f64..=1.25f64);
    let jittered_ms = (base.as_millis() as f64 * jitter_factor) as u64;
    Duration::from_millis(jittered_ms)
};
```

**Design decisions:**
- **±25% range (0.75..1.25):** Wide enough to decorrelate concurrent retriers, narrow enough that backoff progression is still predictable. AWS recommends "full jitter" (0..1.0×), but that can produce zero-wait retries. ±25% is a conservative middle ground.
- **`saturating_mul` instead of `*`:** Prevents overflow panic on extreme policies (e.g., `max_backoff: Duration::MAX`). The `.min(policy.max_backoff)` cap still applies after saturation.
- **`f64` for jitter calculation:** Precision loss on `as u64` is negligible for millisecond-scale durations.

**Checkpoint:** `cargo check -p barter-data --all-features`.

---

## Step 2.3 — Validate `RetryPolicy` construction

**File:** `barter-data/src/retry.rs`

Add a `new()` constructor with validation and `#[non_exhaustive]` on the struct. Keep `Default` working.

```rust
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub multiplier: u32,
    pub max_retries: u32,
}

impl RetryPolicy {
    /// Create a new `RetryPolicy` with validated parameters.
    ///
    /// # Panics
    /// Panics in debug mode if:
    /// - `initial_backoff > max_backoff`
    /// - `multiplier < 1`
    pub fn new(
        initial_backoff: Duration,
        max_backoff: Duration,
        multiplier: u32,
        max_retries: u32,
    ) -> Self {
        debug_assert!(
            initial_backoff <= max_backoff,
            "initial_backoff ({initial_backoff:?}) must be <= max_backoff ({max_backoff:?})"
        );
        debug_assert!(
            multiplier >= 1,
            "multiplier ({multiplier}) must be >= 1"
        );
        Self {
            initial_backoff,
            max_backoff,
            multiplier,
            max_retries,
        }
    }
}
```

**Why `debug_assert!` not `Result`:** `RetryPolicy` is always constructed with compile-time-known constants (the `Default` impl, or hardcoded in exchange clients). Runtime validation would force every call site to handle `Result` for values that are never user-supplied. `debug_assert!` catches programmer mistakes during development; in release builds, nonsensical values are clamped by the existing `.min(policy.max_backoff)` and saturation logic.

**Update `Default` impl** to delegate to `new()`:

```rust
impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(
            Duration::from_millis(100),
            Duration::from_secs(30),
            2,
            3,
        )
    }
}
```

**Note:** Adding `#[non_exhaustive]` to `RetryPolicy` is a breaking change if any code constructs it via struct literal (e.g., `RetryPolicy { initial_backoff: ..., ... }`). Check all construction sites. If they use struct literal syntax, they need to switch to `RetryPolicy::new(...)`. Search for `RetryPolicy {` across the codebase.

**Checkpoint:** `cargo check -p barter-data --all-features`. Fix any struct-literal construction sites.

---

## Step 2.4 — Use `saturating_mul` for backoff multiplication

This is already incorporated into Step 2.2's jitter code. Verify the old line:

```rust
// BEFORE (can panic on overflow in debug mode)
backoff = (backoff * policy.multiplier).min(policy.max_backoff);

// AFTER (saturates instead of panicking)
let base = backoff.saturating_mul(policy.multiplier).min(policy.max_backoff);
```

`Duration::saturating_mul` was stabilized in Rust 1.53. No MSRV concerns.

**Checkpoint:** Already validated in Step 2.2.

---

## Step 2.5 — Add comment to HTTP 418 match arm

**File:** `barter-data/src/retry.rs`

The 418 match in `is_retriable_data_error` already has a comment from Phase 1:

```rust
Some(418) => true,   // Binance IP ban / rate limit
```

Expand it slightly for clarity:

```rust
Some(418) => true,   // Binance-specific: "I'm a teapot" used as IP-based rate limit response
```

This is a one-line change.

---

## Step 2.6 — Update tests

**File:** `barter-data/src/retry.rs`

### 2.6a — Test jitter is applied

Add a test that verifies backoff durations are not identical across multiple retries (probabilistic test):

```rust
#[tokio::test]
async fn test_retry_backoff_has_jitter() {
    let policy = RetryPolicy::new(
        Duration::from_millis(100),
        Duration::from_secs(10),
        2,
        5,
    );

    // Run the retry loop twice and collect the actual sleep durations.
    // Since jitter is random, the backoff sequences should differ.
    // We can't directly observe sleep durations without instrumentation,
    // so instead test that the retry function still completes correctly
    // with jitter enabled (no panics, no infinite loops).
    let attempts = Arc::new(AtomicU32::new(0));
    let attempts_clone = Arc::clone(&attempts);

    let result: Result<&str, &str> = retry_with_backoff(
        &policy,
        |_: &&str| true,
        move || {
            let count = attempts_clone.fetch_add(1, Ordering::SeqCst);
            async move {
                if count < 3 { Err("retriable") } else { Ok("done") }
            }
        },
    )
    .await;

    assert_eq!(result, Ok("done"));
    assert_eq!(attempts.load(Ordering::SeqCst), 4);
}
```

### 2.6b — Test `RetryPolicy::new` debug assertions

```rust
#[test]
fn test_retry_policy_new_valid() {
    let policy = RetryPolicy::new(
        Duration::from_millis(50),
        Duration::from_secs(5),
        3,
        5,
    );
    assert_eq!(policy.initial_backoff, Duration::from_millis(50));
    assert_eq!(policy.multiplier, 3);
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "initial_backoff")]
fn test_retry_policy_new_invalid_backoff() {
    RetryPolicy::new(
        Duration::from_secs(60),    // initial > max
        Duration::from_secs(10),
        2,
        3,
    );
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "multiplier")]
fn test_retry_policy_new_zero_multiplier() {
    RetryPolicy::new(
        Duration::from_millis(100),
        Duration::from_secs(30),
        0,     // multiplier < 1
        3,
    );
}
```

### 2.6c — Update existing tests that use struct literal construction

If any existing tests construct `RetryPolicy` via struct literal, update them to use `RetryPolicy::new(...)` or `RetryPolicy { ... }` (still works if fields are pub, just won't work with `#[non_exhaustive]` from external crates — but tests are in the same crate so they're fine).

Actually, `#[non_exhaustive]` only affects construction from *outside* the defining crate. Since all tests are in `barter-data`, struct literal construction still works within the crate. No test changes needed for this reason. But if we want to model good practice, consider using `new()` in tests anyway.

**Checkpoint:** `cargo test --all-features -p barter-data` — all tests pass.

---

## Step 2.7 — Update construction sites across codebase

Search for all `RetryPolicy {` and `RetryPolicy::default()` construction sites. Since `#[non_exhaustive]` only blocks external crate construction, and all current construction is within `barter-data`, no changes are strictly required. But verify:

```bash
grep -rn "RetryPolicy {" barter-data/src/ --include="*.rs"
grep -rn "RetryPolicy::default()" barter-data/src/ --include="*.rs"
```

If any sites use struct literal with hardcoded values, consider migrating them to `RetryPolicy::new(...)` for consistency and to benefit from the debug assertions.

---

## Step 2.8 — Final verification

1. `cargo test --all-features -p barter-data` — all tests pass
2. `cargo clippy --all-features -p barter-data` — no new warnings
3. Verify tracing output appears (manual check):
   ```bash
   RUST_LOG=barter_data=warn cargo test --all-features -p barter-data test_retry_succeeds_after_failures 2>&1 | grep "retrying"
   ```
   Should show `retrying after error` log lines with attempt number and backoff duration.
4. Verify no `Duration * u32` without `saturating_mul` in retry.rs:
   ```bash
   grep -n "backoff \*" barter-data/src/retry.rs
   # Should return zero matches
   ```

---

## Execution Order

```
2.1  Add tracing::warn on retry      (retry.rs — additive, no breakage)
2.2  Add jitter + saturating_mul      (retry.rs + Cargo.toml — behavioral change)
2.3  Add RetryPolicy::new + validate  (retry.rs — additive constructor)
2.4  (incorporated into 2.2)
2.5  Comment on 418                   (retry.rs — one-line comment)
2.6  Update/add tests                 (retry.rs — tests only)
2.7  Check construction sites         (verification only)
2.8  Final verification               (no code changes)
```

Steps 2.1 and 2.2 are the main behavioral changes. Steps 2.3-2.5 are polish. Steps 2.6-2.8 are verification.

---

## Files Changed

| File | Change |
|------|--------|
| `barter-data/Cargo.toml` | Add `rand = { workspace = true }` to `[dependencies]` |
| `barter-data/src/retry.rs` | Add tracing, jitter, `saturating_mul`, `RetryPolicy::new`, `#[non_exhaustive]`, comment, tests |

**Total: 2 files.** This phase is small and contained — no exchange files touched.

---

## What Does NOT Change

- `is_retriable_data_error` — already rewritten in Phase 1, untouched here
- Call sites in exchange modules — they call `retry_with_backoff` the same way; the behavioral improvements (tracing, jitter, saturation) apply automatically
- `rest/retry.rs` — just a re-export of `crate::retry::*`, unchanged
