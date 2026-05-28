# Phase 2: Retry & Observability — Detailed Implementation Plan

## Context

**File under modification:** `barter-data/src/retry.rs` (254 lines, contains `RetryPolicy`, `retry_with_backoff`, `is_retriable_data_error`, and tests).

**Why before the split:** `retry.rs` is imported by all 6 exchange modules (14+ call sites across REST and bulk paths). Fixing it now avoids carrying bugs into `barter-collector`.

**Prerequisites:** Phase 1 (Error Type Overhaul) should be complete — `is_retriable_data_error` will match on structured `DataError` variants instead of string substrings. If Phase 1 is not yet complete, steps 2.3 and the `is_retriable_data_error` rewrite from Phase 1 can be deferred, but the rest of Phase 2 stands independently.

**Constraints (from /rusty):**
- Rusty §9: No blocking I/O in async — `tokio::time::sleep` is already correct
- Rusty §7: `unwrap`/`expect` only with invariant comments
- Arch Guide §8: Use `tracing` structured fields, not `println!`

---

## Step 2.1: Add tracing to `retry_with_backoff`

**File:** `barter-data/src/retry.rs:33-57`

**What:** Add `tracing::warn!` inside the retry loop so operators can observe retry behavior in production. Currently the function is completely silent — no logging on any retry attempt or final exhaustion.

**Changes:**

2.1.1. Add `use tracing::warn;` to imports (line 1-3).

2.1.2. Inside the `Err(err) if should_retry(&err)` arm (line 47-50), add a `warn!` **before** the sleep:

```rust
Err(err) if should_retry(&err) => {
    warn!(
        attempt = attempt + 1,
        max_retries = policy.max_retries,
        backoff_ms = backoff.as_millis() as u64,
        "retriable error, backing off"
    );
    sleep(backoff).await;
    backoff = ...;
}
```

Note: The loop variable must change from `_` to `attempt` to track the attempt number:
```rust
for attempt in 0..policy.max_retries {
```

2.1.3. The `E` type parameter is not guaranteed to implement `Display` or `Debug`, so we do **not** log the error itself inside `retry_with_backoff`. The caller is responsible for logging error details if needed (matching the existing codebase pattern where errors are logged at the call site). If the caller wants error details in retry logs, they can wrap `should_retry` to add logging.

**Rationale:** Following the established tracing pattern from `barter-data/src/streams/task.rs:165` which uses `warn!(%exchange, %err, backoff_ms, "reconnection failed, backing off")` — same level, similar structured fields.

**Validation:** Existing tests pass unchanged. Verify retry logs appear with `RUST_LOG=barter_data::retry=warn cargo test`.

---

## Step 2.2: Add jitter to backoff

**File:** `barter-data/src/retry.rs:49` and `barter-data/Cargo.toml`

**What:** Add randomized jitter to prevent thundering herd when multiple clients retry simultaneously against the same exchange endpoint. Currently all clients with the same `RetryPolicy` sleep identical durations.

**Changes:**

2.2.1. Add `rand` as an optional dependency in `barter-data/Cargo.toml`:

```toml
# In [dependencies] section:
rand = { workspace = true, optional = true }

# In [features] section:
rest = ["dep:rand"]
bulk = ["dep:zip", "dep:csv", ..., "dep:rand"]
```

`rand` is already a workspace dependency at version `0.9.0` (used by `barter-execution`). Adding it behind both `rest` and `bulk` features ensures jitter is available for all retry paths.

2.2.2. In `retry.rs`, conditionally import `rand` and apply jitter to the backoff duration (line 49):

```rust
#[cfg(any(feature = "rest", feature = "bulk"))]
fn apply_jitter(duration: Duration) -> Duration {
    use rand::Rng;
    let factor = rand::rng().random_range(0.75..=1.25);
    duration.mul_f64(factor)
}

#[cfg(not(any(feature = "rest", feature = "bulk")))]
fn apply_jitter(duration: Duration) -> Duration {
    duration
}
```

2.2.3. Replace line 49:
```rust
// Before:
backoff = (backoff * policy.multiplier).min(policy.max_backoff);

// After:
backoff = apply_jitter((backoff * policy.multiplier).min(policy.max_backoff));
```

**Design decision:** Jitter is applied **after** the multiply+cap, not before. This means the jittered value can exceed `max_backoff` by up to 25%, which is acceptable — the alternative (jitter then cap) would bias toward `max_backoff` once values get close to it, reducing jitter effectiveness.

**Validation:** Existing tests still pass (jitter doesn't affect correctness, only timing). The `test_retry_succeeds_after_failures` and `test_retry_exhausts_all_retries` tests use `Duration::from_millis(1)` backoffs so jitter has negligible effect on test duration.

---

## Step 2.3: Validate `RetryPolicy` in constructor

**File:** `barter-data/src/retry.rs:6-27`

**What:** Add a `new()` constructor with `debug_assert!` validation so invalid policies are caught during development. Currently `RetryPolicy` has all `pub` fields and no validation — a `multiplier: 0` would cause backoff to collapse to zero, and `initial_backoff > max_backoff` is nonsensical.

**Changes:**

2.3.1. Add a `new()` constructor:

```rust
impl RetryPolicy {
    /// Create a new `RetryPolicy`.
    ///
    /// # Panics (debug builds)
    ///
    /// Panics if `initial_backoff > max_backoff` or `multiplier < 1`.
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

2.3.2. Do **not** make the fields `pub(crate)` or private in this phase — that is Phase 5 (API Cleanup, step 5.2). The `new()` constructor is additive; existing code using struct literals continues to work.

2.3.3. Add unit tests for the validation:

```rust
#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "initial_backoff")]
fn test_retry_policy_invalid_backoff_range() {
    RetryPolicy::new(
        Duration::from_secs(60),
        Duration::from_secs(1),
        2,
        3,
    );
}

#[test]
#[cfg(debug_assertions)]
#[should_panic(expected = "multiplier")]
fn test_retry_policy_zero_multiplier() {
    RetryPolicy::new(
        Duration::from_millis(100),
        Duration::from_secs(30),
        0,
        3,
    );
}
```

**Rationale (Rusty §7):** `debug_assert!` is appropriate here — these are invariants that should be caught in development but aren't worth panicking in production over (a multiplier of 0 would just mean no backoff increase, not undefined behavior).

**Validation:** `cargo test --all-features`. The `#[should_panic]` tests verify validation fires in debug builds.

---

## Step 2.4: Use `saturating_mul` to prevent overflow

**File:** `barter-data/src/retry.rs:49`

**What:** Replace `backoff * policy.multiplier` with overflow-safe arithmetic. `Duration * u32` panics on overflow in debug builds and wraps in release builds. With default values (100ms, 2x, 3 retries) this is safe, but with user-configurable policies (e.g., `multiplier: 100`, `max_retries: 50`) it can overflow.

**Changes:**

2.4.1. Replace the backoff calculation on line 49:

```rust
// Before:
backoff = apply_jitter((backoff * policy.multiplier).min(policy.max_backoff));

// After:
backoff = apply_jitter(
    backoff
        .saturating_mul(policy.multiplier)
        .min(policy.max_backoff)
);
```

`Duration::saturating_mul` (stable since Rust 1.53) returns `Duration::MAX` on overflow instead of panicking, and `.min(policy.max_backoff)` immediately caps it. This is the correct two-step: saturate then cap.

2.4.2. Add a unit test proving overflow safety:

```rust
#[tokio::test]
async fn test_retry_backoff_overflow_safety() {
    let policy = RetryPolicy {
        initial_backoff: Duration::from_secs(u64::MAX / 2),
        max_backoff: Duration::from_secs(30),
        multiplier: 100,
        max_retries: 1,
    };

    let result: Result<(), &str> = retry_with_backoff(
        &policy,
        |_: &&str| true,
        || async { Err("fail") },
    )
    .await;

    assert_eq!(result, Err("fail"));
    // Key: did not panic from overflow
}
```

**Note on related overflow in `streams/reconnect/stream.rs:222`:** That file uses a separate `ReconnectionBackoffPolicy` for WebSocket reconnections, not `RetryPolicy`. Fixing it is out of scope for Phase 2 (it is WebSocket infrastructure, not REST retry). If desired, file an issue or address in Phase 4 (Safety Fixes).

**Validation:** `cargo test --all-features`. The new test confirms no panic on extreme values.

---

## Step 2.5: Comment the HTTP 418 match

**File:** `barter-data/src/retry.rs:69`

**What:** Add a comment explaining why HTTP 418 ("I'm a Teapot") is treated as retriable. This is a Binance-specific behavior where 418 means "IP has been auto-banned for exceeding rate limits" — it's not the joke RFC 2324 status. Without context, this looks like a bug.

**Changes:**

2.5.1. Add an inline comment on line 69:

```rust
// Before:
|| lower.contains("418")

// After:
|| lower.contains("418") // Binance uses 418 for IP auto-ban on rate limit violation
```

2.5.2. If Phase 1 is complete and `is_retriable_data_error` now matches on structured `DataError` variants (e.g., `DataError::HttpApi { status, .. }`), this comment should instead be on the status code match:

```rust
DataError::HttpApi { status, .. } => matches!(
    status,
    429 | 418 // Binance uses 418 for IP auto-ban on rate limit violation
    | 500 | 502 | 503 | 504
),
```

**Validation:** No behavioral change — comment only. Existing tests pass unchanged.

---

## Implementation Order

```
2.4  saturating_mul   ← smallest diff, fixes a real bug, no new deps
2.5  HTTP 418 comment ← zero-risk, comment only
2.3  RetryPolicy::new ← additive constructor, no breaking changes
2.1  tracing::warn    ← adds logging, changes loop variable from _ to attempt
2.2  jitter           ← adds rand dep, feature-gated, most invasive
```

Steps 2.4 and 2.5 can be done first as they are zero-risk. Steps 2.1 and 2.3 are independent. Step 2.2 depends on Cargo.toml changes and should go last.

---

## Files Modified

| File | Change |
|------|--------|
| `barter-data/src/retry.rs` | All 5 steps: logging, jitter, constructor, saturating_mul, comment |
| `barter-data/Cargo.toml` | Step 2.2: add `rand` optional dep, update `rest`/`bulk` features |

No other files are modified. All 14+ call sites of `retry_with_backoff` remain unchanged — the function signature is identical.

---

## What Does NOT Change

- `retry_with_backoff` function signature — fully backward compatible
- `RetryPolicy` struct fields and `Default` impl — additive constructor only
- `is_retriable_data_error` logic (Phase 1 rewrites it; Phase 2 only adds a comment)
- Any exchange-specific code (REST clients, bulk downloaders)
- WebSocket reconnection backoff (`ReconnectionBackoffPolicy` in `streams/`)
- Rate limiter configuration or interaction patterns

---

## Validation Checklist

```
[ ] cargo check --all-features
[ ] cargo test --all-features
[ ] cargo clippy --all-features -- -D warnings
[ ] RUST_LOG=barter_data::retry=warn cargo test -- test_retry_succeeds_after_failures
      → verify warn! log lines appear with attempt, max_retries, backoff_ms fields
[ ] cargo test -- test_retry_backoff_overflow_safety
      → verify no panic on extreme Duration values
[ ] cargo test -- test_retry_policy_invalid_backoff_range (debug build)
      → verify debug_assert fires
[ ] cargo test -- test_retry_policy_zero_multiplier (debug build)
      → verify debug_assert fires
```
