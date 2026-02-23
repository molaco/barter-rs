# WebSocket Candles Streaming

## Overview

Add real-time WebSocket candle/kline streaming to barter-data, complementing the existing REST klines (historical backfill) with live updates. All 8 exchanges support WS candles.

## Core Design Problems & Solutions

### Problem 1: Candles needs Interval

`Candles` is currently a unit struct. Every exchange embeds the interval in the WS channel name (e.g., `btcusdt@kline_1m`). The `Identifier<Channel>` impl needs the interval.

**Solution:** `pub struct Candles;` → `pub struct Candles(pub Interval);`

- `SubscriptionKind::as_str()` returns `&'static str` — keep returning `"candles"`. The interval is not needed here; `as_str()` is only used for `Display` and `StreamKey` naming (`streams/consumer.rs:61`), neither of which need per-interval distinction.
- `Display` impl changes to `write!(f, "candles_{}", self.0)` — this does NOT go through `as_str()`, it formats directly, so no `&'static str` constraint.
- `Default` impl returns `Candles(Interval::M1)`.
- REST code unaffected — `KlineFetcher` uses `KlineRequest` with its own `interval` field.

### Problem 2: SubKind::Candles needs Interval

`SubKind::Candles` is a unit variant. In `DynamicStreams::init()`, subscriptions are grouped by `(exchange, sub_kind)`. When converting from dynamic `SubKind` to typed `Candles(interval)`, we need the interval.

**Solution:** `SubKind::Candles` → `SubKind::Candles(Interval)`

- `Interval` already derives `Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize` — all `SubKind` derives still work.
- Grouping by `(exchange, sub_kind)` naturally separates different intervals into different groups — correct behavior since different intervals are different WS channels.
- `derive_more::Display` on `SubKind` may need a manual `Display` impl for the `Candles(Interval)` variant (e.g., `"Candles(1m)"`).
- In the `init()` match arms, extract interval: `SubKind::Candles(interval) => { ... Candles(interval) ... }`.

### Problem 3: Channel types use `&'static str`

All channel types (e.g., `BinanceChannel(pub &'static str)`) hold static strings. Candle channels need dynamic strings like `format!("@kline_{}", interval)`.

**Solution:** Change all channel types from `&'static str` to `String`.

Affected types:
- `BinanceChannel(pub &'static str)` → `BinanceChannel(pub String)`
- `BybitChannel(pub &'static str)` → `BybitChannel(pub String)`
- `OkxChannel(pub &'static str)` → `OkxChannel(pub String)`
- `KrakenChannel(pub &'static str)` → `KrakenChannel(pub String)`
- `BitmexChannel(pub &'static str)` → `BitmexChannel(pub String)`
- `CoinbaseChannel(pub &'static str)` → `CoinbaseChannel(pub String)`
- `GateioChannel(pub &'static str)` → `GateioChannel(pub String)`
- `BitfinexChannel(pub &'static str)` → `BitfinexChannel(pub String)`

Impact:
- **Loses `Copy` derive** — only `Clone` needed. `Connector::Channel` bound is `AsRef<str>`, no `Copy` required. Remove `Copy` from derives.
- **`const` definitions become methods** — e.g., `pub const TRADES: Self = Self("@trade")` becomes `pub fn trades() -> Self { Self("@trade".into()) }`. Alternatively, keep the existing naming convention as associated functions.
- **`AsRef<str>` impl** changes from `self.0` to `self.0.as_str()`.
- **All existing `Identifier<Channel>` impls** update from returning constants to calling methods: `BinanceChannel::trades()` instead of `BinanceChannel::TRADES`.
- **No trait bound changes needed** — `Connector::Channel: AsRef<str>` is satisfied by `String`.

### Problem 4: Interval converters trapped behind `rest` feature

All interval converters (`binance_interval()`, etc.) live in `exchange/{name}/rest/klines.rs`, gated by `#[cfg(feature = "rest")]`. WS code needs them without the `rest` feature.

**Solution:** Move converters to `exchange/{name}/mod.rs` as `pub fn` (ungated).

- Add them after the existing `Connector` impl in each exchange's `mod.rs`.
- In `rest/klines.rs`, replace with `pub use super::binance_interval;` for backward compat.
- For exchanges without REST (BitMEX, GateIO, Bitfinex), add new converters directly to their `mod.rs`.

---

## Exchange WS Candle Specifications

### Binance (Spot + Futures)
- **Channel:** `<symbol>@kline_<interval>` (e.g., `btcusdt@kline_1m`)
- **Intervals:** 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w, 1M
- **Message format:**
  ```json
  {"e":"kline","E":1672515782136,"s":"BTCUSDT","k":{
    "t":1672515780000,"T":1672515839999,
    "s":"BTCUSDT","i":"1m",
    "o":"16850.00","c":"16855.50","h":"16860.00","l":"16845.00",
    "v":"12.345","q":"208000.00","n":150,
    "x":false
  }}
  ```
  - `t`/`T`: open/close time (ms), `x`: is closed, `n`: trade count, `q`: quote volume
- **SubscriptionId extraction:** `s` (symbol) + channel → `@kline_1m|BTCUSDT`

### Bybit (Spot + Perpetuals)
- **Channel:** `kline.<interval>.<symbol>` (e.g., `kline.1.BTCUSDT`)
- **Intervals:** 1, 3, 5, 15, 30, 60, 120, 240, 360, 720, D, W, M
- **Message format:**
  ```json
  {"topic":"kline.1.BTCUSDT","data":[{
    "start":1672502400000,"end":1672502459999,
    "interval":"1","open":"16850","close":"16855.5",
    "high":"16860","low":"16845",
    "volume":"12.345","turnover":"208000",
    "confirm":true,"timestamp":1672502458000
  }],"ts":1672502458000,"type":"snapshot"}
  ```
  - `confirm`: whether candle is closed, `turnover`: quote volume
- **SubscriptionId extraction:** `topic` field → `kline.1.BTCUSDT`

### OKX
- **Channel:** `candle<interval>` with `instId` (e.g., channel `candle1m`, instId `BTC-USDT`)
- **Intervals:** 1m, 3m, 5m, 15m, 30m, 1H, 2H, 4H, 6H, 12H, 1D, 3D, 1W, 1M (case-sensitive H/D)
- **Subscribe format:**
  ```json
  {"op":"subscribe","args":[{"channel":"candle1m","instId":"BTC-USDT"}]}
  ```
- **Message format:**
  ```json
  {"arg":{"channel":"candle1m","instId":"BTC-USDT"},"data":[
    ["1672502400000","16850","16860","16845","16855.5","12.345","208000","208000","1"]
  ]}
  ```
  - Array: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
  - `confirm`: "0" (incomplete) or "1" (complete)
- **SubscriptionId extraction:** `arg.channel` + `arg.instId` → `candle1m|BTC-USDT`

### Kraken
- **Channel:** `ohlc-<interval>` (e.g., `ohlc-1`)
- **Intervals:** 1, 5, 15, 30, 60, 240, 1440, 10080, 21600 (integer minutes)
- **Subscribe format:**
  ```json
  {"event":"subscribe","pair":["XBT/USD"],"subscription":{"name":"ohlc","interval":1}}
  ```
- **Message format:**
  ```json
  [42,["1672502400.000000","1672502459.000000","16850.0","16860.0","16845.0","16855.5","16852.5","12.345",150],"ohlc-1","XBT/USD"]
  ```
  - Array: [time, etime, open, high, low, close, vwap, volume, count]
  - Wrapped in outer array with channelID, channel name, pair
- **SubscriptionId extraction:** channel name + pair → `ohlc-1|XBT/USD`
- **Subscription quirk:** Kraken embeds interval in the subscription JSON body (`"interval":1`), not in the channel name. `KrakenChannel` holds `"ohlc-1"` for subscription ID matching, but `Connector::requests()` must read the interval from the channel string (parse the number after `ohlc-`) or carry it separately. Since `requests()` receives `ExchangeSub<KrakenChannel, KrakenMarket>`, and `KrakenChannel` now holds `String`, it contains `"ohlc-1"` — parseable to extract the interval integer for the JSON body.

### BitMEX
- **Channel:** `tradeBin1m`, `tradeBin5m`, `tradeBin1h`, `tradeBin1d`
- **Intervals:** 1m, 5m, 1h, 1d only (4 intervals)
- **Subscribe format:**
  ```json
  {"op":"subscribe","args":["tradeBin1m:XBTUSD"]}
  ```
- **Message format:**
  ```json
  {"table":"tradeBin1m","action":"insert","data":[{
    "timestamp":"2023-01-01T00:01:00.000Z","symbol":"XBTUSD",
    "open":16850,"close":16855.5,"high":16860,"low":16845,
    "trades":150,"volume":123450,"vwap":16852.5,
    "lastSize":5,"turnover":733000000,"homeNotional":12.345,"foreignNotional":208000
  }]}
  ```
  - `action`: "partial" (snapshot), "insert" (new candle), "update" (live update)
  - Prices are numbers not strings
- **SubscriptionId extraction:** `table` + `symbol` → `tradeBin1m:XBTUSD`

### Coinbase
- **Channel:** `candles` (hardcoded — no interval selection)
- **Intervals:** 5-minute only. Updates every second.
- **Subscribe format:**
  ```json
  {"type":"subscribe","product_ids":["ETH-USD"],"channel":"candles"}
  ```
- **Message format:**
  ```json
  {"channel":"candles","timestamp":"2023-06-09T20:19:35.396Z","sequence_num":0,
   "events":[{"type":"snapshot","candles":[{
     "start":"1688998200","high":"1867.72","low":"1865.63",
     "open":"1867.38","close":"1866.81","volume":"0.20269406",
     "product_id":"ETH-USD"
   }]}]}
  ```
  - `type`: "snapshot" or "update"
  - `start` is unix seconds as string
  - No quote_volume, no trade_count
- **SubscriptionId extraction:** `product_id` from candle → `candles|ETH-USD`
- **Limitation:** Only M5 interval. Subscription with any other interval errors at validation time in `exchange_supports_instrument_kind_sub_kind()` or in the `Identifier<Channel>` impl.

### GateIO
- **Channel:** `spot.candlesticks_<interval>` / `futures.candlesticks_<interval>`
- **Intervals:** 10s, 1m, 5m, 15m, 30m, 1h, 4h, 8h, 1d, 7d, 30d
- **Message format:**
  ```json
  {"time":1672502458,"channel":"spot.candlesticks_1m","event":"update",
   "result":{"t":"1672502400","v":"12.345","c":"16855.5","h":"16860","l":"16845","o":"16850","n":"1m_BTC_USDT","a":"208000"}}
  ```
  - `t`: timestamp (seconds), `a`: amount (quote volume), `n`: name identifier
- **SubscriptionId extraction:** `n` field → `1m_BTC_USDT`

### Bitfinex
- **Channel:** `candles` with key `trade:<interval>:<symbol>` (e.g., `trade:1m:tBTCUSD`)
- **Intervals:** 1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 1W, 14D, 1M
- **Subscribe format:**
  ```json
  {"event":"subscribe","channel":"candles","key":"trade:1m:tBTCUSD"}
  ```
- **Message format:**
  ```json
  [42,"hb"]           // heartbeat
  [42,[1672502400000,16850,16855.5,16860,16845,12.345]]  // snapshot/update
  ```
  - Array: [mts, open, close, high, low, volume]
  - Wrapped in outer array with channelID
- **SubscriptionId extraction:** channelID from subscription confirmation → mapped to key

---

## Implementation Steps

### Step 1: Widen channel types from `&'static str` to `String`

**Files:** All 8 `exchange/{name}/channel.rs` files.

Per file:
1. Change `pub struct ExchangeChannel(pub &'static str)` → `pub struct ExchangeChannel(pub String)`
2. Remove `Copy` from derives (keep `Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize`)
3. Convert `pub const TRADES: Self = Self("@trade")` → `pub fn trades() -> Self { Self("@trade".into()) }`
4. Update `AsRef<str>` impl: `self.0` → `self.0.as_str()`
5. Update all `Identifier<Channel>` impls: `BinanceChannel::TRADES` → `BinanceChannel::trades()`

This is a mechanical change across all exchanges. No behavior changes, no trait bound changes. Tests pass with only const→fn updates.

### Step 2: Update Candles type + SubKind + move interval converters

**File: `barter-data/src/subscription/candle.rs`**
- `pub struct Candles;` → `pub struct Candles(pub Interval);`
- Remove auto-derived `Default`, add manual: `impl Default for Candles { fn default() -> Self { Self(Interval::M1) } }`
- `SubscriptionKind::as_str()` keeps returning `"candles"` (static lifetime preserved)
- `Display` changes to: `write!(f, "candles_{}", self.0)` — bypasses `as_str()`, formats directly

**File: `barter-data/src/subscription/mod.rs`**
- `SubKind::Candles` → `SubKind::Candles(Interval)`
- Replace `derive_more::Display` on `SubKind` with manual `Display` impl (or add `#[display("Candles({_0})")]` attribute on the variant if derive_more supports it)
- Update `exchange_supports_instrument_kind_sub_kind()` — add match arms for each exchange + instrument kind + `Candles(_)` returning `true`

**Files per exchange** (move interval converters):
- Move `binance_interval()` from `exchange/binance/rest/klines.rs` → `exchange/binance/mod.rs`
- Move `bybit_interval()` from `exchange/bybit/rest/klines.rs` → `exchange/bybit/mod.rs`
- Move `okx_interval()` from `exchange/okx/rest/klines.rs` → `exchange/okx/mod.rs`
- Move `coinbase_interval()` from `exchange/coinbase/rest/klines.rs` → `exchange/coinbase/mod.rs`
- Move `kraken_interval()` from `exchange/kraken/rest/klines.rs` → `exchange/kraken/mod.rs`
- In each `rest/klines.rs`, replace with `pub use super::exchange_interval;`

**Add new interval converters** (exchanges without REST):
- `exchange/bitmex/mod.rs`: `pub fn bitmex_interval(interval: Interval) -> Result<&'static str, DataError>` — only "1m", "5m", "1h", "1d"
- `exchange/coinbase/mod.rs`: separate `pub fn coinbase_ws_interval(interval: Interval) -> Result<&'static str, DataError>` — only M5
- `exchange/gateio/mod.rs`: `pub fn gateio_interval(interval: Interval) -> Result<&'static str, DataError>` — "1m", "5m", "15m", "30m", "1h", "4h", "8h", "1d", "7d", "30d"
- `exchange/bitfinex/mod.rs`: `pub fn bitfinex_interval(interval: Interval) -> Result<&'static str, DataError>` — "1m", "5m", "15m", "30m", "1h", "3h", "6h", "12h", "1D", "1W", "14D", "1M"

### Step 3: Per-exchange WS candle message types (Binance first)

For each exchange, create `exchange/{name}/candle.rs` (like existing `trade.rs`):

**a) Raw candle message struct** — matches the exchange's exact WS JSON:
- Custom `Deserialize` where needed
- `Identifier<Option<SubscriptionId>>` impl to extract subscription ID

**b) `From<(ExchangeId, InstrumentKey, ExchangeKline)> for MarketIter<InstrumentKey, Candle>`** — conversion to normalized `Candle`

Start with Binance as reference, then replicate.

### Step 4: Channel `Identifier` impls for Candles

For each exchange, add `Identifier<Channel>` impl for `Subscription<Exchange, Instrument, Candles>` in their `channel.rs`:

- **Binance:** `BinanceChannel(format!("@kline_{}", binance_interval(self.kind.0)))` — now works because channel holds `String`
- **Bybit:** `BybitChannel(format!("kline.{}", bybit_interval(self.kind.0)))`
- **OKX:** `OkxChannel(format!("candle{}", okx_interval(self.kind.0)))`
- **Kraken:** `KrakenChannel(format!("ohlc-{}", kraken_interval(self.kind.0)?))` — the `requests()` method parses the interval integer back from this string to build `{"subscription":{"name":"ohlc","interval":N}}`
- **BitMEX:** `BitmexChannel(format!("tradeBin{}", bitmex_interval(self.kind.0)?))` — channel contains full name like `tradeBin1m`
- **Coinbase:** `CoinbaseChannel("candles".into())` — static channel, interval validated elsewhere
- **GateIO:** `GateioChannel(format!("candlesticks_{}", gateio_interval(self.kind.0)?))` — server prefix (`spot.`/`futures.`) added in `requests()`
- **Bitfinex:** `BitfinexChannel(format!("candles"))` — interval carried in subscription key, not channel name

### Step 5: StreamSelector implementations

For each exchange, implement `StreamSelector<Instrument, Candles>`:

Same pattern as existing `StreamSelector<Instrument, PublicTrades>`:
- `type SnapFetcher = NoInitialSnapshots;`
- `type Stream = ExchangeWsStream<..., StatelessTransformer<Self, Instrument::Key, Candles, ExchangeKline>>`

### Step 6: DynamicStreams integration

**File: `barter-data/src/streams/builder/dynamic/mod.rs`**

1. Add `candles` field to `DynamicStreams`, `Txs`, `Rxs`, and their `Default` impls
2. Add `SubKind::Candles(_)` arm in `Channels::try_from()` — creates candles tx/rx channel
3. Add match arms in `init()` for each `(ExchangeId::*, SubKind::Candles(interval))`:
   ```
   (ExchangeId::BinanceSpot, SubKind::Candles(interval)) => {
       init_market_stream(
           STREAM_RECONNECTION_POLICY,
           subs.into_iter()
               .map(|sub| Subscription::new(BinanceSpot::default(), sub.instrument, Candles(interval)))
               .collect(),
       ).await.map(|stream| tokio::spawn(stream.forward_to(txs.candles.get(&exchange).unwrap().clone())))
   }
   ```
4. Add `Identifier<Market>` bounds for candle subscriptions in the `init()` where clause (one per exchange, matching the existing pattern for trades)
5. Add convenience methods: `select_all_candles()` etc.

### Step 7: Testing

Per exchange:
- Unit tests for candle message deserialization with real WS JSON fixtures
- Unit tests for `Identifier<Channel>` impls (verify correct channel name construction per interval)
- Unit tests for `From<(ExchangeId, Key, ExchangeKline)> for MarketIter<Key, Candle>` conversion
- Unit tests for interval converters (especially error cases for unsupported intervals)

---

## Implementation Order

| Phase | Steps | Scope |
|-------|-------|-------|
| 1 | Step 1 | Widen channel types to String (mechanical, all exchanges) |
| 2 | Step 2 | Foundation: Candles(Interval), SubKind, move interval converters |
| 3 | Steps 3-5 (Binance only) | End-to-end for one exchange |
| 4 | Step 6 | DynamicStreams wiring (Binance only initially) |
| 5 | Step 7 (Binance) | Tests before expanding |
| 6 | Steps 3-5 (Bybit, OKX, Kraken) | Core exchanges |
| 7 | Steps 3-5 (BitMEX, Coinbase, GateIO, Bitfinex) | Remaining exchanges |
| 8 | Step 7 (all) | Full test coverage |

Phase 1 is a standalone refactor that can be validated independently (all existing tests must pass). Start Binance end-to-end in phases 3-5, then replicate.

---

## Exchange Support Matrix

| Exchange | Intervals | Quote Volume | Trade Count | Is Closed |
|----------|-----------|-------------|-------------|-----------|
| Binance | All 14 | Yes (`q`) | Yes (`n`) | Yes (`x`) |
| Bybit | 13 (no D3) | Yes (`turnover`) | No | Yes (`confirm`) |
| OKX | All 14 | Yes (`volCcyQuote`) | No | Yes (`confirm`) |
| Kraken | 8 (subset) | No | Yes (`count`) | No |
| BitMEX | 4 (1m,5m,1h,1d) | Yes (`foreignNotional`) | Yes (`trades`) | Via `action` |
| Coinbase | 1 (5m only) | No | No | Via `type` |
| GateIO | 10 (subset) | Yes (`a`) | No | No |
| Bitfinex | 12 (subset) | No | No | No |

## Resolved Questions

1. **Kraken subscription format** — `KrakenChannel` holds `String` like `"ohlc-1"`. The `Connector::requests()` method for Kraken currently builds custom JSON. For candles, it parses the interval integer from the channel string (e.g., `"ohlc-1"` → `1`) and embeds it in `{"subscription":{"name":"ohlc","interval":1}}`. This is slightly hacky but avoids changing the `ExchangeSub`/`Connector` trait to carry extra data.

2. **Bitfinex channelID mapping** — Follows the same pattern already used for trades. The existing Bitfinex subscription validator maps numeric channelIDs to subscription keys. Candles use channel `"candles"` with a key like `"trade:1m:tBTCUSD"` — the validator maps the returned channelID to this key.

3. **Coinbase single-interval** — Yes, expose it. Subscribe with `Candles(Interval::M5)` only. Validation rejects other intervals in `exchange_supports_instrument_kind_sub_kind()` or the `Identifier<CoinbaseChannel>` impl. Consistent with how REST handles unsupported intervals.

4. **Channel type `&'static str` limitation** — Solved by widening all channel types to `String` in Step 1. This is a prerequisite for all candle channel impls. No trait bound changes needed since `Connector::Channel: AsRef<str>` is satisfied by `String`.

5. **`SubscriptionKind::as_str()` returns `&'static str`** — No problem. Keep returning `"candles"`. The interval is handled by `Display` (which formats directly, not through `as_str()`) and by the channel `Identifier` impl. Only call sites: `Display::fmt()` and `StreamKey` construction — neither needs per-interval distinction.
