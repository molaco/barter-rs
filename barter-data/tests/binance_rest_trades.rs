#![cfg(feature = "rest")]

use barter_data::{
    exchange::binance::{
        futures::BinanceServerFuturesUsd, rest::BinanceRestClient, spot::BinanceServerSpot,
    },
    rest::{TradeFetcher, TradeRequest},
};
use chrono::DateTime;
use futures::StreamExt;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

/// Helper: start a mock server and create a `BinanceRestClient<BinanceServerSpot>`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, BinanceRestClient<BinanceServerSpot>) {
    let mock_server = MockServer::start().await;
    let client = BinanceRestClient::<BinanceServerSpot>::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Fixture: 3 realistic Binance aggTrades JSON objects, oldest-first.
///
/// Binance aggTrades use terse single-letter keys:
///   a = aggTradeId, p = price, q = quantity, f = firstTradeId,
///   l = lastTradeId, T = timestamp (ms), m = buyer_is_maker, M = best match
fn three_trades_json() -> serde_json::Value {
    json!([
        {
            "a": 26129,
            "p": "29000.00",
            "q": "0.50000000",
            "f": 27781,
            "l": 27781,
            "T": 1609459200000_i64,
            "m": true,
            "M": true
        },
        {
            "a": 26130,
            "p": "29100.50",
            "q": "1.20000000",
            "f": 27782,
            "l": 27782,
            "T": 1609459260000_i64,
            "m": false,
            "M": true
        },
        {
            "a": 26131,
            "p": "29200.00",
            "q": "0.75000000",
            "f": 27783,
            "l": 27783,
            "T": 1609459320000_i64,
            "m": true,
            "M": true
        }
    ])
}

// ---------------------------------------------------------------------------
// Test 1: fetch_trades returns a single batch of 3 trades
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_single_batch() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(three_trades_json()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTCUSDT".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    assert_eq!(trades.len(), 3);

    // First trade
    assert_eq!(trades[0].id, "26129");
    assert_eq!(
        trades[0].time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert!((trades[0].price - 29000.0).abs() < 1e-6);
    assert!((trades[0].amount - 0.5).abs() < 1e-6);
    assert_eq!(trades[0].side, barter_instrument::Side::Sell); // buyer_is_maker=true -> Sell

    // Second trade
    assert_eq!(trades[1].id, "26130");
    assert_eq!(
        trades[1].time,
        DateTime::from_timestamp_millis(1609459260000).unwrap()
    );
    assert!((trades[1].price - 29100.50).abs() < 1e-6);
    assert!((trades[1].amount - 1.2).abs() < 1e-6);
    assert_eq!(trades[1].side, barter_instrument::Side::Buy); // buyer_is_maker=false -> Buy

    // Third trade
    assert_eq!(trades[2].id, "26131");
    assert_eq!(
        trades[2].time,
        DateTime::from_timestamp_millis(1609459320000).unwrap()
    );
    assert!((trades[2].price - 29200.0).abs() < 1e-6);
    assert!((trades[2].amount - 0.75).abs() < 1e-6);
    assert_eq!(trades[2].side, barter_instrument::Side::Sell);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_trades with an empty response returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_empty_response() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTCUSDT".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();
    assert!(trades.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_trades propagates a Binance API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"code": -1121, "msg": "Invalid symbol."})),
        )
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "INVALID".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let result = client.fetch_trades(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("-1121"),
        "error should contain Binance error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Invalid symbol"),
        "error should contain Binance error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: stream_trades paginates through multiple batches using ID cursor
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_trades_pagination() {
    let (mock_server, client) = setup().await;

    // First batch (startTime = 1609459200000): return 2 trades.
    // Last agg_trade_id = 26130, so next fromId = 26131
    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .and(query_param("startTime", "1609459200000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "a": 26129,
                "p": "29000.00",
                "q": "0.50000000",
                "f": 27781,
                "l": 27781,
                "T": 1609459200000_i64,
                "m": true,
                "M": true
            },
            {
                "a": 26130,
                "p": "29100.50",
                "q": "1.20000000",
                "f": 27782,
                "l": 27782,
                "T": 1609459260000_i64,
                "m": false,
                "M": true
            }
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Second batch (fromId = 26131): return 1 trade.
    // Last agg_trade_id = 26131, so next fromId = 26132
    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .and(query_param("fromId", "26131"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            {
                "a": 26131,
                "p": "29200.00",
                "q": "0.75000000",
                "f": 27783,
                "l": 27783,
                "T": 1609459320000_i64,
                "m": true,
                "M": true
            }
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Third batch (fromId = 26132): return empty -> pagination ends
    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .and(query_param("fromId", "26132"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTCUSDT".to_string(),
        start: Some(DateTime::from_timestamp_millis(1609459200000).unwrap()),
        end: None,
        limit: None,
    };

    let batches: Vec<_> = client.stream_trades(request).collect().await;

    assert_eq!(batches.len(), 2, "expected 2 non-empty batches");
    let batch1 = batches[0].as_ref().unwrap();
    let batch2 = batches[1].as_ref().unwrap();

    assert_eq!(batch1.len(), 2, "first batch should have 2 trades");
    assert_eq!(batch2.len(), 1, "second batch should have 1 trade");

    // Verify total trade count
    let total: usize = batches.iter().map(|b| b.as_ref().unwrap().len()).sum();
    assert_eq!(total, 3, "expected 3 trades in total");

    // Verify ordering: oldest-first within each batch
    assert_eq!(
        batch1[0].time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert_eq!(
        batch1[1].time,
        DateTime::from_timestamp_millis(1609459260000).unwrap()
    );
    assert_eq!(
        batch2[0].time,
        DateTime::from_timestamp_millis(1609459320000).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Test 5: trades are returned in chronological order (oldest-first)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_oldest_first_ordering() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/aggTrades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(three_trades_json()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTCUSDT".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    // Binance returns oldest-first natively, so no reversal needed.
    // Verify strictly increasing timestamps.
    for window in trades.windows(2) {
        assert!(
            window[0].time <= window[1].time,
            "trades should be ordered oldest-first: {} should come before {}",
            window[0].time,
            window[1].time,
        );
    }
}

// ===========================================================================
// Binance Futures USD tests
// ===========================================================================

/// Helper: start a mock server and create a `BinanceRestClient<BinanceServerFuturesUsd>`
/// whose base URL points at the mock server.
async fn setup_futures() -> (MockServer, BinanceRestClient<BinanceServerFuturesUsd>) {
    let mock_server = MockServer::start().await;
    let client = BinanceRestClient::<BinanceServerFuturesUsd>::with_base_url(mock_server.uri());
    (mock_server, client)
}

// ---------------------------------------------------------------------------
// Futures Test 1: fetch_trades returns a single batch of 3 trades
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_futures_fetch_trades() {
    let (mock_server, client) = setup_futures().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/aggTrades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(three_trades_json()))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTCUSDT".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    assert_eq!(trades.len(), 3);

    // First trade
    assert_eq!(trades[0].id, "26129");
    assert_eq!(
        trades[0].time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert!((trades[0].price - 29000.0).abs() < 1e-6);
    assert!((trades[0].amount - 0.5).abs() < 1e-6);
    assert_eq!(trades[0].side, barter_instrument::Side::Sell);

    // Second trade
    assert_eq!(trades[1].id, "26130");
    assert!((trades[1].price - 29100.50).abs() < 1e-6);
    assert_eq!(trades[1].side, barter_instrument::Side::Buy);

    // Third trade
    assert_eq!(trades[2].id, "26131");
    assert!((trades[2].price - 29200.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// Futures Test 2: fetch_trades propagates a Binance API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_futures_fetch_trades_api_error() {
    let (mock_server, client) = setup_futures().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/aggTrades"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"code": -1121, "msg": "Invalid symbol."})),
        )
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "INVALID".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let result = client.fetch_trades(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("-1121"),
        "error should contain Binance error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Invalid symbol"),
        "error should contain Binance error message, got: {err_msg}"
    );
}
