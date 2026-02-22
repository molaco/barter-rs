#![cfg(feature = "rest")]

use barter_data::{
    exchange::coinbase::rest::{
        CoinbaseHttpParser, CoinbaseRestClient,
        klines::CoinbaseKlinesResponse,
    },
    rest::{KlineFetcher, KlineRequest},
    subscription::candle::Interval,
};
use barter_integration::protocol::http::HttpParser;
use chrono::DateTime;
use futures::StreamExt;
use reqwest::StatusCode;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

/// Helper: start a mock server and create a `CoinbaseRestClient`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, CoinbaseRestClient) {
    let mock_server = MockServer::start().await;
    let client = CoinbaseRestClient::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Helper: wrap an array of candle objects in the Coinbase response envelope.
fn coinbase_klines_response(candles: serde_json::Value) -> serde_json::Value {
    json!({"candles": candles})
}

/// Fixture: 3 realistic Coinbase kline JSON objects in **newest-first** order
/// (as the real API would return them).
///
/// Timestamps (seconds):
///   Candle C (newest): start = 1609466400  (2021-01-01T02:00:00Z)
///   Candle B:          start = 1609462800  (2021-01-01T01:00:00Z)
///   Candle A (oldest): start = 1609459200  (2021-01-01T00:00:00Z)
fn three_klines_newest_first() -> serde_json::Value {
    json!([
        {"start": "1609466400", "low": "29600.00", "high": "30500.00", "open": "29800.00", "close": "30100.00", "volume": "800.00"},
        {"start": "1609462800", "low": "29100.00", "high": "30000.00", "open": "29200.00", "close": "29800.00", "volume": "1200.00"},
        {"start": "1609459200", "low": "28800.00", "high": "29500.00", "open": "29000.00", "close": "29200.00", "volume": "1000.00"}
    ])
}

// ---------------------------------------------------------------------------
// Test 1: fetch_klines returns a single batch of 3 candles (oldest-first)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_single_batch() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/BTC-USD/candles"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(coinbase_klines_response(three_klines_newest_first())),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTC-USD".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();

    assert_eq!(candles.len(), 3);

    // After reversal the candles should be oldest-first.

    // First candle (oldest): open_time = 1609459200
    assert_eq!(
        candles[0].open_time,
        DateTime::from_timestamp(1609459200, 0).unwrap()
    );
    // close_time = open_time + 1 hour
    assert_eq!(
        candles[0].close_time,
        DateTime::from_timestamp(1609462800, 0).unwrap()
    );
    assert!((candles[0].open - 29000.0).abs() < 1e-6);
    assert!((candles[0].high - 29500.0).abs() < 1e-6);
    assert!((candles[0].low - 28800.0).abs() < 1e-6);
    assert!((candles[0].close - 29200.0).abs() < 1e-6);
    assert!((candles[0].volume - 1000.0).abs() < 1e-6);
    // Coinbase does not provide quote_volume or trade_count
    assert_eq!(candles[0].quote_volume, None);
    assert_eq!(candles[0].trade_count, 0);

    // Second candle: open_time = 1609462800
    assert_eq!(
        candles[1].open_time,
        DateTime::from_timestamp(1609462800, 0).unwrap()
    );
    assert!((candles[1].open - 29200.0).abs() < 1e-6);
    assert!((candles[1].close - 29800.0).abs() < 1e-6);

    // Third candle (newest): open_time = 1609466400
    assert_eq!(
        candles[2].open_time,
        DateTime::from_timestamp(1609466400, 0).unwrap()
    );
    assert!((candles[2].open - 29800.0).abs() < 1e-6);
    assert!((candles[2].close - 30100.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_klines with an empty response returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_empty_response() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/BTC-USD/candles"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(coinbase_klines_response(json!([]))),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTC-USD".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();
    assert!(candles.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_klines propagates a Coinbase API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/v3/brokerage/market/products/INVALID/candles",
        ))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": "NOT_FOUND", "message": "Product not found"})),
        )
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "INVALID".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let result = client.fetch_klines(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("NOT_FOUND"),
        "error should contain Coinbase error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Product not found"),
        "error should contain Coinbase error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: fetch_klines with unsupported interval returns an error
//         without making any HTTP call
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_unsupported_interval() {
    // No mock server needed -- the error should fire before any HTTP call.
    let client = CoinbaseRestClient::with_base_url("http://127.0.0.1:1".to_string());

    let request = KlineRequest {
        market: "BTC-USD".to_string(),
        interval: Interval::M3,
        start: None,
        end: None,
        limit: None,
    };

    let result = client.fetch_klines(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("does not support interval"),
        "error should mention unsupported interval, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 5: stream_klines paginates through multiple batches
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_klines_pagination() {
    let (mock_server, client) = setup().await;

    // Batch 1 (start=1609459200): return 2 klines newest-first.
    // After reversal: [candle_A, candle_B]
    //   candle_A: start=1609459200, close_time=1609462800
    //   candle_B: start=1609462800, close_time=1609466400
    // Next cursor = 1609466400 + 1 = 1609466401
    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/BTC-USD/candles"))
        .and(query_param("start", "1609459200"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(coinbase_klines_response(json!([
                {"start": "1609462800", "low": "29100.00", "high": "30000.00", "open": "29200.00", "close": "29800.00", "volume": "1200.00"},
                {"start": "1609459200", "low": "28800.00", "high": "29500.00", "open": "29000.00", "close": "29200.00", "volume": "1000.00"}
            ]))),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Batch 2 (start=1609466401): return 1 kline.
    // After reversal: [candle_C]
    //   candle_C: start=1609466400, close_time=1609470000
    // Next cursor = 1609470000 + 1 = 1609470001
    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/BTC-USD/candles"))
        .and(query_param("start", "1609466401"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(coinbase_klines_response(json!([
                {"start": "1609466400", "low": "29600.00", "high": "30500.00", "open": "29800.00", "close": "30100.00", "volume": "800.00"}
            ]))),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Batch 3 (start=1609470001): return empty -> pagination ends.
    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/BTC-USD/candles"))
        .and(query_param("start", "1609470001"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(coinbase_klines_response(json!([]))),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTC-USD".to_string(),
        interval: Interval::H1,
        start: Some(DateTime::from_timestamp(1609459200, 0).unwrap()),
        end: None,
        limit: None,
    };

    let batches: Vec<_> = client.stream_klines(request).collect().await;

    assert_eq!(batches.len(), 2, "expected 2 non-empty batches");
    let batch1 = batches[0].as_ref().unwrap();
    let batch2 = batches[1].as_ref().unwrap();

    assert_eq!(batch1.len(), 2, "first batch should have 2 candles");
    assert_eq!(batch2.len(), 1, "second batch should have 1 candle");

    // Verify total candle count
    let total: usize = batches.iter().map(|b| b.as_ref().unwrap().len()).sum();
    assert_eq!(total, 3, "expected 3 candles in total");

    // Spot-check ordering: candles within each batch are oldest-first.
    assert_eq!(
        batch1[0].open_time,
        DateTime::from_timestamp(1609459200, 0).unwrap()
    );
    assert_eq!(
        batch1[1].open_time,
        DateTime::from_timestamp(1609462800, 0).unwrap()
    );
    assert_eq!(
        batch2[0].open_time,
        DateTime::from_timestamp(1609466400, 0).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Test 6: CoinbaseHttpParser correctly converts a Coinbase API error payload
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_parser_coinbase_api_error() {
    let parser = CoinbaseHttpParser;
    let error_payload = br#"{"error": "NOT_FOUND", "message": "Product not found"}"#;

    // Attempt to parse as CoinbaseKlinesResponse (which expects a "candles" field).
    // The error payload lacks "candles", so deserialization fails and the parser
    // falls through to the ApiError branch.
    let result: Result<CoinbaseKlinesResponse, _> =
        parser.parse(StatusCode::BAD_REQUEST, error_payload);

    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("NOT_FOUND"),
        "DataError should contain the Coinbase error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Product not found"),
        "DataError should contain the Coinbase error message, got: {err_msg}"
    );
}
