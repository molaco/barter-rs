#![cfg(feature = "rest")]

use barter_data::{
    exchange::binance::{
        futures::BinanceServerFuturesUsd,
        rest::{BinanceHttpParser, BinanceRestClient},
        spot::BinanceServerSpot,
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

/// Helper: start a mock server and create a `BinanceRestClient<BinanceServerSpot>`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, BinanceRestClient<BinanceServerSpot>) {
    let mock_server = MockServer::start().await;
    let client =
        BinanceRestClient::<BinanceServerSpot>::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Fixture: a realistic Binance kline JSON array with 3 candles.
fn three_klines_json() -> serde_json::Value {
    json!([
        [1609459200000_i64,"29000.00","29500.00","28800.00","29200.00","1000.00",1609545599999_i64,"29000000.00",5000,"500.00","14500000.00","0"],
        [1609545600000_i64,"29200.00","30000.00","29100.00","29800.00","1200.00",1609631999999_i64,"35000000.00",6000,"600.00","17400000.00","0"],
        [1609632000000_i64,"29800.00","30500.00","29600.00","30100.00","800.00",1609718399999_i64,"24000000.00",4000,"400.00","12000000.00","0"]
    ])
}

// ---------------------------------------------------------------------------
// Test 1: fetch_klines returns a single batch of 3 candles
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_single_batch() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(three_klines_json()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTCUSDT".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();

    assert_eq!(candles.len(), 3);

    // First candle
    assert_eq!(
        candles[0].open_time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert_eq!(
        candles[0].close_time,
        DateTime::from_timestamp_millis(1609545599999).unwrap()
    );
    assert!((candles[0].open - 29000.0).abs() < 1e-6);
    assert!((candles[0].high - 29500.0).abs() < 1e-6);
    assert!((candles[0].low - 28800.0).abs() < 1e-6);
    assert!((candles[0].close - 29200.0).abs() < 1e-6);
    assert!((candles[0].volume - 1000.0).abs() < 1e-6);
    assert!((candles[0].quote_volume.unwrap() - 29000000.0).abs() < 1e-6);
    assert_eq!(candles[0].trade_count, 5000);

    // Second candle
    assert_eq!(
        candles[1].open_time,
        DateTime::from_timestamp_millis(1609545600000).unwrap()
    );
    assert!((candles[1].open - 29200.0).abs() < 1e-6);
    assert!((candles[1].close - 29800.0).abs() < 1e-6);

    // Third candle
    assert_eq!(
        candles[2].open_time,
        DateTime::from_timestamp_millis(1609632000000).unwrap()
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
        .and(path("/api/v3/klines"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTCUSDT".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();
    assert!(candles.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_klines propagates a Binance API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"code": -1121, "msg": "Invalid symbol."})),
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
        err_msg.contains("-1121"),
        "error should contain Binance error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Invalid symbol"),
        "error should contain Binance error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: stream_klines paginates through multiple batches
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_klines_pagination() {
    let (mock_server, client) = setup().await;

    // First batch (startTime = 1609459200000): return 2 klines
    // close_time of last kline = 1609631999999, so next cursor = 1609632000000
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("startTime", "1609459200000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            [1609459200000_i64,"29000.00","29500.00","28800.00","29200.00","1000.00",1609545599999_i64,"29000000.00",5000,"500.00","14500000.00","0"],
            [1609545600000_i64,"29200.00","30000.00","29100.00","29800.00","1200.00",1609631999999_i64,"35000000.00",6000,"600.00","17400000.00","0"]
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Second batch (startTime = 1609632000000): return 1 kline
    // close_time of last kline = 1609718399999, so next cursor = 1609718400000
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("startTime", "1609632000000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([
            [1609632000000_i64,"29800.00","30500.00","29600.00","30100.00","800.00",1609718399999_i64,"24000000.00",4000,"400.00","12000000.00","0"]
        ])))
        .expect(1)
        .mount(&mock_server)
        .await;

    // Third batch (startTime = 1609718400000): return empty -> pagination ends
    Mock::given(method("GET"))
        .and(path("/api/v3/klines"))
        .and(query_param("startTime", "1609718400000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTCUSDT".to_string(),
        interval: Interval::H1,
        start: Some(
            DateTime::from_timestamp_millis(1609459200000).unwrap(),
        ),
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

    // Spot-check ordering
    assert_eq!(
        batch1[0].open_time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert_eq!(
        batch2[0].open_time,
        DateTime::from_timestamp_millis(1609632000000).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Test 5: BinanceHttpParser correctly converts a Binance API error payload
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_parser_binance_api_error() {
    let parser = BinanceHttpParser;
    let error_payload = br#"{"code": -1121, "msg": "Invalid symbol."}"#;

    // Attempt to parse as Vec<serde_json::Value> (simulating a klines response type).
    // The parser should fall through to the ApiError branch.
    let result: Result<Vec<serde_json::Value>, _> =
        parser.parse(StatusCode::BAD_REQUEST, error_payload);

    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("-1121"),
        "DataError should contain the Binance error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Invalid symbol"),
        "DataError should contain the Binance error message, got: {err_msg}"
    );
}

// ===========================================================================
// Binance Futures USD tests
// ===========================================================================

/// Helper: start a mock server and create a `BinanceRestClient<BinanceServerFuturesUsd>`
/// whose base URL points at the mock server.
async fn setup_futures() -> (MockServer, BinanceRestClient<BinanceServerFuturesUsd>) {
    let mock_server = MockServer::start().await;
    let client =
        BinanceRestClient::<BinanceServerFuturesUsd>::with_base_url(mock_server.uri());
    (mock_server, client)
}

// ---------------------------------------------------------------------------
// Futures Test 1: fetch_klines returns a single batch of 3 candles
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_futures_fetch_klines() {
    let (mock_server, client) = setup_futures().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/klines"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(three_klines_json()),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTCUSDT".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();

    assert_eq!(candles.len(), 3);

    // First candle
    assert_eq!(
        candles[0].open_time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert_eq!(
        candles[0].close_time,
        DateTime::from_timestamp_millis(1609545599999).unwrap()
    );
    assert!((candles[0].open - 29000.0).abs() < 1e-6);
    assert!((candles[0].high - 29500.0).abs() < 1e-6);
    assert!((candles[0].low - 28800.0).abs() < 1e-6);
    assert!((candles[0].close - 29200.0).abs() < 1e-6);
    assert!((candles[0].volume - 1000.0).abs() < 1e-6);
    assert!((candles[0].quote_volume.unwrap() - 29000000.0).abs() < 1e-6);
    assert_eq!(candles[0].trade_count, 5000);

    // Second candle
    assert_eq!(
        candles[1].open_time,
        DateTime::from_timestamp_millis(1609545600000).unwrap()
    );
    assert!((candles[1].open - 29200.0).abs() < 1e-6);
    assert!((candles[1].close - 29800.0).abs() < 1e-6);

    // Third candle
    assert_eq!(
        candles[2].open_time,
        DateTime::from_timestamp_millis(1609632000000).unwrap()
    );
    assert!((candles[2].open - 29800.0).abs() < 1e-6);
    assert!((candles[2].close - 30100.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// Futures Test 2: fetch_klines propagates a Binance API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_futures_fetch_klines_api_error() {
    let (mock_server, client) = setup_futures().await;

    Mock::given(method("GET"))
        .and(path("/fapi/v1/klines"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"code": -1121, "msg": "Invalid symbol."})),
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
        err_msg.contains("-1121"),
        "error should contain Binance error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Invalid symbol"),
        "error should contain Binance error message, got: {err_msg}"
    );
}
