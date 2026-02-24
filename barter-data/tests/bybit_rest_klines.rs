#![cfg(feature = "rest")]

use barter_data::{
    exchange::bybit::{
        rest::{BybitHttpParser, BybitRestClient},
        spot::BybitServerSpot,
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

/// Helper: start a mock server and create a `BybitRestClient<BybitServerSpot>`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, BybitRestClient<BybitServerSpot>) {
    let mock_server = MockServer::start().await;
    let client = BybitRestClient::<BybitServerSpot>::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Fixture: build a Bybit klines JSON response wrapping the given kline arrays.
///
/// Each kline is a Vec of 7 string elements:
/// `[startTime, open, high, low, close, volume, turnover]`
fn bybit_klines_response(klines: Vec<Vec<&str>>) -> serde_json::Value {
    json!({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "symbol": "BTCUSDT",
            "category": "spot",
            "list": klines
        }
    })
}

// ---------------------------------------------------------------------------
// Test 1: fetch_klines returns a single batch of 3 candles in oldest-first
//         order (Bybit API returns newest-first, KlineFetcher reverses them)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_single_batch() {
    let (mock_server, client) = setup().await;

    // Bybit returns klines newest-first, so the mock response has the latest
    // candle first and the oldest candle last.
    let response_body = bybit_klines_response(vec![
        // newest
        vec![
            "1609632000000",
            "29800.00",
            "30500.00",
            "29600.00",
            "30100.00",
            "800.00",
            "24000000.00",
        ],
        // middle
        vec![
            "1609545600000",
            "29200.00",
            "30000.00",
            "29100.00",
            "29800.00",
            "1200.00",
            "35000000.00",
        ],
        // oldest
        vec![
            "1609459200000",
            "29000.00",
            "29500.00",
            "28800.00",
            "29200.00",
            "1000.00",
            "29000000.00",
        ],
    ]);

    Mock::given(method("GET"))
        .and(path("/v5/market/kline"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
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

    // After reversal the oldest candle should be first.
    // First candle (oldest)
    assert_eq!(
        candles[0].open_time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert!((candles[0].open - 29000.0).abs() < 1e-6);
    assert!((candles[0].high - 29500.0).abs() < 1e-6);
    assert!((candles[0].low - 28800.0).abs() < 1e-6);
    assert!((candles[0].close - 29200.0).abs() < 1e-6);
    assert!((candles[0].volume - 1000.0).abs() < 1e-6);
    assert!((candles[0].quote_volume.unwrap() - 29000000.0).abs() < 1e-6);
    assert_eq!(candles[0].trade_count, 0); // Bybit klines do not include trade count

    // Second candle (middle)
    assert_eq!(
        candles[1].open_time,
        DateTime::from_timestamp_millis(1609545600000).unwrap()
    );
    assert!((candles[1].open - 29200.0).abs() < 1e-6);
    assert!((candles[1].close - 29800.0).abs() < 1e-6);

    // Third candle (newest, but now last after reversal)
    assert_eq!(
        candles[2].open_time,
        DateTime::from_timestamp_millis(1609632000000).unwrap()
    );
    assert!((candles[2].open - 29800.0).abs() < 1e-6);
    assert!((candles[2].close - 30100.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_klines with an empty list returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_empty_response() {
    let (mock_server, client) = setup().await;

    let response_body = bybit_klines_response(vec![]);

    Mock::given(method("GET"))
        .and(path("/v5/market/kline"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
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
// Test 3: fetch_klines propagates a Bybit API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/v5/market/kline"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"retCode": 10001, "retMsg": "params error"})),
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
        err_msg.contains("10001"),
        "error should contain Bybit error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("params error"),
        "error should contain Bybit error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: stream_klines paginates through multiple batches
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_klines_pagination() {
    let (mock_server, client) = setup().await;

    // First batch (start = 1609459200000): return 2 klines (newest-first in response).
    // After reversal: open_times 1609459200000, 1609545600000.
    // Cursor advances to last open_time + 1ms = 1609545600001.
    Mock::given(method("GET"))
        .and(path("/v5/market/kline"))
        .and(query_param("start", "1609459200000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(bybit_klines_response(vec![
                // newest first (will be reversed)
                vec![
                    "1609545600000",
                    "29200.00",
                    "30000.00",
                    "29100.00",
                    "29800.00",
                    "1200.00",
                    "35000000.00",
                ],
                vec![
                    "1609459200000",
                    "29000.00",
                    "29500.00",
                    "28800.00",
                    "29200.00",
                    "1000.00",
                    "29000000.00",
                ],
            ])),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Second batch (start = 1609545600001): return 1 kline.
    // After reversal: open_time 1609632000000.
    // Cursor advances to 1609632000001.
    Mock::given(method("GET"))
        .and(path("/v5/market/kline"))
        .and(query_param("start", "1609545600001"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(bybit_klines_response(vec![vec![
                "1609632000000",
                "29800.00",
                "30500.00",
                "29600.00",
                "30100.00",
                "800.00",
                "24000000.00",
            ]])),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Third batch (start = 1609632000001): return empty -> pagination ends.
    Mock::given(method("GET"))
        .and(path("/v5/market/kline"))
        .and(query_param("start", "1609632000001"))
        .respond_with(ResponseTemplate::new(200).set_body_json(bybit_klines_response(vec![])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTCUSDT".to_string(),
        interval: Interval::H1,
        start: Some(DateTime::from_timestamp_millis(1609459200000).unwrap()),
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

    // Verify oldest-first ordering within each batch
    assert_eq!(
        batch1[0].open_time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert_eq!(
        batch1[1].open_time,
        DateTime::from_timestamp_millis(1609545600000).unwrap()
    );
    assert_eq!(
        batch2[0].open_time,
        DateTime::from_timestamp_millis(1609632000000).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Test 5: BybitHttpParser correctly converts a Bybit API error payload
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_parser_bybit_api_error() {
    let parser = BybitHttpParser;
    let error_payload = br#"{"retCode": 10001, "retMsg": "params error"}"#;

    // Attempt to parse as Vec<serde_json::Value> (simulating a klines response
    // type that expects an array). The error payload is an object, so
    // deserialization into Vec will fail, and the parser should fall through to
    // the ApiError branch.
    let result: Result<Vec<serde_json::Value>, _> =
        parser.parse(StatusCode::BAD_REQUEST, error_payload);

    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("10001"),
        "DataError should contain the Bybit error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("params error"),
        "DataError should contain the Bybit error message, got: {err_msg}"
    );
}
