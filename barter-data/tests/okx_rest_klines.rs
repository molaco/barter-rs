#![cfg(feature = "rest")]

use barter_data::{
    exchange::okx::rest::{OkxHttpParser, OkxRestClient},
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

/// Helper: start a mock server and create an `OkxRestClient`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, OkxRestClient) {
    let mock_server = MockServer::start().await;
    let client = OkxRestClient::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Fixture: build an OKX klines response envelope from raw kline arrays.
fn okx_klines_response(klines: Vec<Vec<&str>>) -> serde_json::Value {
    json!({
        "code": "0",
        "msg": "",
        "data": klines
    })
}

// ---------------------------------------------------------------------------
// Test 1: fetch_klines returns a single batch of 3 candles (reversed to
//         oldest-first order)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_single_batch() {
    let (mock_server, client) = setup().await;

    // OKX returns newest-first; the KlineFetcher reverses to oldest-first.
    // Using H1 interval: close_time = open_time + 3_600_000 - 1
    //   Kline C: open_time = 1609632000000  (newest in response, first element)
    //   Kline B: open_time = 1609545600000
    //   Kline A: open_time = 1609459200000  (oldest in response, last element)
    let body = okx_klines_response(vec![
        vec![
            "1609632000000",
            "29800.00",
            "30500.00",
            "29600.00",
            "30100.00",
            "800.00",
            "24000000.00",
            "24000000.00",
            "1",
        ],
        vec![
            "1609545600000",
            "29200.00",
            "30000.00",
            "29100.00",
            "29800.00",
            "1200.00",
            "35000000.00",
            "35000000.00",
            "1",
        ],
        vec![
            "1609459200000",
            "29000.00",
            "29500.00",
            "28800.00",
            "29200.00",
            "1000.00",
            "29000000.00",
            "29000000.00",
            "1",
        ],
    ]);

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTC-USDT".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();

    assert_eq!(candles.len(), 3);

    // After reversal the candles should be oldest-first.
    // First candle (oldest)
    assert_eq!(
        candles[0].open_time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    // close_time = open_time + 1h - 1ms = 1609459200000 + 3599999
    assert_eq!(
        candles[0].close_time,
        DateTime::from_timestamp_millis(1609459200000 + 3_600_000 - 1).unwrap()
    );
    assert!((candles[0].open - 29000.0).abs() < 1e-6);
    assert!((candles[0].high - 29500.0).abs() < 1e-6);
    assert!((candles[0].low - 28800.0).abs() < 1e-6);
    assert!((candles[0].close - 29200.0).abs() < 1e-6);
    assert!((candles[0].volume - 1000.0).abs() < 1e-6);
    assert!((candles[0].quote_volume.unwrap() - 29000000.0).abs() < 1e-6);
    // OKX does not return trade_count
    assert_eq!(candles[0].trade_count, 0);

    // Second candle
    assert_eq!(
        candles[1].open_time,
        DateTime::from_timestamp_millis(1609545600000).unwrap()
    );
    assert!((candles[1].open - 29200.0).abs() < 1e-6);
    assert!((candles[1].close - 29800.0).abs() < 1e-6);

    // Third candle (newest)
    assert_eq!(
        candles[2].open_time,
        DateTime::from_timestamp_millis(1609632000000).unwrap()
    );
    assert!((candles[2].open - 29800.0).abs() < 1e-6);
    assert!((candles[2].close - 30100.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_klines with an empty data array returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_empty_response() {
    let (mock_server, client) = setup().await;

    let body = okx_klines_response(vec![]);

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTC-USDT".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();
    assert!(candles.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_klines propagates an OKX API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "code": "51001",
            "msg": "Instrument ID does not exist",
            "data": []
        })))
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "INVALID-PAIR".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let result = client.fetch_klines(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("51001"),
        "error should contain OKX error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Instrument ID does not exist"),
        "error should contain OKX error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: stream_klines paginates through multiple batches
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_klines_pagination() {
    let (mock_server, client) = setup().await;

    // OKX pagination uses `before` = cursor timestamp (records newer than this).
    // stream_klines sets start = Some(cursor), which becomes `before` in the request.
    //
    // First call: before = 1609459200000 (initial cursor from request.start)
    // Returns 2 klines (newest-first in response, reversed by fetch_klines):
    //   After reversal: [A(open=1609459200000), B(open=1609545600000)]
    //   B.close_time = 1609545600000 + 3_600_000 - 1 = 1609549199999
    //   Next cursor  = 1609549199999 + 1 = 1609549200000
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .and(query_param("before", "1609459200000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(okx_klines_response(vec![
                vec![
                    "1609545600000",
                    "29200.00",
                    "30000.00",
                    "29100.00",
                    "29800.00",
                    "1200.00",
                    "35000000.00",
                    "35000000.00",
                    "1",
                ],
                vec![
                    "1609459200000",
                    "29000.00",
                    "29500.00",
                    "28800.00",
                    "29200.00",
                    "1000.00",
                    "29000000.00",
                    "29000000.00",
                    "1",
                ],
            ])),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Second call: before = 1609549200000 (cursor advanced past B.close_time)
    // Returns 1 kline:
    //   After reversal: [C(open=1609632000000)]
    //   C.close_time = 1609632000000 + 3_600_000 - 1 = 1609635599999
    //   Next cursor  = 1609635599999 + 1 = 1609635600000
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .and(query_param("before", "1609549200000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(okx_klines_response(vec![vec![
                "1609632000000",
                "29800.00",
                "30500.00",
                "29600.00",
                "30100.00",
                "800.00",
                "24000000.00",
                "24000000.00",
                "1",
            ]])),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Third call: before = 1609635600000 -> empty response terminates pagination
    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-candles"))
        .and(query_param("before", "1609635600000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(okx_klines_response(vec![])))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "BTC-USDT".to_string(),
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

    // Spot-check ordering: all candles are oldest-first within each batch
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
// Test 5: OkxHttpParser correctly converts an OKX API error payload
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_parser_okx_api_error() {
    let parser = OkxHttpParser;
    let error_payload = br#"{"code": "51001", "msg": "Instrument ID does not exist", "data": []}"#;

    // Attempt to parse as Vec<serde_json::Value> (simulating a klines response type).
    // The parser should fall through to the ApiError branch.
    let result: Result<Vec<serde_json::Value>, _> =
        parser.parse(StatusCode::BAD_REQUEST, error_payload);

    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("51001"),
        "DataError should contain the OKX error code, got: {err_msg}"
    );
    assert!(
        err_msg.contains("Instrument ID does not exist"),
        "DataError should contain the OKX error message, got: {err_msg}"
    );
}
