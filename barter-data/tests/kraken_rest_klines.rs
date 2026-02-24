#![cfg(feature = "rest")]

use barter_data::{
    exchange::kraken::rest::{KrakenHttpParser, KrakenRestClient},
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

/// Helper: start a mock server and create a `KrakenRestClient`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, KrakenRestClient) {
    let mock_server = MockServer::start().await;
    let client = KrakenRestClient::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Helper: build a Kraken OHLC response JSON with a dynamic pair key.
///
/// Kraken responses have the pair name as a dynamic key in the `result` object,
/// so we must build the JSON manually using `serde_json::Map`.
fn kraken_klines_response(
    pair: &str,
    klines: Vec<serde_json::Value>,
    last: i64,
) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    result.insert(pair.to_string(), serde_json::Value::Array(klines));
    result.insert("last".to_string(), json!(last));
    json!({
        "error": [],
        "result": result
    })
}

/// Fixture: 3 realistic Kraken kline arrays.
///
/// Each kline is: [time_seconds, open, high, low, close, vwap, volume, count]
fn three_klines_json() -> Vec<serde_json::Value> {
    vec![
        json!([
            1672502400, "16800.00", "16900.50", "16750.00", "16850.00", "16825.30", "1234.56", 5000
        ]),
        json!([
            1672506000, "16850.00", "16950.00", "16800.00", "16900.00", "16875.00", "987.65", 3000
        ]),
        json!([
            1672509600, "16900.00", "17000.00", "16850.00", "16950.00", "16925.00", "1500.00", 4000
        ]),
    ]
}

// ---------------------------------------------------------------------------
// Test 1: fetch_klines returns a single batch of 3 candles (oldest-first)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_single_batch() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_klines_response(
                "XBTUSD",
                three_klines_json(),
                1672588800,
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "XBTUSD".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();

    assert_eq!(candles.len(), 3);

    // First candle (time in seconds -> millis: 1672502400 * 1000)
    assert_eq!(
        candles[0].open_time,
        DateTime::from_timestamp_millis(1672502400 * 1000).unwrap()
    );
    // Kraken sets close_time = open_time (no close_time in response)
    assert_eq!(candles[0].close_time, candles[0].open_time);
    assert!((candles[0].open - 16800.0).abs() < 1e-6);
    assert!((candles[0].high - 16900.50).abs() < 1e-6);
    assert!((candles[0].low - 16750.0).abs() < 1e-6);
    assert!((candles[0].close - 16850.0).abs() < 1e-6);
    assert!((candles[0].volume - 1234.56).abs() < 1e-6);
    assert!(candles[0].quote_volume.is_none());
    assert_eq!(candles[0].trade_count, 5000);

    // Second candle
    assert_eq!(
        candles[1].open_time,
        DateTime::from_timestamp_millis(1672506000 * 1000).unwrap()
    );
    assert!((candles[1].open - 16850.0).abs() < 1e-6);
    assert!((candles[1].close - 16900.0).abs() < 1e-6);

    // Third candle (oldest-first ordering preserved, no reversal)
    assert_eq!(
        candles[2].open_time,
        DateTime::from_timestamp_millis(1672509600 * 1000).unwrap()
    );
    assert!((candles[2].open - 16900.0).abs() < 1e-6);
    assert!((candles[2].close - 16950.0).abs() < 1e-6);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_klines with an empty kline array returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_empty_response() {
    let (mock_server, client) = setup().await;

    let response_body = json!({
        "error": [],
        "result": {
            "XBTUSD": [],
            "last": 0
        }
    });

    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "XBTUSD".to_string(),
        interval: Interval::H1,
        start: None,
        end: None,
        limit: None,
    };

    let candles = client.fetch_klines(request).await.unwrap();
    assert!(candles.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_klines propagates an API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": ["EGeneral:Invalid arguments"], "result": {}})),
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
        err_msg.contains("EGeneral:Invalid arguments"),
        "error should contain Kraken error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: stream_klines paginates using cursor-based `last` / `since` params
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_klines_pagination() {
    let (mock_server, client) = setup().await;

    // Page 1: since=1672502400 (from start time) -> returns 2 klines, last=1672520000
    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .and(query_param("since", "1672502400"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_klines_response(
                "XBTUSD",
                vec![
                    json!([
                        1672502400, "16800.00", "16900.50", "16750.00", "16850.00", "16825.30",
                        "1234.56", 5000
                    ]),
                    json!([
                        1672506000, "16850.00", "16950.00", "16800.00", "16900.00", "16875.00",
                        "987.65", 3000
                    ]),
                ],
                1672520000,
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Page 2: since=1672520000 (cursor from page 1) -> returns 1 kline, last=1672530000
    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .and(query_param("since", "1672520000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_klines_response(
                "XBTUSD",
                vec![json!([
                    1672509600, "16900.00", "17000.00", "16850.00", "16950.00", "16925.00",
                    "1500.00", 4000
                ])],
                1672530000,
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Page 3: since=1672530000 (cursor from page 2) -> returns empty -> pagination ends
    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .and(query_param("since", "1672530000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_klines_response(
                "XBTUSD",
                vec![],
                1672530000,
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "XBTUSD".to_string(),
        interval: Interval::H1,
        // start time -> converted to seconds via .timestamp() -> since=1672502400
        start: Some(DateTime::from_timestamp(1672502400, 0).unwrap()),
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

    // Spot-check ordering across batches
    assert_eq!(
        batch1[0].open_time,
        DateTime::from_timestamp_millis(1672502400 * 1000).unwrap()
    );
    assert_eq!(
        batch1[1].open_time,
        DateTime::from_timestamp_millis(1672506000 * 1000).unwrap()
    );
    assert_eq!(
        batch2[0].open_time,
        DateTime::from_timestamp_millis(1672509600 * 1000).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Test 5: fetch_klines with an unsupported interval returns an error
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_klines_unsupported_interval() {
    let (mock_server, client) = setup().await;

    // No mock needed — the error should occur before any HTTP request is made.
    // But mount a catch-all to verify no request is sent.
    Mock::given(method("GET"))
        .and(path("/0/public/OHLC"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .expect(0)
        .mount(&mock_server)
        .await;

    let request = KlineRequest {
        market: "XBTUSD".to_string(),
        interval: Interval::M3,
        start: None,
        end: None,
        limit: None,
    };

    let result = client.fetch_klines(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("3m") || err_msg.contains("M3") || err_msg.contains("3m interval"),
        "error should indicate unsupported interval, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 6: KrakenHttpParser correctly converts a Kraken API error payload
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_http_parser_kraken_api_error() {
    let parser = KrakenHttpParser;
    let error_payload = br#"{"error": ["EGeneral:Invalid arguments"], "result": {}}"#;

    // Attempt to parse as KrakenOhlcResponse (simulating a klines response type).
    // The parser should fall through to the ApiError branch for non-200 status.
    let result: Result<Vec<serde_json::Value>, _> =
        parser.parse(StatusCode::BAD_REQUEST, error_payload);

    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("EGeneral:Invalid arguments"),
        "DataError should contain the Kraken error message, got: {err_msg}"
    );
}
