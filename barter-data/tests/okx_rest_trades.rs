#![cfg(feature = "rest")]

use barter_data::{
    exchange::okx::rest::OkxRestClient,
    rest::{TradeFetcher, TradeRequest},
};
use chrono::DateTime;
use serde_json::json;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param, query_param_is_missing},
};

/// Helper: start a mock server and create an `OkxRestClient`
/// whose base URL points at the mock server.
async fn setup() -> (MockServer, OkxRestClient) {
    let mock_server = MockServer::start().await;
    let client = OkxRestClient::with_base_url(mock_server.uri());
    (mock_server, client)
}

/// Helper: build an OKX trades response envelope from raw trade objects.
fn okx_trades_response(trades: serde_json::Value) -> serde_json::Value {
    json!({
        "code": "0",
        "msg": "",
        "data": trades
    })
}

/// Fixture: 3 realistic OKX trade JSON objects in **newest-first** order
/// (as the real API returns them).
///
/// Trade C (newest): ts = 1609459320000  tradeId = "391120172"
/// Trade B:          ts = 1609459260000  tradeId = "391120171"
/// Trade A (oldest): ts = 1609459200000  tradeId = "391120170"
fn three_trades_newest_first() -> serde_json::Value {
    json!([
        {
            "instId": "BTC-USDT",
            "tradeId": "391120172",
            "px": "29200.00",
            "sz": "0.750000",
            "side": "buy",
            "ts": "1609459320000"
        },
        {
            "instId": "BTC-USDT",
            "tradeId": "391120171",
            "px": "29100.50",
            "sz": "1.200000",
            "side": "sell",
            "ts": "1609459260000"
        },
        {
            "instId": "BTC-USDT",
            "tradeId": "391120170",
            "px": "29000.00",
            "sz": "0.500000",
            "side": "buy",
            "ts": "1609459200000"
        }
    ])
}

// ---------------------------------------------------------------------------
// Test 1: fetch_trades returns a single batch reversed to oldest-first
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_single_batch() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-trades"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(okx_trades_response(three_trades_newest_first())),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTC-USDT".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    assert_eq!(trades.len(), 3);

    // After reversal the trades should be oldest-first.

    // First trade (oldest): Trade A
    assert_eq!(trades[0].id, "391120170");
    assert_eq!(
        trades[0].time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert!((trades[0].price - 29000.0).abs() < 1e-6);
    assert!((trades[0].amount - 0.5).abs() < 1e-6);
    assert_eq!(trades[0].side, barter_instrument::Side::Buy);

    // Second trade: Trade B
    assert_eq!(trades[1].id, "391120171");
    assert_eq!(
        trades[1].time,
        DateTime::from_timestamp_millis(1609459260000).unwrap()
    );
    assert!((trades[1].price - 29100.5).abs() < 1e-6);
    assert!((trades[1].amount - 1.2).abs() < 1e-6);
    assert_eq!(trades[1].side, barter_instrument::Side::Sell);

    // Third trade (newest): Trade C
    assert_eq!(trades[2].id, "391120172");
    assert_eq!(
        trades[2].time,
        DateTime::from_timestamp_millis(1609459320000).unwrap()
    );
    assert!((trades[2].price - 29200.0).abs() < 1e-6);
    assert!((trades[2].amount - 0.75).abs() < 1e-6);
    assert_eq!(trades[2].side, barter_instrument::Side::Buy);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_trades with an empty data array returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_empty_response() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-trades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(okx_trades_response(json!([]))))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTC-USDT".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();
    assert!(trades.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_trades propagates an OKX API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-trades"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "code": "51001",
            "msg": "Instrument ID does not exist",
            "data": []
        })))
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "INVALID-PAIR".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let result = client.fetch_trades(request).await;
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
// Test 4: fetch_trades propagates an OKX application-level error (HTTP 200
//         but code != "0")
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_okx_level_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-trades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "51001",
            "msg": "Instrument ID does not exist",
            "data": []
        })))
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "INVALID-PAIR".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let result = client.fetch_trades(request).await;
    assert!(result.is_err());

    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("51001"),
        "error should contain OKX error code, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 6: after reversal trades are in oldest-first order
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_oldest_first_after_reversal() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v5/market/history-trades"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(okx_trades_response(three_trades_newest_first())),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTC-USDT".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    // OKX returns newest-first; fetch_trades reverses to oldest-first.
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
