#![cfg(feature = "rest")]

use barter_data::{
    exchange::coinbase::rest::CoinbaseRestClient,
    rest::{TradeFetcher, TradeRequest},
};
use chrono::DateTime;
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

/// Helper: wrap an array of trade objects in the Coinbase response envelope.
fn coinbase_trades_response(trades: serde_json::Value) -> serde_json::Value {
    json!({
        "trades": trades,
        "best_bid": "29000.00",
        "best_ask": "29001.00"
    })
}

/// Fixture: 3 realistic Coinbase trade JSON objects in **newest-first** order
/// (as the real API returns them).
///
/// Trade C (newest): time = 2021-01-01T00:05:00Z
/// Trade B:          time = 2021-01-01T00:02:00Z
/// Trade A (oldest): time = 2021-01-01T00:00:00Z
fn three_trades_newest_first() -> serde_json::Value {
    json!([
        {
            "trade_id": "34964585",
            "product_id": "BTC-USD",
            "price": "29200.00",
            "size": "0.750000",
            "time": "2021-01-01T00:05:00.000Z",
            "side": "SELL",
            "bid": "",
            "ask": ""
        },
        {
            "trade_id": "34964584",
            "product_id": "BTC-USD",
            "price": "29100.50",
            "size": "1.200000",
            "time": "2021-01-01T00:02:00.000Z",
            "side": "BUY",
            "bid": "",
            "ask": ""
        },
        {
            "trade_id": "34964583",
            "product_id": "BTC-USD",
            "price": "29000.00",
            "size": "0.500000",
            "time": "2021-01-01T00:00:00.000Z",
            "side": "SELL",
            "bid": "",
            "ask": ""
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
        .and(path("/api/v3/brokerage/market/products/BTC-USD/ticker"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(coinbase_trades_response(three_trades_newest_first())),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTC-USD".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    assert_eq!(trades.len(), 3);

    // After reversal the trades should be oldest-first.

    // First trade (oldest): Trade A
    assert_eq!(trades[0].id, "34964583");
    assert_eq!(
        trades[0].time,
        DateTime::parse_from_rfc3339("2021-01-01T00:00:00.000Z")
            .unwrap()
            .to_utc()
    );
    assert!((trades[0].price - 29000.0).abs() < 1e-6);
    assert!((trades[0].amount - 0.5).abs() < 1e-6);
    assert_eq!(trades[0].side, barter_instrument::Side::Sell);

    // Second trade: Trade B
    assert_eq!(trades[1].id, "34964584");
    assert_eq!(
        trades[1].time,
        DateTime::parse_from_rfc3339("2021-01-01T00:02:00.000Z")
            .unwrap()
            .to_utc()
    );
    assert!((trades[1].price - 29100.50).abs() < 1e-6);
    assert!((trades[1].amount - 1.2).abs() < 1e-6);
    assert_eq!(trades[1].side, barter_instrument::Side::Buy);

    // Third trade (newest): Trade C
    assert_eq!(trades[2].id, "34964585");
    assert_eq!(
        trades[2].time,
        DateTime::parse_from_rfc3339("2021-01-01T00:05:00.000Z")
            .unwrap()
            .to_utc()
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
        .and(path("/api/v3/brokerage/market/products/BTC-USD/ticker"))
        .respond_with(ResponseTemplate::new(200).set_body_json(coinbase_trades_response(json!([]))))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTC-USD".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();
    assert!(trades.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_trades propagates a Coinbase API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/INVALID/ticker"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": "NOT_FOUND", "message": "Product not found"})),
        )
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "INVALID".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let result = client.fetch_trades(request).await;
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
// Test 5: after reversal trades are in oldest-first order
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_oldest_first_after_reversal() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/api/v3/brokerage/market/products/BTC-USD/ticker"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(coinbase_trades_response(three_trades_newest_first())),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "BTC-USD".to_string(),
        start: None,
        end: None,
        limit: None,
        initial_cursor: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    // Verify strictly increasing timestamps (oldest-first after reversal)
    for window in trades.windows(2) {
        assert!(
            window[0].time <= window[1].time,
            "trades should be ordered oldest-first: {} should come before {}",
            window[0].time,
            window[1].time,
        );
    }
}

/// Helper to extract the trade ID (avoids repeated `.id.as_str()` everywhere).
fn trades_id(trade: &barter_data::rest::RestTrade) -> &str {
    &trade.id
}
