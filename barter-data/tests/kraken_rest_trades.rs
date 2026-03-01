#![cfg(feature = "rest")]

use barter_data::{
    exchange::kraken::rest::KrakenRestClient,
    rest::{TradeFetcher, TradeRequest},
};
use chrono::DateTime;
use futures::StreamExt;
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

/// Helper: build a Kraken Trades response JSON with a dynamic pair key.
///
/// Kraken responses have the pair name as a dynamic key in the `result` object,
/// so we must build the JSON manually using `serde_json::Map`.
fn kraken_trades_response(
    pair: &str,
    trades: Vec<serde_json::Value>,
    last: &str,
) -> serde_json::Value {
    let mut result = serde_json::Map::new();
    result.insert(pair.to_string(), serde_json::Value::Array(trades));
    result.insert("last".to_string(), json!(last));
    json!({
        "error": [],
        "result": result
    })
}

/// Fixture: 3 realistic Kraken trade arrays, oldest-first.
///
/// Each trade is: [price, volume, time, buy_sell, market_limit, misc, trade_id]
fn three_trades_json() -> Vec<serde_json::Value> {
    vec![
        json!(["29000.00000", "0.50000000", 1609459200.0000, "b", "m", "", 391120170_u64]),
        json!(["29100.50000", "1.20000000", 1609459260.1234, "s", "l", "", 391120171_u64]),
        json!(["29200.00000", "0.75000000", 1609459320.5678, "b", "m", "", 391120172_u64]),
    ]
}

// ---------------------------------------------------------------------------
// Test 1: fetch_trades returns a single batch of 3 trades (oldest-first)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_single_batch() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_trades_response(
                "XXBTZUSD",
                three_trades_json(),
                "1609459320567800000",
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "XBTUSD".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    assert_eq!(trades.len(), 3);

    // First trade (oldest)
    assert_eq!(trades[0].id, "391120170");
    // 1609459200.0 * 1000 = 1609459200000
    assert_eq!(
        trades[0].time,
        DateTime::from_timestamp_millis(1609459200000).unwrap()
    );
    assert!((trades[0].price - 29000.0).abs() < 1e-2);
    assert!((trades[0].amount - 0.5).abs() < 1e-6);
    assert_eq!(trades[0].side, barter_instrument::Side::Buy);

    // Second trade
    assert_eq!(trades[1].id, "391120171");
    // 1609459260.1234 * 1000 = 1609459260123.4 -> rounded to 1609459260123
    assert_eq!(
        trades[1].time,
        DateTime::from_timestamp_millis(1609459260123).unwrap()
    );
    assert!((trades[1].price - 29100.5).abs() < 1e-2);
    assert!((trades[1].amount - 1.2).abs() < 1e-6);
    assert_eq!(trades[1].side, barter_instrument::Side::Sell);

    // Third trade
    assert_eq!(trades[2].id, "391120172");
    // 1609459320.5678 * 1000 = 1609459320567.8 -> rounded to 1609459320568
    assert_eq!(
        trades[2].time,
        DateTime::from_timestamp_millis(1609459320568).unwrap()
    );
    assert!((trades[2].price - 29200.0).abs() < 1e-2);
    assert!((trades[2].amount - 0.75).abs() < 1e-6);
    assert_eq!(trades[2].side, barter_instrument::Side::Buy);
}

// ---------------------------------------------------------------------------
// Test 2: fetch_trades with an empty trades array returns an empty vec
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_empty_response() {
    let (mock_server, client) = setup().await;

    let response_body = json!({
        "error": [],
        "result": {
            "XXBTZUSD": [],
            "last": "0"
        }
    });

    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response_body))
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "XBTUSD".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();
    assert!(trades.is_empty());
}

// ---------------------------------------------------------------------------
// Test 3: fetch_trades propagates a Kraken API error (HTTP 400)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_api_error() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .respond_with(
            ResponseTemplate::new(400)
                .set_body_json(json!({"error": ["EGeneral:Invalid arguments"], "result": {}})),
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
        err_msg.contains("EGeneral:Invalid arguments"),
        "error should contain Kraken error message, got: {err_msg}"
    );
}

// ---------------------------------------------------------------------------
// Test 4: stream_trades paginates using nonce-based `last` / `since` params
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_stream_trades_pagination() {
    let (mock_server, client) = setup().await;

    // The start time 2021-01-01T00:00:00Z = 1609459200 seconds
    // As nanoseconds: 1609459200000000000
    //
    // Page 1: since=1609459200000000000 -> returns 2 trades, last="1609459260123400000"
    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .and(query_param("since", "1609459200000000000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_trades_response(
                "XXBTZUSD",
                vec![
                    json!(["29000.00000", "0.50000000", 1609459200.0000, "b", "m", "", 391120170_u64]),
                    json!(["29100.50000", "1.20000000", 1609459260.1234, "s", "l", "", 391120171_u64]),
                ],
                "1609459260123400000",
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Page 2: since=1609459260123400000 -> returns 1 trade, last="1609459320567800000"
    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .and(query_param("since", "1609459260123400000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_trades_response(
                "XXBTZUSD",
                vec![
                    json!(["29200.00000", "0.75000000", 1609459320.5678, "b", "m", "", 391120172_u64]),
                ],
                "1609459320567800000",
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    // Page 3: since=1609459320567800000 -> returns empty -> pagination ends
    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .and(query_param("since", "1609459320567800000"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_trades_response(
                "XXBTZUSD",
                vec![],
                "1609459320567800000",
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "XBTUSD".to_string(),
        start: Some(DateTime::from_timestamp(1609459200, 0).unwrap()),
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

    // Spot-check ordering across batches (oldest-first, Kraken returns this natively)
    assert_eq!(batch1[0].id, "391120170");
    assert_eq!(batch1[1].id, "391120171");
    assert_eq!(batch2[0].id, "391120172");
}

// ---------------------------------------------------------------------------
// Test 5: trades are returned in chronological order (oldest-first)
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_oldest_first_ordering() {
    let (mock_server, client) = setup().await;

    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_trades_response(
                "XXBTZUSD",
                three_trades_json(),
                "1609459320567800000",
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "XBTUSD".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    // Kraken returns oldest-first natively, so no reversal needed.
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

// ---------------------------------------------------------------------------
// Test 6: response with a different pair key (e.g. XETHZUSD) still works
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_fetch_trades_dynamic_pair_key() {
    let (mock_server, client) = setup().await;

    // Kraken response may use a different key than the requested pair
    Mock::given(method("GET"))
        .and(path("/0/public/Trades"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(kraken_trades_response(
                "XETHZUSD",
                vec![
                    json!(["1200.50000", "2.50000000", 1609459200.1234, "s", "l", "", 500000001_u64]),
                ],
                "1609459200123400000",
            )),
        )
        .expect(1)
        .mount(&mock_server)
        .await;

    let request = TradeRequest {
        market: "ETHUSD".to_string(),
        start: None,
        end: None,
        limit: None,
    };

    let trades = client.fetch_trades(request).await.unwrap();

    assert_eq!(trades.len(), 1);
    assert_eq!(trades[0].id, "500000001");
    assert!((trades[0].price - 1200.5).abs() < 1e-2);
    assert!((trades[0].amount - 2.5).abs() < 1e-6);
    assert_eq!(trades[0].side, barter_instrument::Side::Sell);
}
