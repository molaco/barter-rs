use crate::{
    SnapshotFetcher,
    books::OrderBook,
    event::MarketEvent,
    exchange::{Connector, kraken::Kraken},
    instrument::InstrumentData,
    subscription::{
        SubKind, Subscription,
        book::{OrderBookEvent, OrderBooksL2},
    },
};
use barter_integration::error::SocketError;
use chrono::Utc;
use futures_util::future::try_join_all;
use serde::Deserialize;
use std::collections::HashMap;
use std::future::Future;

use super::l2::KrakenBookLevel;

/// Kraken REST OrderBook snapshot url.
///
/// Note: REST API uses different pair format (e.g. "BTCUSD" not "BTC/USD").
/// We strip the slash from the WS market name.
///
/// See docs: <https://docs.kraken.com/api/docs/rest-api/get-order-book>
const HTTP_BOOK_SNAPSHOT_URL: &str = "https://api.kraken.com/0/public/Depth";

#[derive(Debug)]
pub struct KrakenOrderBooksL2SnapshotFetcher;

impl SnapshotFetcher<Kraken, OrderBooksL2> for KrakenOrderBooksL2SnapshotFetcher {
    fn fetch_snapshots<Instrument>(
        subscriptions: &[Subscription<Kraken, Instrument, OrderBooksL2>],
    ) -> impl Future<Output = Result<Vec<MarketEvent<Instrument::Key, OrderBookEvent>>, SocketError>>
    + Send
    where
        Instrument: InstrumentData,
    {
        let snapshot_futures = subscriptions.iter().map(|subscription| {
            let market = Kraken::resolve_market(
                subscription.instrument.market_input(),
                &SubKind::OrderBooksL2,
            );
            // REST uses pair without slash: "BTC/USD" → "BTCUSD"
            let rest_pair = market.as_ref().replace('/', "");
            let snapshot_url = format!(
                "{}?pair={}&count=25",
                HTTP_BOOK_SNAPSHOT_URL,
                rest_pair,
            );

            async move {
                let resp = reqwest::get(&snapshot_url)
                    .await
                    .map_err(SocketError::Http)?
                    .json::<KrakenRestResponse>()
                    .await
                    .map_err(SocketError::Http)?;

                let (_, book) = resp
                    .result
                    .into_iter()
                    .next()
                    .ok_or_else(|| SocketError::Deserialise {
                        error: serde::de::Error::custom("empty Kraken depth response"),
                        payload: String::new(),
                    })?;

                let time_received = Utc::now();

                // Convert REST levels to KrakenBookLevel
                let bids: Vec<KrakenBookLevel> = book.bids.iter().map(|l| KrakenBookLevel {
                    price: l.price.parse().unwrap_or(0.0),
                    qty: l.volume.parse().unwrap_or(0.0),
                }).collect();
                let asks: Vec<KrakenBookLevel> = book.asks.iter().map(|l| KrakenBookLevel {
                    price: l.price.parse().unwrap_or(0.0),
                    qty: l.volume.parse().unwrap_or(0.0),
                }).collect();

                Ok(MarketEvent {
                    time_exchange: time_received,
                    time_received,
                    exchange: Kraken::ID,
                    instrument: subscription.instrument.key().clone(),
                    kind: OrderBookEvent::Snapshot(OrderBook::new(
                        0, // Kraken REST has no sequence number
                        None,
                        bids,
                        asks,
                    )),
                })
            }
        });

        try_join_all(snapshot_futures)
    }
}

/// Kraken REST depth response wrapper.
#[derive(Debug, Deserialize)]
struct KrakenRestResponse {
    #[serde(default)]
    error: Vec<String>,
    result: HashMap<String, KrakenRestBook>,
}

/// Kraken REST orderbook for a single pair.
#[derive(Debug, Deserialize)]
struct KrakenRestBook {
    asks: Vec<KrakenRestLevel>,
    bids: Vec<KrakenRestLevel>,
}

/// Kraken REST level: ["price", "volume", timestamp_int].
#[derive(Debug)]
struct KrakenRestLevel {
    price: String,
    volume: String,
    #[allow(dead_code)]
    timestamp: u64,
}

/// Custom deserialize for Kraken REST levels (3-element arrays).
impl<'de> serde::de::Deserialize<'de> for KrakenRestLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Levels come as ["price_str", "volume_str", timestamp_int]
        let arr: (String, String, u64) = serde::Deserialize::deserialize(deserializer)?;
        Ok(KrakenRestLevel {
            price: arr.0,
            volume: arr.1,
            timestamp: arr.2,
        })
    }
}
