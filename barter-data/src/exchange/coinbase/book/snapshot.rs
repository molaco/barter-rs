use crate::{
    SnapshotFetcher,
    books::{Level, OrderBook},
    event::MarketEvent,
    exchange::{Connector, coinbase::Coinbase},
    instrument::InstrumentData,
    subscription::{
        SubKind, Subscription,
        book::{OrderBookEvent, OrderBooksL2},
    },
};
use barter_integration::error::SocketError;
use chrono::Utc;
use futures_util::future::try_join_all;
use rust_decimal::Decimal;
use serde::Deserialize;
use std::future::Future;

/// Coinbase REST OrderBook L2 snapshot url.
///
/// Returns full L2 book; we truncate to 50 levels per side.
///
/// See docs: <https://docs.cloud.coinbase.com/exchange/reference/exchangerestapi_getproductbook>
const HTTP_BOOK_L2_SNAPSHOT_URL: &str = "https://api.exchange.coinbase.com/products";

#[derive(Debug)]
pub struct CoinbaseOrderBooksL2SnapshotFetcher;

impl SnapshotFetcher<Coinbase, OrderBooksL2> for CoinbaseOrderBooksL2SnapshotFetcher {
    fn fetch_snapshots<Instrument>(
        subscriptions: &[Subscription<Coinbase, Instrument, OrderBooksL2>],
    ) -> impl Future<Output = Result<Vec<MarketEvent<Instrument::Key, OrderBookEvent>>, SocketError>>
    + Send
    where
        Instrument: InstrumentData,
    {
        let snapshot_futures = subscriptions.iter().map(|subscription| {
            let market = Coinbase::resolve_market(
                subscription.instrument.market_input(),
                &SubKind::OrderBooksL2,
            );
            let snapshot_url = format!(
                "{}/{}/book?level=2",
                HTTP_BOOK_L2_SNAPSHOT_URL,
                market.as_ref(),
            );

            async move {
                let resp = reqwest::get(&snapshot_url)
                    .await
                    .map_err(SocketError::Http)?
                    .json::<CoinbaseRestBook>()
                    .await
                    .map_err(SocketError::Http)?;

                let time_received = Utc::now();

                let bids: Vec<Level> = resp
                    .bids
                    .iter()
                    .take(50)
                    .filter_map(|l| {
                        let p: Decimal = l.price.parse().ok()?;
                        let s: Decimal = l.size.parse().ok()?;
                        Some(Level { price: p, amount: s })
                    })
                    .collect();

                let asks: Vec<Level> = resp
                    .asks
                    .iter()
                    .take(50)
                    .filter_map(|l| {
                        let p: Decimal = l.price.parse().ok()?;
                        let s: Decimal = l.size.parse().ok()?;
                        Some(Level { price: p, amount: s })
                    })
                    .collect();

                Ok(MarketEvent {
                    time_exchange: time_received,
                    time_received,
                    exchange: Coinbase::ID,
                    instrument: subscription.instrument.key().clone(),
                    kind: OrderBookEvent::Snapshot(OrderBook::new(
                        resp.sequence as u64,
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

/// Coinbase REST L2 orderbook response.
#[derive(Debug, Deserialize)]
struct CoinbaseRestBook {
    bids: Vec<CoinbaseRestLevel>,
    asks: Vec<CoinbaseRestLevel>,
    sequence: i64,
}

/// Coinbase REST level: ["price", "size", num_orders].
#[derive(Debug)]
struct CoinbaseRestLevel {
    price: String,
    size: String,
}

impl<'de> serde::de::Deserialize<'de> for CoinbaseRestLevel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Coinbase L2 levels: ["price", "size", num_orders]
        let arr: Vec<serde_json::Value> = serde::Deserialize::deserialize(deserializer)?;
        if arr.len() < 2 {
            return Err(serde::de::Error::custom("expected at least 2 elements"));
        }
        let price = arr[0].as_str().unwrap_or("0").to_string();
        let size = arr[1].as_str().unwrap_or("0").to_string();
        Ok(CoinbaseRestLevel { price, size })
    }
}
