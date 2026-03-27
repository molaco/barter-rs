use crate::{
    SnapshotFetcher,
    books::OrderBook,
    event::MarketEvent,
    exchange::{Connector, ExchangeServer, bybit::Bybit},
    instrument::InstrumentData,
    subscription::{
        SubKind, Subscription,
        book::{OrderBookEvent, OrderBooksL2},
    },
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::error::SocketError;
use chrono::Utc;
use futures_util::future::try_join_all;
use serde::Deserialize;
use std::future::Future;

use super::BybitLevel;

/// Bybit HTTP OrderBook L2 snapshot url.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/market/orderbook>
const HTTP_BOOK_L2_SNAPSHOT_URL: &str = "https://api.bybit.com/v5/market/orderbook";

/// Resolve the Bybit API category from an [`ExchangeId`].
fn category_for(id: ExchangeId) -> &'static str {
    match id {
        ExchangeId::BybitSpot => "spot",
        ExchangeId::BybitPerpetualsUsd => "linear",
        _ => "linear",
    }
}

#[derive(Debug)]
pub struct BybitOrderBooksL2SnapshotFetcher;

impl<Server> SnapshotFetcher<Bybit<Server>, OrderBooksL2> for BybitOrderBooksL2SnapshotFetcher
where
    Server: ExchangeServer + Send + Sync,
{
    fn fetch_snapshots<Instrument>(
        subscriptions: &[Subscription<Bybit<Server>, Instrument, OrderBooksL2>],
    ) -> impl Future<Output = Result<Vec<MarketEvent<Instrument::Key, OrderBookEvent>>, SocketError>>
    + Send
    where
        Instrument: InstrumentData,
    {
        let category = category_for(Server::ID);

        let snapshot_futures = subscriptions.iter().map(|subscription| {
            let market = Bybit::<Server>::resolve_market(
                subscription.instrument.market_input(),
                &SubKind::OrderBooksL2,
            );
            let snapshot_url = format!(
                "{}?category={}&symbol={}&limit=50",
                HTTP_BOOK_L2_SNAPSHOT_URL,
                category,
                market.as_ref(),
            );

            async move {
                let resp = reqwest::get(&snapshot_url)
                    .await
                    .map_err(SocketError::Http)?
                    .json::<BybitRestResponse>()
                    .await
                    .map_err(SocketError::Http)?;

                let snapshot = resp.result;
                let time_received = Utc::now();

                Ok(MarketEvent {
                    time_exchange: time_received,
                    time_received,
                    exchange: Server::ID,
                    instrument: subscription.instrument.key().clone(),
                    kind: OrderBookEvent::Snapshot(OrderBook::new(
                        snapshot.update_id,
                        None,
                        snapshot.bids,
                        snapshot.asks,
                    )),
                })
            }
        });

        try_join_all(snapshot_futures)
    }
}

/// Bybit REST orderbook response wrapper.
///
/// See docs: <https://bybit-exchange.github.io/docs/v5/market/orderbook>
#[derive(Debug, Deserialize)]
struct BybitRestResponse {
    result: BybitOrderBookL2Snapshot,
}

/// Bybit REST orderbook snapshot payload.
#[derive(Debug, Deserialize)]
struct BybitOrderBookL2Snapshot {
    #[serde(rename = "b")]
    bids: Vec<BybitLevel>,
    #[serde(rename = "a")]
    asks: Vec<BybitLevel>,
    #[serde(rename = "u")]
    update_id: u64,
    #[serde(default)]
    seq: u64,
}
