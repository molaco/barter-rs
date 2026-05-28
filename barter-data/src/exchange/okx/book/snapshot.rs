use crate::{
    SnapshotFetcher,
    books::OrderBook,
    event::MarketEvent,
    exchange::{Connector, ExchangeServer, okx::Okx},
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
use std::future::Future;

use super::OkxLevel;

/// OKX HTTP OrderBook L2 snapshot url.
///
/// See docs: <https://www.okx.com/docs-v5/en/#order-book-trading-market-data-get-order-book>
const HTTP_BOOK_L2_SNAPSHOT_URL: &str = "https://www.okx.com/api/v5/market/books";

#[derive(Debug)]
pub struct OkxOrderBooksL2SnapshotFetcher;

impl<Server> SnapshotFetcher<Okx<Server>, OrderBooksL2> for OkxOrderBooksL2SnapshotFetcher
where
    Server: ExchangeServer + Send + Sync,
{
    fn fetch_snapshots<Instrument>(
        subscriptions: &[Subscription<Okx<Server>, Instrument, OrderBooksL2>],
    ) -> impl Future<Output = Result<Vec<MarketEvent<Instrument::Key, OrderBookEvent>>, SocketError>>
    + Send
    where
        Instrument: InstrumentData,
    {
        let snapshot_futures = subscriptions.iter().map(|subscription| {
            let market = Okx::<Server>::resolve_market(
                subscription.instrument.market_input(),
                &SubKind::OrderBooksL2,
            );
            let snapshot_url = format!(
                "{}?instId={}&sz=50",
                HTTP_BOOK_L2_SNAPSHOT_URL,
                market.as_ref(),
            );

            async move {
                let resp = reqwest::get(&snapshot_url)
                    .await
                    .map_err(SocketError::Http)?
                    .json::<OkxRestResponse>()
                    .await
                    .map_err(SocketError::Http)?;

                let snapshot =
                    resp.data
                        .into_iter()
                        .next()
                        .ok_or_else(|| SocketError::Deserialise {
                            error: serde::de::Error::custom("empty OKX books response"),
                            payload: String::new(),
                        })?;

                let time_received = Utc::now();

                Ok(MarketEvent {
                    time_exchange: snapshot.time,
                    time_received,
                    exchange: Server::ID,
                    instrument: subscription.instrument.key().clone(),
                    kind: OrderBookEvent::Snapshot(OrderBook::new(
                        snapshot.seq_id,
                        Some(snapshot.time),
                        snapshot.bids,
                        snapshot.asks,
                    )),
                })
            }
        });

        try_join_all(snapshot_futures)
    }
}

/// OKX REST orderbook response wrapper.
#[derive(Debug, Deserialize)]
struct OkxRestResponse {
    data: Vec<OkxRestSnapshot>,
}

/// OKX REST orderbook snapshot payload.
#[derive(Debug, Deserialize)]
struct OkxRestSnapshot {
    asks: Vec<OkxLevel>,
    bids: Vec<OkxLevel>,
    #[serde(
        rename = "ts",
        deserialize_with = "barter_integration::de::de_str_u64_epoch_ms_as_datetime_utc"
    )]
    time: chrono::DateTime<Utc>,
    #[serde(rename = "seqId", default)]
    seq_id: u64,
}
