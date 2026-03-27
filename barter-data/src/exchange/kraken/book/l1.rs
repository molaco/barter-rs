use super::super::channel::KrakenChannel;
use super::super::message::KrakenMessage;
use crate::{
    Identifier,
    books::Level,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::book::{OrderBookL1, OrderBooksL1},
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Terse type alias for a Kraken v2 ticker (L1 best bid/ask) WebSocket message.
pub type KrakenOrderBookL1 = KrakenMessage<KrakenTickerPayload>;

/// Kraken v2 ticker data message.
///
/// ### Raw Payload Example
/// ```json
/// {
///     "channel": "ticker",
///     "type": "update",
///     "data": [{
///         "symbol": "BTC/USD",
///         "bid": 66650.0,
///         "bid_qty": 740.0,
///         "ask": 66651.0,
///         "ask_qty": 1361.44,
///         "timestamp": "2026-03-27T09:04:31.742648Z"
///     }]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenTickerPayload {
    pub channel: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: Vec<KrakenTickerData>,
}

impl Identifier<Option<SubscriptionId>> for KrakenTickerPayload {
    fn id(&self) -> Option<SubscriptionId> {
        self.data.first().map(|t| {
            ExchangeSub::from((KrakenChannel::TICKER, &t.symbol)).id()
        })
    }
}

/// A single Kraken v2 ticker snapshot.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenTickerData {
    pub symbol: String,
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
    #[serde(default)]
    pub last: f64,
    pub timestamp: DateTime<Utc>,
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, KrakenOrderBookL1)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from(
        (exchange, instrument, msg): (ExchangeId, InstrumentKey, KrakenOrderBookL1),
    ) -> Self {
        match msg {
            KrakenOrderBookL1::Data(payload) => payload
                .data
                .into_iter()
                .map(|ticker| {
                    Ok(MarketEvent {
                        time_exchange: ticker.timestamp,
                        time_received: Utc::now(),
                        exchange,
                        instrument: instrument.clone(),
                        kind: OrderBookL1 {
                            last_update_time: ticker.timestamp,
                            best_bid: Some(Level {
                                price: Decimal::from_f64(ticker.bid).unwrap_or_default(),
                                amount: Decimal::from_f64(ticker.bid_qty).unwrap_or_default(),
                            }),
                            best_ask: Some(Level {
                                price: Decimal::from_f64(ticker.ask).unwrap_or_default(),
                                amount: Decimal::from_f64(ticker.ask_qty).unwrap_or_default(),
                            }),
                        },
                    })
                })
                .collect(),
            KrakenOrderBookL1::Event(_) => Self(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_v2_ticker_deserialize() {
        let input = r#"{
            "channel": "ticker",
            "type": "snapshot",
            "data": [{
                "symbol": "BTC/USD",
                "bid": 66650.0,
                "bid_qty": 740.0,
                "ask": 66651.0,
                "ask_qty": 1361.44,
                "last": 66650.5,
                "volume": 997038.98,
                "vwap": 66648.0,
                "low": 65000.0,
                "high": 67000.0,
                "change": -17.0,
                "change_pct": -0.03,
                "timestamp": "2026-03-27T09:04:31.742648Z"
            }]
        }"#;
        let msg: KrakenOrderBookL1 = serde_json::from_str(input).unwrap();
        match msg {
            KrakenOrderBookL1::Data(payload) => {
                assert_eq!(payload.data.len(), 1);
                assert_eq!(payload.data[0].bid, 66650.0);
                assert_eq!(payload.data[0].ask, 66651.0);
            }
            _ => panic!("expected Data variant"),
        }
    }
}
