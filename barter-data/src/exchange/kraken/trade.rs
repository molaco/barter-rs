use super::{channel::KrakenChannel, message::KrakenMessage};
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Terse type alias for a Kraken v2 trades WebSocket message.
pub type KrakenTrades = KrakenMessage<KrakenTradesPayload>;

/// Kraken v2 trades data message.
///
/// ### Raw Payload Example
/// ```json
/// {
///     "channel": "trade",
///     "type": "update",
///     "data": [{
///         "symbol": "BTC/USD",
///         "side": "buy",
///         "price": 66650.0,
///         "qty": 0.001,
///         "ord_type": "limit",
///         "trade_id": 4665846,
///         "timestamp": "2026-03-27T07:48:36.925533Z"
///     }]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenTradesPayload {
    pub channel: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: Vec<KrakenTrade>,
}

impl Identifier<Option<SubscriptionId>> for KrakenTradesPayload {
    fn id(&self) -> Option<SubscriptionId> {
        self.data
            .first()
            .map(|t| ExchangeSub::from((KrakenChannel::TRADES, &t.symbol)).id())
    }
}

/// A single Kraken v2 trade.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenTrade {
    pub symbol: String,
    pub side: KrakenSide,
    pub price: f64,
    pub qty: f64,
    #[serde(default)]
    pub ord_type: String,
    #[serde(default)]
    pub trade_id: u64,
    pub timestamp: DateTime<Utc>,
}

/// Kraken v2 side: "buy" or "sell".
#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KrakenSide {
    Buy,
    Sell,
}

impl From<KrakenSide> for Side {
    fn from(side: KrakenSide) -> Self {
        match side {
            KrakenSide::Buy => Side::Buy,
            KrakenSide::Sell => Side::Sell,
        }
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, KrakenTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange, instrument, trades): (ExchangeId, InstrumentKey, KrakenTrades)) -> Self {
        match trades {
            KrakenTrades::Data(payload) => payload
                .data
                .into_iter()
                .map(|trade| {
                    Ok(MarketEvent {
                        time_exchange: trade.timestamp,
                        time_received: Utc::now(),
                        exchange,
                        instrument: instrument.clone(),
                        kind: PublicTrade {
                            id: trade.trade_id.to_string(),
                            price: trade.price,
                            amount: trade.qty,
                            side: trade.side.into(),
                        },
                    })
                })
                .collect(),
            KrakenTrades::Event(_) => Self(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_v2_trade_deserialize() {
        let input = r#"{
            "channel": "trade",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "side": "sell",
                "price": 66650.0,
                "qty": 0.001,
                "ord_type": "market",
                "trade_id": 4665846,
                "timestamp": "2026-03-27T07:48:36.925533Z"
            }]
        }"#;
        let msg: KrakenTrades = serde_json::from_str(input).unwrap();
        match msg {
            KrakenTrades::Data(payload) => {
                assert_eq!(payload.data.len(), 1);
                assert_eq!(payload.data[0].symbol, "BTC/USD");
                assert_eq!(payload.data[0].price, 66650.0);
                assert_eq!(payload.data[0].side, KrakenSide::Sell);
            }
            _ => panic!("expected Data variant"),
        }
    }
}
