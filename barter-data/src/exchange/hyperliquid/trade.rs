use super::channel::HyperliquidChannel;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use barter_instrument::{Side, exchange::ExchangeId};
use barter_integration::{de::datetime_utc_from_epoch_duration, subscription::SubscriptionId};
use chrono::Utc;
use serde::{Deserialize, Deserializer, Serialize};
use smol_str::SmolStr;
use std::time::Duration;

/// [`Hyperliquid`](super::Hyperliquid) real-time trades WebSocket message.
///
/// Wraps a batch of [`HyperliquidTrade`]s along with the derived [`SubscriptionId`].
///
/// ### Raw Payload Examples
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
/// ```json
/// {
///     "channel": "trades",
///     "data": [
///         {
///             "coin": "ETH",
///             "side": "B",
///             "px": "1850.5",
///             "sz": "0.1",
///             "time": 1672502400000,
///             "tid": 12345
///         }
///     ]
/// }
/// ```
#[derive(Clone, PartialEq, PartialOrd, Debug, Serialize)]
pub struct HyperliquidTrades {
    pub subscription_id: SubscriptionId,
    pub trades: Vec<HyperliquidTrade>,
}

/// Individual [`Hyperliquid`](super::Hyperliquid) trade within a [`HyperliquidTrades`] message.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct HyperliquidTrade {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: u64,
    pub tid: u64,
}

impl<'de> Deserialize<'de> for HyperliquidTrades {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Helper struct for deserializing the raw Hyperliquid trades message.
        #[derive(Deserialize)]
        struct RawHyperliquidTrades {
            #[allow(dead_code)]
            channel: String,
            data: Vec<HyperliquidTrade>,
        }

        let raw = RawHyperliquidTrades::deserialize(deserializer)?;

        // Derive subscription_id from the channel + first trade's coin
        let subscription_id = raw
            .data
            .first()
            .map(|trade| {
                ExchangeSub::from((
                    HyperliquidChannel(SmolStr::new_static("trades")),
                    trade.coin.as_str(),
                ))
                .id()
            })
            .unwrap_or_else(|| {
                ExchangeSub::from((HyperliquidChannel(SmolStr::new_static("trades")), "")).id()
            });

        Ok(HyperliquidTrades {
            subscription_id,
            trades: raw.data,
        })
    }
}

impl Identifier<Option<SubscriptionId>> for HyperliquidTrades {
    fn id(&self) -> Option<SubscriptionId> {
        Some(self.subscription_id.clone())
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, HyperliquidTrades)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from(
        (exchange, instrument, trades): (ExchangeId, InstrumentKey, HyperliquidTrades),
    ) -> Self {
        Self(
            trades
                .trades
                .into_iter()
                .filter_map(|trade| {
                    let side = match trade.side.as_str() {
                        "B" => Side::Buy,
                        "A" => Side::Sell,
                        _ => return None,
                    };

                    let price = trade.px.parse::<f64>().ok()?;
                    let amount = trade.sz.parse::<f64>().ok()?;

                    Some(Ok(MarketEvent {
                        time_exchange: datetime_utc_from_epoch_duration(Duration::from_millis(
                            trade.time,
                        )),
                        time_received: Utc::now(),
                        exchange,
                        instrument: instrument.clone(),
                        kind: PublicTrade {
                            id: trade.tid.to_string(),
                            price,
                            amount,
                            side,
                        },
                    }))
                })
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_hyperliquid_trades_deserialise() {
            let input = r#"
            {
                "channel": "trades",
                "data": [
                    {
                        "coin": "ETH",
                        "side": "B",
                        "px": "1850.5",
                        "sz": "0.1",
                        "time": 1672502400000,
                        "tid": 12345
                    }
                ]
            }
            "#;

            let actual = serde_json::from_str::<HyperliquidTrades>(input).unwrap();

            assert_eq!(
                actual.subscription_id,
                SubscriptionId::from("trades|ETH")
            );
            assert_eq!(actual.trades.len(), 1);
            assert_eq!(actual.trades[0].coin, "ETH");
            assert_eq!(actual.trades[0].side, "B");
            assert_eq!(actual.trades[0].px, "1850.5");
            assert_eq!(actual.trades[0].sz, "0.1");
            assert_eq!(actual.trades[0].time, 1672502400000);
            assert_eq!(actual.trades[0].tid, 12345);
        }

        #[test]
        fn test_hyperliquid_trades_subscription_id_extraction() {
            let input = r#"
            {
                "channel": "trades",
                "data": [
                    {
                        "coin": "BTC",
                        "side": "A",
                        "px": "42000.0",
                        "sz": "1.5",
                        "time": 1672502400000,
                        "tid": 99999
                    },
                    {
                        "coin": "BTC",
                        "side": "B",
                        "px": "42001.0",
                        "sz": "0.5",
                        "time": 1672502400001,
                        "tid": 100000
                    }
                ]
            }
            "#;

            let actual = serde_json::from_str::<HyperliquidTrades>(input).unwrap();

            // subscription_id derived from first trade's coin
            assert_eq!(
                actual.subscription_id,
                SubscriptionId::from("trades|BTC")
            );
            assert_eq!(actual.trades.len(), 2);
        }

        #[test]
        fn test_hyperliquid_trades_empty_data_array() {
            let input = r#"
            {
                "channel": "trades",
                "data": []
            }
            "#;

            let actual = serde_json::from_str::<HyperliquidTrades>(input).unwrap();

            // With no trades, subscription_id falls back to "trades|"
            assert_eq!(
                actual.subscription_id,
                SubscriptionId::from("trades|")
            );
            assert!(actual.trades.is_empty());
        }

        #[test]
        fn test_hyperliquid_trades_missing_field_error() {
            // Missing "data" field entirely
            let input = r#"
            {
                "channel": "trades"
            }
            "#;

            let result = serde_json::from_str::<HyperliquidTrades>(input);
            assert!(result.is_err());
        }

        #[test]
        fn test_hyperliquid_trade_missing_required_field() {
            // Missing "tid" field in a trade
            let input = r#"
            {
                "channel": "trades",
                "data": [
                    {
                        "coin": "ETH",
                        "side": "B",
                        "px": "1850.5",
                        "sz": "0.1",
                        "time": 1672502400000
                    }
                ]
            }
            "#;

            let result = serde_json::from_str::<HyperliquidTrades>(input);
            assert!(result.is_err());
        }
    }

    mod conversion {
        use super::*;

        #[test]
        fn test_hyperliquid_trades_to_public_trades() {
            let trades = HyperliquidTrades {
                subscription_id: SubscriptionId::from("trades|ETH"),
                trades: vec![
                    HyperliquidTrade {
                        coin: "ETH".to_string(),
                        side: "B".to_string(),
                        px: "1850.5".to_string(),
                        sz: "0.1".to_string(),
                        time: 1672502400000,
                        tid: 12345,
                    },
                    HyperliquidTrade {
                        coin: "ETH".to_string(),
                        side: "A".to_string(),
                        px: "1851.0".to_string(),
                        sz: "0.2".to_string(),
                        time: 1672502400001,
                        tid: 12346,
                    },
                ],
            };

            let instrument = "ETH-USDT";
            let market_iter = MarketIter::<&str, PublicTrade>::from((
                ExchangeId::Hyperliquid,
                instrument,
                trades,
            ));

            assert_eq!(market_iter.0.len(), 2);

            let event0 = market_iter.0[0].as_ref().unwrap();
            assert_eq!(event0.exchange, ExchangeId::Hyperliquid);
            assert_eq!(event0.instrument, "ETH-USDT");
            assert_eq!(event0.kind.id, "12345");
            assert_eq!(event0.kind.price, 1850.5);
            assert_eq!(event0.kind.amount, 0.1);
            assert_eq!(event0.kind.side, Side::Buy);
            assert_eq!(
                event0.time_exchange,
                datetime_utc_from_epoch_duration(Duration::from_millis(1672502400000))
            );

            let event1 = market_iter.0[1].as_ref().unwrap();
            assert_eq!(event1.kind.id, "12346");
            assert_eq!(event1.kind.price, 1851.0);
            assert_eq!(event1.kind.amount, 0.2);
            assert_eq!(event1.kind.side, Side::Sell);
        }

        #[test]
        fn test_hyperliquid_trades_unknown_side_skipped() {
            let trades = HyperliquidTrades {
                subscription_id: SubscriptionId::from("trades|ETH"),
                trades: vec![
                    HyperliquidTrade {
                        coin: "ETH".to_string(),
                        side: "X".to_string(),
                        px: "1850.5".to_string(),
                        sz: "0.1".to_string(),
                        time: 1672502400000,
                        tid: 12345,
                    },
                    HyperliquidTrade {
                        coin: "ETH".to_string(),
                        side: "B".to_string(),
                        px: "1851.0".to_string(),
                        sz: "0.2".to_string(),
                        time: 1672502400001,
                        tid: 12346,
                    },
                ],
            };

            let market_iter = MarketIter::<&str, PublicTrade>::from((
                ExchangeId::Hyperliquid,
                "ETH-USDT",
                trades,
            ));

            // Only one trade should be present (the one with unknown side "X" is skipped)
            assert_eq!(market_iter.0.len(), 1);
            assert_eq!(market_iter.0[0].as_ref().unwrap().kind.side, Side::Buy);
        }

        #[test]
        fn test_hyperliquid_trades_empty_conversion() {
            let trades = HyperliquidTrades {
                subscription_id: SubscriptionId::from("trades|ETH"),
                trades: vec![],
            };

            let market_iter = MarketIter::<&str, PublicTrade>::from((
                ExchangeId::Hyperliquid,
                "ETH-USDT",
                trades,
            ));

            assert!(market_iter.0.is_empty());
        }
    }
}
