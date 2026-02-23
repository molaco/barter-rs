use super::{channel::HyperliquidChannel, market::HyperliquidMarket};
use crate::exchange::subscription::ExchangeSub;
use barter_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize, Serializer, ser::SerializeStruct};

/// [`Hyperliquid`](super::Hyperliquid) WebSocket subscription response.
///
/// ### Raw Payload Examples
/// See docs: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>
/// #### Subscription Success
/// ```json
/// {
///     "channel": "subscriptionResponse",
///     "data": { "method": "subscribe", "subscription": { "type": "trades", "coin": "BTC" } }
/// }
/// ```
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct HyperliquidSubResponse {
    pub channel: String,
}

impl Validator for HyperliquidSubResponse {
    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        if self.channel == "subscriptionResponse" {
            Ok(self)
        } else {
            Err(SocketError::Subscribe(format!(
                "received non-subscription response: {}",
                self.channel
            )))
        }
    }
}

// Implement custom Serialize to produce Hyperliquid subscription payloads.
//
// For trades subscriptions:
// ```json
// { "type": "trades", "coin": "BTC" }
// ```
//
// For candle subscriptions:
// ```json
// { "type": "candle", "coin": "BTC", "interval": "1m" }
// ```
impl Serialize for ExchangeSub<HyperliquidChannel, HyperliquidMarket> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let channel = self.channel.as_ref();

        if let Some(interval) = channel.strip_prefix("candle.") {
            let mut state = serializer.serialize_struct("HyperliquidSub", 3)?;
            state.serialize_field("type", "candle")?;
            state.serialize_field("coin", self.market.as_ref())?;
            state.serialize_field("interval", interval)?;
            state.end()
        } else {
            let mut state = serializer.serialize_struct("HyperliquidSub", 2)?;
            state.serialize_field("type", channel)?;
            state.serialize_field("coin", self.market.as_ref())?;
            state.end()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod de {
        use super::*;

        #[test]
        fn test_hyperliquid_sub_response() {
            struct TestCase {
                input: &'static str,
                expected: Result<HyperliquidSubResponse, SocketError>,
            }

            let cases = vec![
                TestCase {
                    // TC0: input response is a subscription response
                    input: r#"
                    {
                        "channel": "subscriptionResponse",
                        "data": { "method": "subscribe", "subscription": { "type": "trades", "coin": "BTC" } }
                    }
                    "#,
                    expected: Ok(HyperliquidSubResponse {
                        channel: "subscriptionResponse".to_string(),
                    }),
                },
                TestCase {
                    // TC1: input response is a non-subscription response (trades data)
                    input: r#"
                    {
                        "channel": "trades",
                        "data": []
                    }
                    "#,
                    expected: Ok(HyperliquidSubResponse {
                        channel: "trades".to_string(),
                    }),
                },
            ];

            for (index, test) in cases.into_iter().enumerate() {
                let actual = serde_json::from_str::<HyperliquidSubResponse>(test.input);
                match (actual, test.expected) {
                    (Ok(actual), Ok(expected)) => {
                        assert_eq!(actual, expected, "TC{} failed", index)
                    }
                    (Err(_), Err(_)) => {
                        // Test passed
                    }
                    (actual, expected) => {
                        // Test failed
                        panic!(
                            "TC{index} failed because actual != expected. \nActual: {actual:?}\nExpected: {expected:?}\n"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_validate_hyperliquid_sub_response() {
        struct TestCase {
            input_response: HyperliquidSubResponse,
            is_valid: bool,
        }

        let cases = vec![
            TestCase {
                // TC0: input response is successful subscription
                input_response: HyperliquidSubResponse {
                    channel: "subscriptionResponse".to_string(),
                },
                is_valid: true,
            },
            TestCase {
                // TC1: input response is not a subscription response
                input_response: HyperliquidSubResponse {
                    channel: "trades".to_string(),
                },
                is_valid: false,
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            let actual = test.input_response.validate().is_ok();
            assert_eq!(actual, test.is_valid, "TestCase {} failed", index);
        }
    }

    mod ser {
        use super::*;
        use smol_str::SmolStr;

        #[test]
        fn test_serialize_trades_sub() {
            let sub = ExchangeSub {
                channel: HyperliquidChannel(SmolStr::new_static("trades")),
                market: HyperliquidMarket(SmolStr::new_static("BTC")),
            };

            let json = serde_json::to_value(&sub).unwrap();
            assert_eq!(json["type"], "trades");
            assert_eq!(json["coin"], "BTC");
            assert!(!json.as_object().unwrap().contains_key("interval"));
        }

        #[test]
        fn test_serialize_candle_sub() {
            use smol_str::format_smolstr;

            let sub = ExchangeSub {
                channel: HyperliquidChannel(format_smolstr!("candle.1m")),
                market: HyperliquidMarket(SmolStr::new_static("ETH")),
            };

            let json = serde_json::to_value(&sub).unwrap();
            assert_eq!(json["type"], "candle");
            assert_eq!(json["coin"], "ETH");
            assert_eq!(json["interval"], "1m");
        }
    }
}
