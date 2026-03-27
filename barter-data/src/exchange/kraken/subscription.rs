use super::message::KrakenError;
use barter_integration::{Validator, error::SocketError};
use serde::{Deserialize, Serialize};

/// [`Kraken`](super::Kraken) v2 WebSocket subscription response.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.kraken.com/api/docs/websocket-v2/trade/>
///
/// #### Success
/// ```json
/// {
///     "method": "subscribe",
///     "result": {
///         "channel": "trade",
///         "snapshot": true,
///         "symbol": "BTC/USD"
///     },
///     "success": true,
///     "time_in": "2026-03-27T12:17:06.298588Z",
///     "time_out": "2026-03-27T12:17:06.298626Z"
/// }
/// ```
///
/// #### Failure
/// ```json
/// {
///     "error": "Currency pair not supported BTC/INVALID",
///     "method": "subscribe",
///     "success": false,
///     "time_in": "2026-03-27T12:17:06.298588Z",
///     "time_out": "2026-03-27T12:17:06.298626Z"
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenSubResponse {
    pub method: String,
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub result: Option<KrakenSubResult>,
}

/// The `result` field of a successful v2 subscription response.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenSubResult {
    pub channel: String,
    #[serde(default)]
    pub symbol: Option<String>,
}

impl Validator for KrakenSubResponse {
    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        if self.success {
            Ok(self)
        } else {
            let msg = self
                .error
                .unwrap_or_else(|| "unknown subscription error".into());
            Err(SocketError::Subscribe(format!(
                "Kraken subscription failed: {msg}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_v2_sub_response_success() {
        let input = r#"{
            "method": "subscribe",
            "result": {
                "channel": "trade",
                "snapshot": true,
                "symbol": "BTC/USD"
            },
            "success": true,
            "time_in": "2026-03-27T12:17:06.298588Z",
            "time_out": "2026-03-27T12:17:06.298626Z"
        }"#;
        let resp: KrakenSubResponse = serde_json::from_str(input).unwrap();
        assert!(resp.success);
        assert!(resp.validate().is_ok());
    }

    #[test]
    fn test_kraken_v2_sub_response_failure() {
        let input = r#"{
            "error": "Currency pair not supported BTC/INVALID",
            "method": "subscribe",
            "success": false,
            "time_in": "2026-03-27T12:17:06.298588Z",
            "time_out": "2026-03-27T12:17:06.298626Z"
        }"#;
        let resp: KrakenSubResponse = serde_json::from_str(input).unwrap();
        assert!(!resp.success);
        assert!(resp.validate().is_err());
    }
}
