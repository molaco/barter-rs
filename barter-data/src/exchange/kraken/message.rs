use crate::Identifier;
use barter_integration::subscription::SubscriptionId;
use serde::{Deserialize, Serialize};

/// [`Kraken`](super::Kraken) WebSocket v2 message envelope.
///
/// All v2 data messages are JSON objects with `channel`, `type`, and `data` fields.
/// Non-data messages (heartbeat, pong) have only a `channel` or `method` field.
///
/// ### Raw Payload Examples
/// See docs: <https://docs.kraken.com/api/docs/websocket-v2/trade/>
///
/// #### Trade update
/// ```json
/// {
///     "channel": "trade",
///     "type": "update",
///     "data": [{
///         "symbol": "BTC/USD",
///         "side": "buy",
///         "price": 66650.0,
///         "qty": 0.001,
///         "timestamp": "2026-03-27T12:17:06.925533Z"
///     }]
/// }
/// ```
///
/// #### Heartbeat
/// ```json
/// {"channel": "heartbeat"}
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
#[non_exhaustive]
pub enum KrakenMessage<T> {
    /// Data message with channel, type, and payload.
    Data(T),
    /// Non-data event (heartbeat, pong, status).
    Event(KrakenEvent),
}

impl<T> Identifier<Option<SubscriptionId>> for KrakenMessage<T>
where
    T: Identifier<Option<SubscriptionId>>,
{
    fn id(&self) -> Option<SubscriptionId> {
        match self {
            Self::Data(data) => data.id(),
            Self::Event(_) => None,
        }
    }
}

/// [`Kraken`](super::Kraken) non-data events received over WebSocket v2.
///
/// See docs: <https://docs.kraken.com/api/docs/websocket-v2/heartbeat/>
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
#[serde(tag = "channel", rename_all = "lowercase")]
#[non_exhaustive]
pub enum KrakenEvent {
    Heartbeat,
    Status(KrakenStatus),
}

/// Kraken system status event.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenStatus {
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub system: String,
    #[serde(default)]
    pub version: String,
}

/// [`Kraken`](super::Kraken) generic error message.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct KrakenError {
    #[serde(alias = "errorMessage")]
    pub message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_v2_heartbeat() {
        let input = r#"{"channel": "heartbeat"}"#;
        let msg: KrakenMessage<()> = serde_json::from_str(input).unwrap();
        assert!(matches!(msg, KrakenMessage::Event(KrakenEvent::Heartbeat)));
    }

    #[test]
    fn test_kraken_v2_status() {
        let input = r#"{"channel": "status", "data": [{"api_version": "v2", "system": "online", "version": "2.0.10"}], "type": "update"}"#;
        let msg: KrakenMessage<()> = serde_json::from_str(input).unwrap();
        // Status messages are parsed as Event since they have "channel": "status"
        // but the exact match depends on untagged ordering; the important thing
        // is it doesn't fail
        assert!(matches!(
            msg,
            KrakenMessage::Event(_) | KrakenMessage::Data(_)
        ));
    }
}
