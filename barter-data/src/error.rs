use crate::subscription::SubKind;
use barter_instrument::{exchange::ExchangeId, index::error::IndexError};
use barter_integration::{error::SocketError, subscription::SubscriptionId};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// All errors generated in `barter-data`.
#[non_exhaustive]
#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Error)]
pub enum DataError {
    #[error("failed to index market data Subscriptions: {0}")]
    Index(#[from] IndexError),

    #[error("failed to initialise reconnecting MarketStream due to empty subscriptions")]
    SubscriptionsEmpty,

    #[error("connection task terminated before signaling init result")]
    ConnectionTaskTerminated,

    #[error("unsupported DynamicStreams Subscription SubKind: {0}")]
    UnsupportedSubKind(SubKind),

    #[error("initial snapshot missing for: {0}")]
    InitialSnapshotMissing(SubscriptionId),

    #[error("initial snapshot invalid: {0}")]
    InitialSnapshotInvalid(String),

    #[error("command channel closed")]
    CommandChannelClosed,

    #[error("SocketError: {0}")]
    Socket(String),

    #[error("unsupported dynamic Subscription for exchange: {exchange}, kind: {sub_kind}")]
    Unsupported {
        exchange: ExchangeId,
        sub_kind: SubKind,
    },

    #[error("no connection for {exchange} {sub_kind}")]
    NoConnection {
        exchange: ExchangeId,
        sub_kind: SubKind,
    },

    #[error(
        "subscription batch mismatch: expected ({expected_exchange}, {expected_sub_kind}), got ({actual_exchange}, {actual_sub_kind})"
    )]
    SubscriptionMismatch {
        expected_exchange: ExchangeId,
        expected_sub_kind: SubKind,
        actual_exchange: ExchangeId,
        actual_sub_kind: SubKind,
    },

    #[error(
        "\
        InvalidSequence: first_update_id {first_update_id} does not follow on from the \
        prev_last_update_id {prev_last_update_id} \
    "
    )]
    InvalidSequence {
        prev_last_update_id: u64,
        first_update_id: u64,
    },

    /// HTTP transport error (request send failure, body read failure, non-2xx status).
    /// `status` is `Some` when an HTTP response was received; `None` for
    /// connection-level failures where no response arrived.
    /// `url` captures which endpoint failed.
    #[error("HTTP error (status={}) for {url}: {message}", status.map_or("none".to_string(), |s| s.to_string()))]
    Http {
        status: Option<u16>,
        url: String,
        message: String,
    },

    /// Exchange API returned a structured error response (HTTP 200 with error payload).
    #[error("{exchange} API error (code {code}): {message}")]
    ExchangeApi {
        exchange: String,
        code: String,
        message: String,
    },

    /// Data parsing failure (CSV, JSON, timestamp, numeric conversion).
    #[error("data parse error: {0}")]
    DataParse(String),

    /// Archive decompression or extraction failure (ZIP, LZ4, gzip).
    #[error("bulk archive error: {0}")]
    BulkArchive(String),

    /// SHA-256 checksum mismatch on a downloaded archive.
    #[error("checksum mismatch: expected {expected}, actual {actual}")]
    ChecksumMismatch { expected: String, actual: String },

    /// Exchange does not support the requested interval.
    #[error("{exchange} does not support interval: {interval}")]
    UnsupportedInterval {
        exchange: String,
        interval: String,
    },

    /// Local filesystem I/O error (distinct from archive decompression).
    #[error("I/O error: {0}")]
    Io(String),

    /// Pagination safety limit exceeded.
    #[error("{exchange} pagination limit exceeded ({limit}) for {market}")]
    PaginationLimit {
        exchange: String,
        market: String,
        limit: usize,
    },
}

impl DataError {
    /// Determine if an error requires a [`MarketStream`](super::MarketStream) to re-initialise.
    #[allow(clippy::match_like_matches_macro)]
    pub fn is_terminal(&self) -> bool {
        match self {
            DataError::InvalidSequence { .. } => true,
            _ => false,
        }
    }
}

impl From<SocketError> for DataError {
    fn from(value: SocketError) -> Self {
        match value {
            SocketError::Http(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("HTTP error: {e}"),
            },
            SocketError::HttpTimeout(e) => DataError::Http {
                status: None,
                url: String::new(),
                message: format!("request timeout: {e}"),
            },
            SocketError::HttpResponse(status, body) => DataError::Http {
                status: Some(status.as_u16()),
                url: String::new(),
                message: body,
            },
            SocketError::Deserialise { error, .. } => DataError::DataParse(error.to_string()),
            SocketError::DeserialiseBinary { error, .. } => {
                DataError::DataParse(error.to_string())
            }
            SocketError::DeserialiseProtobuf { error, .. } => {
                DataError::DataParse(error.to_string())
            }
            SocketError::Serialise(e) => DataError::DataParse(e.to_string()),
            // Genuine socket/WS/transport errors — keep as Socket
            SocketError::Sink => DataError::Socket("Sink error".into()),
            SocketError::QueryParams(e) => DataError::Socket(format!("query params error: {e}")),
            SocketError::UrlEncoded(e) => DataError::Socket(format!("url encoding error: {e}")),
            SocketError::UrlParse(e) => DataError::Socket(format!("url parse error: {e}")),
            SocketError::Subscribe(e) => DataError::Socket(format!("subscribe error: {e}")),
            SocketError::Terminated(e) => DataError::Socket(format!("terminated: {e}")),
            SocketError::Unsupported { entity, item } => {
                DataError::Socket(format!("{entity} does not support: {item}"))
            }
            SocketError::WebSocket(e) => DataError::Socket(format!("WebSocket error: {e}")),
            SocketError::Unidentifiable(id) => {
                DataError::Socket(format!("unidentifiable message: {id}"))
            }
            SocketError::Exchange(e) => DataError::Socket(format!("exchange error: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_error_is_terminal() {
        struct TestCase {
            input: DataError,
            expected: bool,
        }

        let tests = vec![
            TestCase {
                // TC0: is terminal w/ DataError::InvalidSequence
                input: DataError::InvalidSequence {
                    prev_last_update_id: 0,
                    first_update_id: 0,
                },
                expected: true,
            },
            TestCase {
                // TC1: is not terminal w/ DataError::Socket
                input: DataError::from(SocketError::Sink),
                expected: false,
            },
        ];

        for (index, test) in tests.into_iter().enumerate() {
            let actual = test.input.is_terminal();
            assert_eq!(actual, test.expected, "TC{} failed", index);
        }
    }
}
