use super::{SubKind, SubscriptionKind};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Barter [`Subscription`](super::Subscription) [`SubscriptionKind`] that yields [`Candle`]
/// [`MarketEvent<T>`](crate::event::MarketEvent) events.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct Candles(pub Interval);

impl Default for Candles {
    fn default() -> Self {
        Self(Interval::M1)
    }
}

impl SubscriptionKind for Candles {
    type Event = Candle;

    fn as_str(&self) -> &'static str {
        "candles"
    }

    fn as_sub_kind(&self) -> SubKind {
        SubKind::Candles(self.0)
    }
}

impl std::fmt::Display for Candles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "candles_{}", self.0)
    }
}

/// Normalised Barter OHLCV [`Candle`] model.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct Candle {
    pub open_time: DateTime<Utc>,
    pub close_time: DateTime<Utc>,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: Option<f64>,
    pub trade_count: u64,
    pub is_closed: bool,
}

/// Normalised candlestick interval period.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub enum Interval {
    M1,
    M3,
    M5,
    M15,
    M30,
    H1,
    H2,
    H4,
    H6,
    H12,
    D1,
    D3,
    W1,
    Month1,
}

impl std::fmt::Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Interval::M1 => write!(f, "1m"),
            Interval::M3 => write!(f, "3m"),
            Interval::M5 => write!(f, "5m"),
            Interval::M15 => write!(f, "15m"),
            Interval::M30 => write!(f, "30m"),
            Interval::H1 => write!(f, "1h"),
            Interval::H2 => write!(f, "2h"),
            Interval::H4 => write!(f, "4h"),
            Interval::H6 => write!(f, "6h"),
            Interval::H12 => write!(f, "12h"),
            Interval::D1 => write!(f, "1d"),
            Interval::D3 => write!(f, "3d"),
            Interval::W1 => write!(f, "1w"),
            Interval::Month1 => write!(f, "1M"),
        }
    }
}
