use super::{SubKind, SubscriptionKind};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

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
        write!(f, "{}", self.as_str())
    }
}

impl Interval {
    /// Get the duration of this interval in milliseconds.
    pub const fn to_ms(&self) -> i64 {
        match self {
            Interval::M1 => 60_000,
            Interval::M3 => 180_000,
            Interval::M5 => 300_000,
            Interval::M15 => 900_000,
            Interval::M30 => 1_800_000,
            Interval::H1 => 3_600_000,
            Interval::H2 => 7_200_000,
            Interval::H4 => 14_400_000,
            Interval::H6 => 21_600_000,
            Interval::H12 => 43_200_000,
            Interval::D1 => 86_400_000,
            Interval::D3 => 259_200_000,
            Interval::W1 => 604_800_000,
            Interval::Month1 => 2_592_000_000,
        }
    }

    /// Get the duration of this interval in seconds.
    pub const fn to_seconds(&self) -> i64 {
        self.to_ms() / 1000
    }

    /// Get the canonical string representation.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Interval::M1 => "1m",
            Interval::M3 => "3m",
            Interval::M5 => "5m",
            Interval::M15 => "15m",
            Interval::M30 => "30m",
            Interval::H1 => "1h",
            Interval::H2 => "2h",
            Interval::H4 => "4h",
            Interval::H6 => "6h",
            Interval::H12 => "12h",
            Interval::D1 => "1d",
            Interval::D3 => "3d",
            Interval::W1 => "1w",
            Interval::Month1 => "1M",
        }
    }

    /// Get a human-readable description.
    pub const fn description(&self) -> &'static str {
        match self {
            Interval::M1 => "1 Minute",
            Interval::M3 => "3 Minutes",
            Interval::M5 => "5 Minutes",
            Interval::M15 => "15 Minutes",
            Interval::M30 => "30 Minutes",
            Interval::H1 => "1 Hour",
            Interval::H2 => "2 Hours",
            Interval::H4 => "4 Hours",
            Interval::H6 => "6 Hours",
            Interval::H12 => "12 Hours",
            Interval::D1 => "1 Day",
            Interval::D3 => "3 Days",
            Interval::W1 => "1 Week",
            Interval::Month1 => "1 Month",
        }
    }

    /// Get all supported intervals in ascending duration order.
    pub const fn all() -> &'static [Interval] {
        &[
            Interval::M1,
            Interval::M3,
            Interval::M5,
            Interval::M15,
            Interval::M30,
            Interval::H1,
            Interval::H2,
            Interval::H4,
            Interval::H6,
            Interval::H12,
            Interval::D1,
            Interval::D3,
            Interval::W1,
            Interval::Month1,
        ]
    }

    /// Parse an interval string and return its milliseconds, or None if invalid.
    pub fn ms_from_str(s: &str) -> Option<i64> {
        s.parse::<Interval>().ok().map(|iv| iv.to_ms())
    }
}

/// Error type for interval parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseIntervalError {
    pub input: String,
}

impl std::fmt::Display for ParseIntervalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "unknown interval '{}', valid options: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d, 3d, 1w, 1M",
            self.input
        )
    }
}

impl std::error::Error for ParseIntervalError {}

impl FromStr for Interval {
    type Err = ParseIntervalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "1m" => Ok(Interval::M1),
            "3m" => Ok(Interval::M3),
            "5m" => Ok(Interval::M5),
            "15m" => Ok(Interval::M15),
            "30m" => Ok(Interval::M30),
            "1h" => Ok(Interval::H1),
            "2h" => Ok(Interval::H2),
            "4h" => Ok(Interval::H4),
            "6h" => Ok(Interval::H6),
            "12h" => Ok(Interval::H12),
            "1d" => Ok(Interval::D1),
            "3d" => Ok(Interval::D3),
            "1w" => Ok(Interval::W1),
            "1M" => Ok(Interval::Month1),
            _ => Err(ParseIntervalError {
                input: s.to_string(),
            }),
        }
    }
}

impl Default for Interval {
    fn default() -> Self {
        Interval::M1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_interval_to_ms() {
        assert_eq!(Interval::M1.to_ms(), 60_000);
        assert_eq!(Interval::M3.to_ms(), 180_000);
        assert_eq!(Interval::M5.to_ms(), 300_000);
        assert_eq!(Interval::M15.to_ms(), 900_000);
        assert_eq!(Interval::M30.to_ms(), 1_800_000);
        assert_eq!(Interval::H1.to_ms(), 3_600_000);
        assert_eq!(Interval::H2.to_ms(), 7_200_000);
        assert_eq!(Interval::H4.to_ms(), 14_400_000);
        assert_eq!(Interval::H6.to_ms(), 21_600_000);
        assert_eq!(Interval::H12.to_ms(), 43_200_000);
        assert_eq!(Interval::D1.to_ms(), 86_400_000);
        assert_eq!(Interval::D3.to_ms(), 259_200_000);
        assert_eq!(Interval::W1.to_ms(), 604_800_000);
        assert_eq!(Interval::Month1.to_ms(), 2_592_000_000);
    }

    #[test]
    fn test_interval_to_seconds() {
        assert_eq!(Interval::M1.to_seconds(), 60);
        assert_eq!(Interval::M3.to_seconds(), 180);
        assert_eq!(Interval::M5.to_seconds(), 300);
        assert_eq!(Interval::M15.to_seconds(), 900);
        assert_eq!(Interval::M30.to_seconds(), 1_800);
        assert_eq!(Interval::H1.to_seconds(), 3_600);
        assert_eq!(Interval::H2.to_seconds(), 7_200);
        assert_eq!(Interval::H4.to_seconds(), 14_400);
        assert_eq!(Interval::H6.to_seconds(), 21_600);
        assert_eq!(Interval::H12.to_seconds(), 43_200);
        assert_eq!(Interval::D1.to_seconds(), 86_400);
        assert_eq!(Interval::D3.to_seconds(), 259_200);
        assert_eq!(Interval::W1.to_seconds(), 604_800);
        assert_eq!(Interval::Month1.to_seconds(), 2_592_000);
    }

    #[test]
    fn test_interval_as_str() {
        assert_eq!(Interval::M1.as_str(), "1m");
        assert_eq!(Interval::M3.as_str(), "3m");
        assert_eq!(Interval::M5.as_str(), "5m");
        assert_eq!(Interval::M15.as_str(), "15m");
        assert_eq!(Interval::M30.as_str(), "30m");
        assert_eq!(Interval::H1.as_str(), "1h");
        assert_eq!(Interval::H2.as_str(), "2h");
        assert_eq!(Interval::H4.as_str(), "4h");
        assert_eq!(Interval::H6.as_str(), "6h");
        assert_eq!(Interval::H12.as_str(), "12h");
        assert_eq!(Interval::D1.as_str(), "1d");
        assert_eq!(Interval::D3.as_str(), "3d");
        assert_eq!(Interval::W1.as_str(), "1w");
        assert_eq!(Interval::Month1.as_str(), "1M");
    }

    #[test]
    fn test_interval_description() {
        assert_eq!(Interval::M1.description(), "1 Minute");
        assert_eq!(Interval::M3.description(), "3 Minutes");
        assert_eq!(Interval::M5.description(), "5 Minutes");
        assert_eq!(Interval::M15.description(), "15 Minutes");
        assert_eq!(Interval::M30.description(), "30 Minutes");
        assert_eq!(Interval::H1.description(), "1 Hour");
        assert_eq!(Interval::H2.description(), "2 Hours");
        assert_eq!(Interval::H4.description(), "4 Hours");
        assert_eq!(Interval::H6.description(), "6 Hours");
        assert_eq!(Interval::H12.description(), "12 Hours");
        assert_eq!(Interval::D1.description(), "1 Day");
        assert_eq!(Interval::D3.description(), "3 Days");
        assert_eq!(Interval::W1.description(), "1 Week");
        assert_eq!(Interval::Month1.description(), "1 Month");
    }

    #[test]
    fn test_interval_all() {
        let all = Interval::all();
        assert_eq!(all.len(), 14);
        assert_eq!(all[0], Interval::M1);
        assert_eq!(all[13], Interval::Month1);
    }

    #[test]
    fn test_interval_durations_increase() {
        let all = Interval::all();
        for i in 0..(all.len() - 1) {
            assert!(
                all[i].to_ms() < all[i + 1].to_ms(),
                "{:?} should have shorter duration than {:?}",
                all[i],
                all[i + 1]
            );
        }
    }

    #[test]
    fn test_interval_from_str() {
        assert_eq!("1m".parse::<Interval>().unwrap(), Interval::M1);
        assert_eq!("3m".parse::<Interval>().unwrap(), Interval::M3);
        assert_eq!("5m".parse::<Interval>().unwrap(), Interval::M5);
        assert_eq!("15m".parse::<Interval>().unwrap(), Interval::M15);
        assert_eq!("30m".parse::<Interval>().unwrap(), Interval::M30);
        assert_eq!("1h".parse::<Interval>().unwrap(), Interval::H1);
        assert_eq!("2h".parse::<Interval>().unwrap(), Interval::H2);
        assert_eq!("4h".parse::<Interval>().unwrap(), Interval::H4);
        assert_eq!("6h".parse::<Interval>().unwrap(), Interval::H6);
        assert_eq!("12h".parse::<Interval>().unwrap(), Interval::H12);
        assert_eq!("1d".parse::<Interval>().unwrap(), Interval::D1);
        assert_eq!("3d".parse::<Interval>().unwrap(), Interval::D3);
        assert_eq!("1w".parse::<Interval>().unwrap(), Interval::W1);
        assert_eq!("1M".parse::<Interval>().unwrap(), Interval::Month1);

        assert!("invalid".parse::<Interval>().is_err());
        assert!("".parse::<Interval>().is_err());
        assert!("1x".parse::<Interval>().is_err());
    }

    #[test]
    fn test_interval_roundtrip() {
        for iv in Interval::all() {
            let s = iv.as_str();
            let parsed: Interval = s.parse().unwrap();
            assert_eq!(*iv, parsed);
        }
    }

    #[test]
    fn test_interval_display() {
        assert_eq!(format!("{}", Interval::M1), "1m");
        assert_eq!(format!("{}", Interval::H4), "4h");
        assert_eq!(format!("{}", Interval::D1), "1d");
        assert_eq!(format!("{}", Interval::Month1), "1M");
    }

    #[test]
    fn test_interval_default() {
        assert_eq!(Interval::default(), Interval::M1);
    }

    #[test]
    fn test_interval_ms_from_str() {
        assert_eq!(Interval::ms_from_str("1m"), Some(60_000));
        assert_eq!(Interval::ms_from_str("3m"), Some(180_000));
        assert_eq!(Interval::ms_from_str("1h"), Some(3_600_000));
        assert_eq!(Interval::ms_from_str("4h"), Some(14_400_000));
        assert_eq!(Interval::ms_from_str("1d"), Some(86_400_000));
        assert_eq!(Interval::ms_from_str("1M"), Some(2_592_000_000));
        assert_eq!(Interval::ms_from_str("invalid"), None);
        assert_eq!(Interval::ms_from_str(""), None);
    }

    #[test]
    fn test_parse_interval_error_display() {
        let result = "invalid".parse::<Interval>();
        assert!(result.is_err());

        let err = result.unwrap_err();
        let msg = format!("{}", err);
        assert!(msg.contains("invalid"));
        assert!(msg.contains("1m"));
    }
}
