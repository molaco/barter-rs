use super::channel::KrakenChannel;
use super::kraken_interval;
use super::message::KrakenMessage;
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::candle::Candle,
};
use barter_instrument::exchange::ExchangeId;
use barter_integration::subscription::SubscriptionId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Terse type alias for a Kraken v2 OHLC WebSocket message.
pub type KrakenKline = KrakenMessage<KrakenKlinePayload>;

/// Kraken v2 OHLC data message.
///
/// ### Raw Payload Example
/// ```json
/// {
///     "channel": "ohlc",
///     "type": "update",
///     "data": [{
///         "symbol": "BTC/USD",
///         "open": 66650.0,
///         "high": 66660.0,
///         "low": 66640.0,
///         "close": 66655.0,
///         "vwap": 66652.5,
///         "volume": 12.345,
///         "trades": 150,
///         "interval_begin": "2026-03-27T16:25:00.000000000Z",
///         "interval": 5
///     }]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenKlinePayload {
    pub channel: String,
    #[serde(rename = "type")]
    pub msg_type: String,
    pub data: Vec<KrakenKlineData>,
}

impl Identifier<Option<SubscriptionId>> for KrakenKlinePayload {
    fn id(&self) -> Option<SubscriptionId> {
        self.data.first().map(|k| {
            let channel = KrakenChannel::from_ohlc_interval(k.interval);
            ExchangeSub::from((channel, &k.symbol)).id()
        })
    }
}

/// A single Kraken v2 OHLC candle.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct KrakenKlineData {
    pub symbol: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub vwap: f64,
    pub volume: f64,
    pub trades: u64,
    pub interval_begin: DateTime<Utc>,
    pub interval: u32,
    #[serde(default)]
    pub timestamp: Option<DateTime<Utc>>,
}

impl KrakenChannel {
    /// Construct an OHLC channel name from a Kraken interval (minutes).
    pub fn from_ohlc_interval(interval_minutes: u32) -> Self {
        Self(smol_str::format_smolstr!("ohlc-{interval_minutes}"))
    }
}

impl<InstrumentKey: Clone> From<(ExchangeId, InstrumentKey, KrakenKline)>
    for MarketIter<InstrumentKey, Candle>
{
    fn from((exchange_id, instrument, kline): (ExchangeId, InstrumentKey, KrakenKline)) -> Self {
        match kline {
            KrakenKline::Data(payload) => payload
                .data
                .into_iter()
                .map(|k| {
                    // Compute close_time from interval_begin + interval
                    let close_time = k.interval_begin
                        + chrono::Duration::minutes(k.interval as i64);
                    Ok(MarketEvent {
                        time_exchange: close_time,
                        time_received: Utc::now(),
                        exchange: exchange_id,
                        instrument: instrument.clone(),
                        kind: Candle {
                            open_time: k.interval_begin,
                            close_time,
                            open: k.open,
                            high: k.high,
                            low: k.low,
                            close: k.close,
                            volume: k.volume,
                            quote_volume: None,
                            trade_count: k.trades,
                            is_closed: true,
                        },
                    })
                })
                .collect(),
            KrakenKline::Event(_) => Self(vec![]),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kraken_v2_kline_deserialize() {
        let input = r#"{
            "channel": "ohlc",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "open": 66650.0,
                "high": 66660.0,
                "low": 66640.0,
                "close": 66655.0,
                "vwap": 66652.5,
                "volume": 12.345,
                "trades": 150,
                "interval_begin": "2026-03-27T16:25:00.000000000Z",
                "interval": 5
            }]
        }"#;
        let msg: KrakenKline = serde_json::from_str(input).unwrap();
        match msg {
            KrakenKline::Data(payload) => {
                assert_eq!(payload.data.len(), 1);
                assert_eq!(payload.data[0].close, 66655.0);
                assert_eq!(payload.data[0].interval, 5);
            }
            _ => panic!("expected Data variant"),
        }
    }
}
