use crate::{error::DataError, trade::RestTrade};
use barter_instrument::Side;
use chrono::DateTime;
use serde::Deserialize;
use std::collections::HashMap;

/// A single fill event from the Hyperliquid hourly archive.
///
/// Each JSON line in the archive is an array of fill events. Extra fields
/// beyond what we need are silently ignored (no `deny_unknown_fields`).
#[derive(Debug, Deserialize)]
pub struct HyperliquidFillEvent {
    pub coin: String,
    pub px: String,
    pub sz: String,
    /// `"B"` for buy, `"A"` or `"S"` for sell.
    pub side: String,
    /// Millisecond unix timestamp.
    pub time: u64,
    /// Unique trade id.
    pub tid: u64,
}

impl TryFrom<HyperliquidFillEvent> for RestTrade {
    type Error = DataError;

    fn try_from(fill: HyperliquidFillEvent) -> Result<Self, Self::Error> {
        let time = DateTime::from_timestamp_millis(fill.time as i64)
            .ok_or_else(|| DataError::Socket(format!("invalid fill timestamp: {}", fill.time)))?;

        let price: f64 = fill
            .px
            .parse()
            .map_err(|e| DataError::Socket(format!("invalid fill price '{}': {e}", fill.px)))?;

        let amount: f64 = fill
            .sz
            .parse()
            .map_err(|e| DataError::Socket(format!("invalid fill size '{}': {e}", fill.sz)))?;

        let side = match fill.side.as_str() {
            "B" => Side::Buy,
            "A" | "S" => Side::Sell,
            other => {
                return Err(DataError::Socket(format!("unknown fill side '{other}'")));
            }
        };

        Ok(RestTrade {
            id: fill.tid.to_string(),
            time,
            price,
            amount,
            side,
        })
    }
}

/// Parse LZ4-decompressed JSON fill data, filtering by market coin.
///
/// Each line in the data is either:
/// - A JSON array of [`HyperliquidFillEvent`]s: `[{...}, {...}]`
/// - A JSON object with an `"events"` array: `{"events": [{...}]}`
///
/// Lines that fail to parse in either format are logged and skipped.
pub fn parse_fills(json_data: &[u8], market: &str) -> Result<Vec<RestTrade>, DataError> {
    let text = std::str::from_utf8(json_data)
        .map_err(|e| DataError::Socket(format!("invalid UTF-8 in fill data: {e}")))?;

    let mut trades = Vec::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // Try block format first (most common in hourly archives):
        // {"events": [["0xaddr", {...fill...}], ...]}
        if let Ok(block) = serde_json::from_str::<BlockWrapper>(line) {
            for (_addr, fill) in block.events {
                if fill.coin == market {
                    trades.push(RestTrade::try_from(fill)?);
                }
            }
            continue;
        }

        // Fallback: flat array of fills
        if let Ok(array) = serde_json::from_str::<Vec<HyperliquidFillEvent>>(line) {
            for fill in array {
                if fill.coin == market {
                    trades.push(RestTrade::try_from(fill)?);
                }
            }
            continue;
        }

        tracing::warn!(
            "skipping unparseable fill line: {}",
            &line[..line.len().min(120)]
        );
    }

    Ok(trades)
}

/// Parse LZ4-decompressed JSON fill data, returning trades grouped by coin.
///
/// Collects fills for all coins into a HashMap. This avoids downloading
/// the same hourly file multiple times for different coins.
pub fn parse_fills_multi(
    json_data: &[u8],
) -> Result<HashMap<String, Vec<RestTrade>>, DataError> {
    let text = std::str::from_utf8(json_data)
        .map_err(|e| DataError::Socket(format!("invalid UTF-8 in fill data: {e}")))?;

    let mut result: HashMap<String, Vec<RestTrade>> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        if let Ok(block) = serde_json::from_str::<BlockWrapper>(line) {
            for (_addr, fill) in block.events {
                let coin = fill.coin.clone();
                result
                    .entry(coin)
                    .or_default()
                    .push(RestTrade::try_from(fill)?);
            }
            continue;
        }

        if let Ok(array) = serde_json::from_str::<Vec<HyperliquidFillEvent>>(line) {
            for fill in array {
                let coin = fill.coin.clone();
                result
                    .entry(coin)
                    .or_default()
                    .push(RestTrade::try_from(fill)?);
            }
            continue;
        }

        tracing::warn!(
            "skipping unparseable fill line: {}",
            &line[..line.len().min(120)]
        );
    }

    Ok(result)
}

/// Wrapper for the block-object JSON line format.
///
/// Each event is a tuple `[address, fill_data]` in the JSON.
#[derive(Debug, Deserialize)]
struct BlockWrapper {
    #[serde(default)]
    events: Vec<(String, HyperliquidFillEvent)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_ARRAY_LINE: &str = r#"[{"coin":"BTC","side":"B","px":"42000.0","sz":"0.1","time":1704067200000,"hash":"0xabc","startPosition":{"type":"oneWay","szi":"1.0"},"dir":"Open Long","closedPnl":"0","oid":12345,"crossed":true,"fee":"0.42","tid":100,"feeToken":"USDC"},{"coin":"ETH","side":"A","px":"2200.0","sz":"1.5","time":1704067200001,"hash":"0xdef","startPosition":{"type":"oneWay","szi":"2.0"},"dir":"Open Short","closedPnl":"0","oid":12346,"crossed":false,"fee":"0.22","tid":101,"feeToken":"USDC"}]"#;

    const SAMPLE_BLOCK_LINE: &str = r#"{"block_number":12345,"block_time":"2024-01-01T00:00:00.000Z","events":[["0xabc123",{"coin":"BTC","side":"B","px":"42000.0","sz":"0.1","time":1704067200000,"tid":200}]]}"#;

    #[test]
    fn test_parse_array_format_filters_by_market() {
        let trades = parse_fills(SAMPLE_ARRAY_LINE.as_bytes(), "BTC").unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].id, "100");
        assert_eq!(trades[0].price, 42000.0);
        assert_eq!(trades[0].amount, 0.1);
        assert_eq!(trades[0].side, Side::Buy);
    }

    #[test]
    fn test_parse_array_format_different_market() {
        let trades = parse_fills(SAMPLE_ARRAY_LINE.as_bytes(), "ETH").unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].id, "101");
        assert_eq!(trades[0].price, 2200.0);
        assert_eq!(trades[0].amount, 1.5);
        assert_eq!(trades[0].side, Side::Sell);
    }

    #[test]
    fn test_parse_block_format() {
        let trades = parse_fills(SAMPLE_BLOCK_LINE.as_bytes(), "BTC").unwrap();
        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].id, "200");
        assert_eq!(trades[0].price, 42000.0);
    }

    #[test]
    fn test_parse_no_matching_market() {
        let trades = parse_fills(SAMPLE_ARRAY_LINE.as_bytes(), "SOL").unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn test_parse_empty_data() {
        let trades = parse_fills(b"", "BTC").unwrap();
        assert!(trades.is_empty());
    }

    #[test]
    fn test_parse_multiline() {
        let data = format!("{SAMPLE_ARRAY_LINE}\n{SAMPLE_BLOCK_LINE}\n");
        let trades = parse_fills(data.as_bytes(), "BTC").unwrap();
        assert_eq!(trades.len(), 2);
    }

    #[test]
    fn test_parse_skips_invalid_lines() {
        let data = format!("not valid json\n{SAMPLE_ARRAY_LINE}\n");
        let trades = parse_fills(data.as_bytes(), "BTC").unwrap();
        assert_eq!(trades.len(), 1);
    }

    #[test]
    fn test_fill_event_try_from_buy() {
        let fill = HyperliquidFillEvent {
            coin: "BTC".to_string(),
            px: "50000.0".to_string(),
            sz: "0.5".to_string(),
            side: "B".to_string(),
            time: 1704067200000,
            tid: 42,
        };
        let trade = RestTrade::try_from(fill).unwrap();
        assert_eq!(trade.id, "42");
        assert_eq!(trade.price, 50000.0);
        assert_eq!(trade.amount, 0.5);
        assert_eq!(trade.side, Side::Buy);
    }

    #[test]
    fn test_fill_event_try_from_sell_a() {
        let fill = HyperliquidFillEvent {
            coin: "ETH".to_string(),
            px: "2000.0".to_string(),
            sz: "1.0".to_string(),
            side: "A".to_string(),
            time: 1704067200000,
            tid: 43,
        };
        let trade = RestTrade::try_from(fill).unwrap();
        assert_eq!(trade.side, Side::Sell);
    }

    #[test]
    fn test_fill_event_try_from_sell_s() {
        let fill = HyperliquidFillEvent {
            coin: "ETH".to_string(),
            px: "2000.0".to_string(),
            sz: "1.0".to_string(),
            side: "S".to_string(),
            time: 1704067200000,
            tid: 44,
        };
        let trade = RestTrade::try_from(fill).unwrap();
        assert_eq!(trade.side, Side::Sell);
    }

    #[test]
    fn test_fill_event_try_from_invalid_side() {
        let fill = HyperliquidFillEvent {
            coin: "BTC".to_string(),
            px: "50000.0".to_string(),
            sz: "0.5".to_string(),
            side: "X".to_string(),
            time: 1704067200000,
            tid: 45,
        };
        assert!(RestTrade::try_from(fill).is_err());
    }

    #[test]
    fn test_fill_event_try_from_invalid_price() {
        let fill = HyperliquidFillEvent {
            coin: "BTC".to_string(),
            px: "not_a_number".to_string(),
            sz: "0.5".to_string(),
            side: "B".to_string(),
            time: 1704067200000,
            tid: 46,
        };
        assert!(RestTrade::try_from(fill).is_err());
    }
}
