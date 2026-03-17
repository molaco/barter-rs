use barter_data::rest::RestTrade;
use chrono::{DateTime, Utc};

/// Filter trades to the `[start, end]` time range (inclusive on both bounds).
pub fn filter_trades_by_time(
    trades: Vec<RestTrade>,
    start: Option<DateTime<Utc>>,
    end: Option<DateTime<Utc>>,
) -> Vec<RestTrade> {
    trades
        .into_iter()
        .filter(|t| {
            if let Some(start) = start
                && t.time < start
            {
                return false;
            }
            if let Some(end) = end
                && t.time > end
            {
                return false;
            }
            true
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use barter_instrument::Side;
    use chrono::TimeZone;

    fn trade_at(secs: i64) -> RestTrade {
        RestTrade {
            id: "1".into(),
            time: Utc.timestamp_opt(secs, 0).unwrap(),
            price: 100.0,
            amount: 1.0,
            side: Side::Buy,
        }
    }

    #[test]
    fn test_filter_no_bounds() {
        let trades = vec![trade_at(10), trade_at(20), trade_at(30)];
        let filtered = filter_trades_by_time(trades, None, None);
        assert_eq!(filtered.len(), 3);
    }

    #[test]
    fn test_filter_start_only() {
        let trades = vec![trade_at(10), trade_at(20), trade_at(30)];
        let start = Some(Utc.timestamp_opt(20, 0).unwrap());
        let filtered = filter_trades_by_time(trades, start, None);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].time.timestamp(), 20);
    }

    #[test]
    fn test_filter_end_only() {
        let trades = vec![trade_at(10), trade_at(20), trade_at(30)];
        let end = Some(Utc.timestamp_opt(20, 0).unwrap());
        let filtered = filter_trades_by_time(trades, None, end);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[1].time.timestamp(), 20);
    }

    #[test]
    fn test_filter_both_bounds() {
        let trades = vec![trade_at(10), trade_at(20), trade_at(30)];
        let start = Some(Utc.timestamp_opt(15, 0).unwrap());
        let end = Some(Utc.timestamp_opt(25, 0).unwrap());
        let filtered = filter_trades_by_time(trades, start, end);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].time.timestamp(), 20);
    }

    #[test]
    fn test_filter_empty_input() {
        let filtered = filter_trades_by_time(
            vec![],
            Some(Utc.timestamp_opt(10, 0).unwrap()),
            Some(Utc.timestamp_opt(20, 0).unwrap()),
        );
        assert!(filtered.is_empty());
    }
}
