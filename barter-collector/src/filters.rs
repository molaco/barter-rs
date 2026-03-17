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
