use barter_instrument::Side;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single trade returned by a REST API or bulk archive, with timestamp included.
///
/// Distinct from [`PublicTrade`](crate::subscription::trade::PublicTrade) because
/// REST trade responses always include timestamps directly, whereas `PublicTrade`
/// omits `time` (it's provided by the `MarketEvent` wrapper in the WebSocket path).
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct RestTrade {
    pub id: String,
    pub time: DateTime<Utc>,
    pub price: f64,
    pub amount: f64,
    pub side: Side,
}
