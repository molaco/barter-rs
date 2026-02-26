use derive_more::{Constructor, Display};
use serde::{Deserialize, Serialize};

#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Constructor,
)]
pub struct ExchangeIndex(pub usize);

impl ExchangeIndex {
    pub fn index(&self) -> usize {
        self.0
    }
}

impl std::fmt::Display for ExchangeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ExchangeIndex({})", self.0)
    }
}

/// Unique identifier for an execution server.
///
/// ### Notes
/// An execution may have a distinct server for different
/// [`InstrumentKinds`](super::instrument::kind::InstrumentKind).
///
/// For example, BinanceSpot and BinanceFuturesUsd have distinct APIs, and are therefore
/// represented as unique variants.
#[derive(
    Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Deserialize, Serialize, Display,
)]
#[serde(rename = "execution", rename_all = "snake_case")]
pub enum ExchangeId {
    Other,
    Simulated,
    Mock,
    BinanceFuturesCoin,
    BinanceFuturesUsd,
    BinanceOptions,
    BinancePortfolioMargin,
    BinanceSpot,
    BinanceUs,
    Bitazza,
    Bitfinex,
    Bitflyer,
    Bitget,
    Bitmart,
    BitmartFuturesUsd,
    Bitmex,
    Bitso,
    Bitstamp,
    Bitvavo,
    Bithumb,
    BybitPerpetualsUsd,
    BybitSpot,
    Cexio,
    Coinbase,
    CoinbaseInternational,
    Cryptocom,
    Deribit,
    GateioFuturesBtc,
    GateioFuturesUsd,
    GateioOptions,
    GateioPerpetualsBtc,
    GateioPerpetualsUsd,
    GateioSpot,
    Gemini,
    Hitbtc,
    #[serde(alias = "huobi")]
    Htx,
    Hyperliquid,
    Kraken,
    Kucoin,
    Liquid,
    Mexc,
    Okx,
    Poloniex,
}

impl ExchangeId {
    /// Return the &str representation of this [`ExchangeId`]
    pub fn as_str(&self) -> &'static str {
        match self {
            ExchangeId::Other => "other",
            ExchangeId::Simulated => "simulated",
            ExchangeId::Mock => "mock",
            ExchangeId::BinanceFuturesCoin => "binance_futures_coin",
            ExchangeId::BinanceFuturesUsd => "binance_futures_usd",
            ExchangeId::BinanceOptions => "binance_options",
            ExchangeId::BinancePortfolioMargin => "binance_portfolio_margin",
            ExchangeId::BinanceSpot => "binance_spot",
            ExchangeId::BinanceUs => "binance_us",
            ExchangeId::Bitazza => "bitazza",
            ExchangeId::Bitfinex => "bitfinex",
            ExchangeId::Bitflyer => "bitflyer",
            ExchangeId::Bitget => "bitget",
            ExchangeId::Bitmart => "bitmart",
            ExchangeId::BitmartFuturesUsd => "bitmart_futures_usd",
            ExchangeId::Bitmex => "bitmex",
            ExchangeId::Bitso => "bitso",
            ExchangeId::Bitstamp => "bitstamp",
            ExchangeId::Bitvavo => "bitvavo",
            ExchangeId::Bithumb => "bithumb",
            ExchangeId::BybitPerpetualsUsd => "bybit_perpetuals_usd",
            ExchangeId::BybitSpot => "bybit_spot",
            ExchangeId::Cexio => "cexio",
            ExchangeId::Coinbase => "coinbase",
            ExchangeId::CoinbaseInternational => "coinbase_international",
            ExchangeId::Cryptocom => "cryptocom",
            ExchangeId::Deribit => "deribit",
            ExchangeId::GateioFuturesBtc => "gateio_futures_btc",
            ExchangeId::GateioFuturesUsd => "gateio_futures_usd",
            ExchangeId::GateioOptions => "gateio_options",
            ExchangeId::GateioPerpetualsBtc => "gateio_perpetuals_btc",
            ExchangeId::GateioPerpetualsUsd => "gateio_perpetuals_usd",
            ExchangeId::GateioSpot => "gateio_spot",
            ExchangeId::Gemini => "gemini",
            ExchangeId::Hitbtc => "hitbtc",
            ExchangeId::Htx => "htx", // huobi alias
            ExchangeId::Hyperliquid => "hyperliquid",
            ExchangeId::Kraken => "kraken",
            ExchangeId::Kucoin => "kucoin",
            ExchangeId::Liquid => "liquid",
            ExchangeId::Mexc => "mexc",
            ExchangeId::Okx => "okx",
            ExchangeId::Poloniex => "poloniex",
        }
    }
}

impl std::str::FromStr for ExchangeId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let lower = s.to_ascii_lowercase();
        match lower.as_str() {
            "other" => Ok(ExchangeId::Other),
            "simulated" => Ok(ExchangeId::Simulated),
            "mock" => Ok(ExchangeId::Mock),
            "binance_futures_coin" | "binanceinverse" => Ok(ExchangeId::BinanceFuturesCoin),
            "binance_futures_usd" | "binancelinear" => Ok(ExchangeId::BinanceFuturesUsd),
            "binance_options" => Ok(ExchangeId::BinanceOptions),
            "binance_portfolio_margin" => Ok(ExchangeId::BinancePortfolioMargin),
            "binance_spot" => Ok(ExchangeId::BinanceSpot),
            "binance_us" => Ok(ExchangeId::BinanceUs),
            "bitazza" => Ok(ExchangeId::Bitazza),
            "bitfinex" => Ok(ExchangeId::Bitfinex),
            "bitflyer" => Ok(ExchangeId::Bitflyer),
            "bitget" => Ok(ExchangeId::Bitget),
            "bitmart" => Ok(ExchangeId::Bitmart),
            "bitmart_futures_usd" => Ok(ExchangeId::BitmartFuturesUsd),
            "bitmex" => Ok(ExchangeId::Bitmex),
            "bitso" => Ok(ExchangeId::Bitso),
            "bitstamp" => Ok(ExchangeId::Bitstamp),
            "bitvavo" => Ok(ExchangeId::Bitvavo),
            "bithumb" => Ok(ExchangeId::Bithumb),
            "bybit_perpetuals_usd" | "bybitlinear" | "bybitperps" => {
                Ok(ExchangeId::BybitPerpetualsUsd)
            }
            "bybit_spot" => Ok(ExchangeId::BybitSpot),
            "cexio" => Ok(ExchangeId::Cexio),
            "coinbase" => Ok(ExchangeId::Coinbase),
            "coinbase_international" => Ok(ExchangeId::CoinbaseInternational),
            "cryptocom" => Ok(ExchangeId::Cryptocom),
            "deribit" => Ok(ExchangeId::Deribit),
            "gateio_futures_btc" => Ok(ExchangeId::GateioFuturesBtc),
            "gateio_futures_usd" => Ok(ExchangeId::GateioFuturesUsd),
            "gateio_options" => Ok(ExchangeId::GateioOptions),
            "gateio_perpetuals_btc" => Ok(ExchangeId::GateioPerpetualsBtc),
            "gateio_perpetuals_usd" => Ok(ExchangeId::GateioPerpetualsUsd),
            "gateio_spot" => Ok(ExchangeId::GateioSpot),
            "gemini" => Ok(ExchangeId::Gemini),
            "hitbtc" => Ok(ExchangeId::Hitbtc),
            "htx" | "huobi" => Ok(ExchangeId::Htx),
            "hyperliquid" => Ok(ExchangeId::Hyperliquid),
            "kraken" => Ok(ExchangeId::Kraken),
            "kucoin" => Ok(ExchangeId::Kucoin),
            "liquid" => Ok(ExchangeId::Liquid),
            "mexc" => Ok(ExchangeId::Mexc),
            "okx" => Ok(ExchangeId::Okx),
            "poloniex" => Ok(ExchangeId::Poloniex),
            _ => Err(format!("unrecognised ExchangeId: {s}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_de_exchange_id() {
        assert_eq!(
            serde_json::from_str::<ExchangeId>(r#""htx""#).unwrap(),
            ExchangeId::Htx
        );
        assert_eq!(
            serde_json::from_str::<ExchangeId>(r#""huobi""#).unwrap(),
            ExchangeId::Htx
        );
    }

    #[test]
    fn test_from_str_roundtrip() {
        let variants = [
            ExchangeId::Other,
            ExchangeId::Simulated,
            ExchangeId::Mock,
            ExchangeId::BinanceFuturesCoin,
            ExchangeId::BinanceFuturesUsd,
            ExchangeId::BinanceOptions,
            ExchangeId::BinancePortfolioMargin,
            ExchangeId::BinanceSpot,
            ExchangeId::BinanceUs,
            ExchangeId::Bitazza,
            ExchangeId::Bitfinex,
            ExchangeId::Bitflyer,
            ExchangeId::Bitget,
            ExchangeId::Bitmart,
            ExchangeId::BitmartFuturesUsd,
            ExchangeId::Bitmex,
            ExchangeId::Bitso,
            ExchangeId::Bitstamp,
            ExchangeId::Bitvavo,
            ExchangeId::Bithumb,
            ExchangeId::BybitPerpetualsUsd,
            ExchangeId::BybitSpot,
            ExchangeId::Cexio,
            ExchangeId::Coinbase,
            ExchangeId::CoinbaseInternational,
            ExchangeId::Cryptocom,
            ExchangeId::Deribit,
            ExchangeId::GateioFuturesBtc,
            ExchangeId::GateioFuturesUsd,
            ExchangeId::GateioOptions,
            ExchangeId::GateioPerpetualsBtc,
            ExchangeId::GateioPerpetualsUsd,
            ExchangeId::GateioSpot,
            ExchangeId::Gemini,
            ExchangeId::Hitbtc,
            ExchangeId::Htx,
            ExchangeId::Hyperliquid,
            ExchangeId::Kraken,
            ExchangeId::Kucoin,
            ExchangeId::Liquid,
            ExchangeId::Mexc,
            ExchangeId::Okx,
            ExchangeId::Poloniex,
        ];
        for variant in variants {
            let parsed: ExchangeId = variant.as_str().parse().unwrap_or_else(|e: String| {
                panic!("failed to parse {:?} from {:?}: {e}", variant, variant.as_str())
            });
            assert_eq!(parsed, variant, "roundtrip failed for {:?}", variant);
        }
    }

    #[test]
    fn test_from_str_chart_aliases() {
        assert_eq!(
            "BinanceLinear".parse::<ExchangeId>().unwrap(),
            ExchangeId::BinanceFuturesUsd
        );
        assert_eq!(
            "BinanceInverse".parse::<ExchangeId>().unwrap(),
            ExchangeId::BinanceFuturesCoin
        );
        assert_eq!(
            "BybitLinear".parse::<ExchangeId>().unwrap(),
            ExchangeId::BybitPerpetualsUsd
        );
        assert_eq!(
            "BybitPerps".parse::<ExchangeId>().unwrap(),
            ExchangeId::BybitPerpetualsUsd
        );
        // case-insensitive: uppercase/mixed should also work
        assert_eq!(
            "BINANCELINEAR".parse::<ExchangeId>().unwrap(),
            ExchangeId::BinanceFuturesUsd
        );
        assert_eq!(
            "bybitperps".parse::<ExchangeId>().unwrap(),
            ExchangeId::BybitPerpetualsUsd
        );
        // huobi alias
        assert_eq!(
            "huobi".parse::<ExchangeId>().unwrap(),
            ExchangeId::Htx
        );
    }

    #[test]
    fn test_from_str_invalid() {
        let result = "not_a_real_exchange".parse::<ExchangeId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not_a_real_exchange"));
    }
}
