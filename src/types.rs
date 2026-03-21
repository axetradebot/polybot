use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BotMode {
    Paper,
    Live,
}

impl fmt::Display for BotMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BotMode::Paper => write!(f, "paper"),
            BotMode::Live => write!(f, "live"),
        }
    }
}

impl std::str::FromStr for BotMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "paper" => Ok(BotMode::Paper),
            "live" => Ok(BotMode::Live),
            _ => Err(anyhow::anyhow!("Invalid bot mode: {s}. Use 'paper' or 'live'")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Up,
    Down,
}

impl fmt::Display for Direction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Direction::Up => write!(f, "UP"),
            Direction::Down => write!(f, "DOWN"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TradeOutcome {
    Win,
    Loss,
    Pending,
}

impl fmt::Display for TradeOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TradeOutcome::Win => write!(f, "WIN"),
            TradeOutcome::Loss => write!(f, "LOSS"),
            TradeOutcome::Pending => write!(f, "PENDING"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WindowInfo {
    pub window_ts: u64,
    pub slug: String,
    pub seconds_remaining: u64,
}

impl WindowInfo {
    pub fn close_ts(&self) -> u64 {
        self.window_ts + 300
    }
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub direction: Direction,
    pub delta_pct: Decimal,
    pub target_price: Decimal,
    pub token_id: String,
    pub window_ts: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MarketInfo {
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub slug: String,
    pub accepting_orders: bool,
    pub neg_risk: bool,
    pub tick_size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub id: Option<i64>,
    pub timestamp: DateTime<Utc>,
    pub window_ts: u64,
    pub market_slug: String,
    pub direction: String,
    pub token_id: String,
    pub order_id: String,
    /// "early", "mid", "late", or "single"
    pub tier_name: String,
    pub entry_price: Decimal,
    pub size: Decimal,
    pub cost_usd: Decimal,
    pub outcome: String,
    pub pnl: Decimal,
    pub btc_open_price: Decimal,
    pub btc_close_price: Decimal,
    pub delta_pct: Decimal,
    /// Seconds remaining in the window when the order was placed.
    pub seconds_at_entry: Decimal,
    pub mode: String,
    pub filled: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OrderResult {
    pub order_id: String,
    pub filled: bool,
    pub fill_price: Decimal,
    pub fill_size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceTrade {
    #[serde(rename = "e")]
    pub event_type: String,
    #[serde(rename = "E")]
    pub event_time: u64,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub quantity: String,
    #[serde(rename = "T")]
    pub trade_time: u64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignatureType {
    Eoa,
    Proxy,
    GnosisSafe,
}

impl std::str::FromStr for SignatureType {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "eoa" => Ok(SignatureType::Eoa),
            "proxy" => Ok(SignatureType::Proxy),
            "gnosissafe" | "gnosis_safe" | "safe" => Ok(SignatureType::GnosisSafe),
            _ => Err(anyhow::anyhow!("Invalid signature type: {s}")),
        }
    }
}
