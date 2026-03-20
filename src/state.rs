use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::types::MarketInfo;

pub type SharedState = Arc<RwLock<BotState>>;

pub fn new_shared_state(starting_bankroll: Decimal) -> SharedState {
    Arc::new(RwLock::new(BotState::new(starting_bankroll)))
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct BotState {
    pub btc_price: Decimal,
    pub btc_price_updated_at: Option<DateTime<Utc>>,

    pub window_open_price: Decimal,
    pub current_window_ts: u64,
    pub window_open_captured: bool,

    pub bankroll: Decimal,
    pub daily_pnl: Decimal,
    pub daily_trades: u32,
    pub daily_wins: u32,
    pub daily_losses: u32,
    pub daily_reset_date: Option<chrono::NaiveDate>,

    pub consecutive_losses: u32,
    pub paused_until: Option<tokio::time::Instant>,

    pub active_order_id: Option<String>,
    pub current_market: Option<MarketInfo>,

    pub total_trades: u64,
    pub total_pnl: Decimal,
    pub session_start: DateTime<Utc>,
}

impl BotState {
    fn new(starting_bankroll: Decimal) -> Self {
        Self {
            btc_price: Decimal::ZERO,
            btc_price_updated_at: None,
            window_open_price: Decimal::ZERO,
            current_window_ts: 0,
            window_open_captured: false,
            bankroll: starting_bankroll,
            daily_pnl: Decimal::ZERO,
            daily_trades: 0,
            daily_wins: 0,
            daily_losses: 0,
            daily_reset_date: None,
            consecutive_losses: 0,
            paused_until: None,
            active_order_id: None,
            current_market: None,
            total_trades: 0,
            total_pnl: Decimal::ZERO,
            session_start: Utc::now(),
        }
    }

    pub fn reset_daily_if_needed(&mut self) {
        let today = Utc::now().date_naive();
        if self.daily_reset_date != Some(today) {
            self.daily_pnl = Decimal::ZERO;
            self.daily_trades = 0;
            self.daily_wins = 0;
            self.daily_losses = 0;
            self.daily_reset_date = Some(today);
        }
    }

    pub fn record_win(&mut self, pnl: Decimal) {
        self.daily_pnl += pnl;
        self.daily_trades += 1;
        self.daily_wins += 1;
        self.total_trades += 1;
        self.total_pnl += pnl;
        self.consecutive_losses = 0;
        self.bankroll += pnl;
    }

    pub fn record_loss(&mut self, pnl: Decimal) {
        self.daily_pnl += pnl;
        self.daily_trades += 1;
        self.daily_losses += 1;
        self.total_trades += 1;
        self.total_pnl += pnl;
        self.consecutive_losses += 1;
        self.bankroll += pnl;
    }

    pub fn btc_price_age_ms(&self) -> Option<i64> {
        self.btc_price_updated_at
            .map(|t| (Utc::now() - t).num_milliseconds())
    }

    pub fn has_fresh_price(&self) -> bool {
        self.btc_price_age_ms()
            .map(|age| age < 5000)
            .unwrap_or(false)
    }
}
