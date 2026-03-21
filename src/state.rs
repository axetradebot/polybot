use chrono::Utc;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;

pub type SharedState = Arc<RwLock<BotState>>;

pub fn new_shared_state(starting_bankroll: Decimal) -> SharedState {
    Arc::new(RwLock::new(BotState::new(starting_bankroll)))
}

/// Global bot state: bankroll tracking, daily statistics, and circuit breaker.
///
/// Price data and position tracking live in `PriceFeeds` and `PositionTracker`
/// respectively — this struct only holds financial accounting.
#[derive(Debug)]
#[allow(dead_code)]
pub struct BotState {
    pub bankroll: Decimal,
    pub daily_pnl: Decimal,
    pub daily_trades: u32,
    pub daily_wins: u32,
    pub daily_losses: u32,
    pub daily_fills: u32,
    pub daily_skips: u32,
    pub daily_reset_date: Option<chrono::NaiveDate>,

    pub consecutive_losses: u32,
    pub paused_until: Option<tokio::time::Instant>,

    pub total_trades: u64,
    pub total_pnl: Decimal,
    pub session_start: chrono::DateTime<Utc>,
}

impl BotState {
    fn new(starting_bankroll: Decimal) -> Self {
        Self {
            bankroll: starting_bankroll,
            daily_pnl: Decimal::ZERO,
            daily_trades: 0,
            daily_wins: 0,
            daily_losses: 0,
            daily_fills: 0,
            daily_skips: 0,
            daily_reset_date: None,
            consecutive_losses: 0,
            paused_until: None,
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
            self.daily_fills = 0;
            self.daily_skips = 0;
            self.daily_reset_date = Some(today);
        }
    }

    pub fn record_win(&mut self, pnl: Decimal) {
        self.daily_pnl += pnl;
        self.daily_trades += 1;
        self.daily_wins += 1;
        self.daily_fills += 1;
        self.total_trades += 1;
        self.total_pnl += pnl;
        self.consecutive_losses = 0;
        self.bankroll += pnl;
    }

    pub fn record_loss(&mut self, pnl: Decimal) {
        self.daily_pnl += pnl;
        self.daily_trades += 1;
        self.daily_losses += 1;
        self.daily_fills += 1;
        self.total_trades += 1;
        self.total_pnl += pnl;
        self.consecutive_losses += 1;
        self.bankroll += pnl;
    }
}
