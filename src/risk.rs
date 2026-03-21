use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::positions::PositionTracker;
use crate::state::BotState;

pub struct RiskManager {
    daily_loss_limit: Decimal,
    max_consecutive_losses: u32,
    pause_duration: std::time::Duration,
    bet_size_usd: Decimal,
    max_concurrent: usize,
    max_per_market: usize,
}

#[derive(Debug)]
pub enum RiskVeto {
    DailyLossLimitReached,
    ConsecutiveLossPause,
    InsufficientBankroll,
    MaxConcurrentReached,
    MaxPerMarketReached,
    Paused { seconds_remaining: u64 },
}

impl std::fmt::Display for RiskVeto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskVeto::DailyLossLimitReached => write!(f, "daily loss limit reached"),
            RiskVeto::ConsecutiveLossPause => write!(f, "consecutive loss circuit breaker"),
            RiskVeto::InsufficientBankroll => write!(f, "insufficient bankroll"),
            RiskVeto::MaxConcurrentReached => write!(f, "max concurrent positions reached"),
            RiskVeto::MaxPerMarketReached => write!(f, "max positions per market reached"),
            RiskVeto::Paused { seconds_remaining } => {
                write!(f, "paused for {seconds_remaining}s")
            }
        }
    }
}

impl RiskManager {
    pub fn new(config: &AppConfig) -> Self {
        Self {
            daily_loss_limit: config.daily_loss_limit_decimal(),
            max_consecutive_losses: config.bankroll.consecutive_loss_pause,
            pause_duration: std::time::Duration::from_secs(
                config.bankroll.pause_duration_minutes * 60,
            ),
            bet_size_usd: config.bet_size_decimal(),
            max_concurrent: config.bankroll.max_concurrent_positions,
            max_per_market: config.bankroll.max_per_market,
        }
    }

    /// Check global trading eligibility (bankroll, daily loss, pauses).
    pub fn check_global(&self, state: &BotState) -> Result<(), RiskVeto> {
        if let Some(pause_until) = state.paused_until {
            if tokio::time::Instant::now() < pause_until {
                let remaining = (pause_until - tokio::time::Instant::now()).as_secs();
                return Err(RiskVeto::Paused {
                    seconds_remaining: remaining,
                });
            }
        }

        if state.daily_pnl < Decimal::ZERO && state.daily_pnl.abs() >= self.daily_loss_limit {
            warn!(
                daily_pnl = %state.daily_pnl,
                limit = %self.daily_loss_limit,
                "Daily loss limit reached"
            );
            return Err(RiskVeto::DailyLossLimitReached);
        }

        if state.consecutive_losses >= self.max_consecutive_losses {
            warn!(
                consecutive = state.consecutive_losses,
                max = self.max_consecutive_losses,
                "Consecutive loss circuit breaker"
            );
            return Err(RiskVeto::ConsecutiveLossPause);
        }

        if state.bankroll < self.bet_size_usd {
            warn!(
                bankroll = %state.bankroll,
                min = %self.bet_size_usd,
                "Insufficient bankroll"
            );
            return Err(RiskVeto::InsufficientBankroll);
        }

        Ok(())
    }

    /// Check if we can open a new position given current position counts.
    pub async fn check_position_limits(
        &self,
        positions: &PositionTracker,
        market_name: &str,
    ) -> Result<(), RiskVeto> {
        if positions.open_count().await >= self.max_concurrent {
            return Err(RiskVeto::MaxConcurrentReached);
        }
        if positions.open_count_for_market(market_name).await >= self.max_per_market {
            return Err(RiskVeto::MaxPerMarketReached);
        }
        Ok(())
    }

    /// Apply consecutive loss pause to state.
    pub fn apply_pause(&self, state: &mut BotState) {
        if state.consecutive_losses >= self.max_consecutive_losses {
            let pause_until = tokio::time::Instant::now() + self.pause_duration;
            state.paused_until = Some(pause_until);
            state.consecutive_losses = 0;
            info!(
                duration_minutes = self.pause_duration.as_secs() / 60,
                "Circuit breaker pause applied"
            );
        }
    }

    pub fn bet_size(&self) -> Decimal {
        self.bet_size_usd
    }
}
