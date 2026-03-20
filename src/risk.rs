use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::state::BotState;

pub struct RiskManager {
    daily_loss_limit: Decimal,
    max_consecutive_losses: u32,
    pause_duration: std::time::Duration,
    max_bet_usd: Decimal,
}

#[derive(Debug)]
pub enum RiskVeto {
    DailyLossLimitReached,
    ConsecutiveLossPause,
    InsufficientBankroll,
    Paused { seconds_remaining: u64 },
}

impl std::fmt::Display for RiskVeto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RiskVeto::DailyLossLimitReached => write!(f, "daily loss limit reached"),
            RiskVeto::ConsecutiveLossPause => write!(f, "consecutive loss circuit breaker"),
            RiskVeto::InsufficientBankroll => write!(f, "insufficient bankroll"),
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
            max_consecutive_losses: config.trading.consecutive_loss_pause,
            pause_duration: std::time::Duration::from_secs(
                config.trading.pause_duration_minutes * 60,
            ),
            max_bet_usd: config.max_bet_decimal(),
        }
    }

    /// Check if we're allowed to trade. Returns Ok(()) or Err with the reason.
    pub fn check(&self, state: &BotState) -> Result<(), RiskVeto> {
        // Check pause
        if let Some(pause_until) = state.paused_until {
            if tokio::time::Instant::now() < pause_until {
                let remaining = (pause_until - tokio::time::Instant::now()).as_secs();
                warn!(seconds_remaining = remaining, "Trading paused");
                return Err(RiskVeto::Paused {
                    seconds_remaining: remaining,
                });
            }
        }

        // Check daily loss limit
        if state.daily_pnl < Decimal::ZERO && state.daily_pnl.abs() >= self.daily_loss_limit {
            warn!(
                daily_pnl = %state.daily_pnl,
                limit = %self.daily_loss_limit,
                "Daily loss limit reached"
            );
            return Err(RiskVeto::DailyLossLimitReached);
        }

        // Check consecutive losses
        if state.consecutive_losses >= self.max_consecutive_losses {
            warn!(
                consecutive = state.consecutive_losses,
                max = self.max_consecutive_losses,
                "Consecutive loss circuit breaker triggered"
            );
            return Err(RiskVeto::ConsecutiveLossPause);
        }

        // Check bankroll
        if state.bankroll < self.max_bet_usd {
            warn!(
                bankroll = %state.bankroll,
                min_required = %self.max_bet_usd,
                "Insufficient bankroll"
            );
            return Err(RiskVeto::InsufficientBankroll);
        }

        Ok(())
    }

    /// Calculate position size (flat betting for v1, half-Kelly later).
    pub fn position_size(&self, _state: &BotState) -> Decimal {
        self.max_bet_usd
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
}
