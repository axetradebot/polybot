use rust_decimal::Decimal;
use rust_decimal_macros::dec;

struct RiskManager {
    daily_loss_limit: Decimal,
    max_consecutive_losses: u32,
    max_bet_usd: Decimal,
}

struct RiskState {
    bankroll: Decimal,
    daily_pnl: Decimal,
    consecutive_losses: u32,
    paused: bool,
    daily_wins: u32,
    daily_losses: u32,
}

impl RiskManager {
    fn new() -> Self {
        Self {
            daily_loss_limit: dec!(25),
            max_consecutive_losses: 5,
            max_bet_usd: dec!(5),
        }
    }

    fn can_trade(&self, state: &RiskState) -> bool {
        if state.paused {
            return false;
        }
        if state.daily_pnl < Decimal::ZERO && state.daily_pnl.abs() >= self.daily_loss_limit {
            return false;
        }
        if state.consecutive_losses >= self.max_consecutive_losses {
            return false;
        }
        if state.bankroll < self.max_bet_usd {
            return false;
        }
        true
    }

    fn position_size(&self) -> Decimal {
        self.max_bet_usd
    }
}

impl RiskState {
    fn fresh() -> Self {
        Self {
            bankroll: dec!(500),
            daily_pnl: Decimal::ZERO,
            consecutive_losses: 0,
            paused: false,
            daily_wins: 0,
            daily_losses: 0,
        }
    }

    fn record_win(&mut self, pnl: Decimal) {
        self.daily_pnl += pnl;
        self.daily_wins += 1;
        self.consecutive_losses = 0;
        self.bankroll += pnl;
    }

    fn record_loss(&mut self, pnl: Decimal) {
        self.daily_pnl += pnl;
        self.daily_losses += 1;
        self.consecutive_losses += 1;
        self.bankroll += pnl;
    }
}

#[test]
fn test_risk_allows_normal_trade() {
    let rm = RiskManager::new();
    let state = RiskState::fresh();
    assert!(rm.can_trade(&state));
}

#[test]
fn test_risk_blocks_daily_loss_limit() {
    let rm = RiskManager::new();
    let mut state = RiskState::fresh();
    state.daily_pnl = dec!(-26);
    assert!(!rm.can_trade(&state));
}

#[test]
fn test_risk_blocks_consecutive_losses() {
    let rm = RiskManager::new();
    let mut state = RiskState::fresh();
    state.consecutive_losses = 5;
    assert!(!rm.can_trade(&state));
}

#[test]
fn test_risk_blocks_insufficient_bankroll() {
    let rm = RiskManager::new();
    let mut state = RiskState::fresh();
    state.bankroll = dec!(3);
    assert!(!rm.can_trade(&state));
}

#[test]
fn test_position_size_flat() {
    let rm = RiskManager::new();
    assert_eq!(rm.position_size(), dec!(5.0));
}

#[test]
fn test_record_win_updates_state() {
    let mut state = RiskState::fresh();
    state.record_win(dec!(0.18));
    assert_eq!(state.daily_wins, 1);
    assert_eq!(state.consecutive_losses, 0);
    assert_eq!(state.daily_pnl, dec!(0.18));
    assert_eq!(state.bankroll, dec!(500.18));
}

#[test]
fn test_record_loss_updates_state() {
    let mut state = RiskState::fresh();
    state.record_loss(dec!(-0.82));
    assert_eq!(state.daily_losses, 1);
    assert_eq!(state.consecutive_losses, 1);
    assert_eq!(state.daily_pnl, dec!(-0.82));
    assert_eq!(state.bankroll, dec!(499.18));
}
