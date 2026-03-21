use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::orderbook::OrderbookState;
use crate::state::BotState;
use crate::types::{Direction, MarketInfo};

/// Everything needed to place and manage an orderbook-aware trade.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TradeDecision {
    pub direction: Direction,
    pub token_id: String,
    pub initial_price: Decimal,
    pub max_price: Decimal,
    pub delta_pct: Decimal,
    pub contracts: Decimal,
    pub bet_size_usd: Decimal,
    pub best_ask_at_entry: Decimal,
    pub skip_reason: Option<String>,
}

pub struct SignalEngine;

impl SignalEngine {
    pub fn new(_config: &AppConfig) -> Self {
        Self
    }

    /// Evaluate whether to trade this window, and at what price.
    /// Returns None with a logged reason if delta too low, book too expensive, or no edge.
    pub fn evaluate_trade(
        &self,
        state: &BotState,
        market: &MarketInfo,
        orderbook: &OrderbookState,
        config: &AppConfig,
        max_bet_usd: Decimal,
    ) -> Option<TradeDecision> {
        if state.btc_price.is_zero() || state.window_open_price.is_zero() {
            warn!("No BTC price data for trade evaluation");
            return None;
        }
        if !state.has_fresh_price() {
            warn!(age_ms = ?state.btc_price_age_ms(), "BTC price stale");
            return None;
        }

        let (direction, abs_delta_pct) = compute_delta(state.btc_price, state.window_open_price);
        let min_delta = Decimal::try_from(config.pricing.min_delta_pct).unwrap_or(dec!(0.07));

        if abs_delta_pct < min_delta {
            info!(
                delta = %abs_delta_pct,
                min = %min_delta,
                "Delta below minimum, skipping"
            );
            return None;
        }

        let delta_f64 = abs_delta_pct.try_into().unwrap_or(0.0f64);
        let max_price_for_delta = Decimal::try_from(config.max_price_for_delta(delta_f64))
            .unwrap_or(dec!(0.85));

        let undercut = Decimal::try_from(config.pricing.undercut_offset).unwrap_or(dec!(0.03));
        let min_entry = Decimal::try_from(config.pricing.min_entry_price).unwrap_or(dec!(0.55));

        let mut initial_price = (orderbook.best_ask - undercut).round_dp(2);

        if initial_price < min_entry {
            debug!(raw = %initial_price, clamped = %min_entry, "Clamped to min_entry_price");
            initial_price = min_entry;
        }
        if initial_price > max_price_for_delta {
            info!(
                initial = %initial_price,
                ceiling = %max_price_for_delta,
                best_ask = %orderbook.best_ask,
                delta = %abs_delta_pct,
                "Book too expensive for this delta level"
            );
            return None;
        }

        let token_id = match direction {
            Direction::Up => market.up_token_id.clone(),
            Direction::Down => market.down_token_id.clone(),
        };

        let contracts = (max_bet_usd / initial_price)
            .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);
        let bet_size_usd = contracts * initial_price;

        if contracts < Decimal::ONE {
            info!("Position size < 1 contract, skipping");
            return None;
        }

        info!(
            direction = %direction,
            delta = %abs_delta_pct,
            best_ask = %orderbook.best_ask,
            initial_price = %initial_price,
            ceiling = %max_price_for_delta,
            contracts = %contracts,
            "Trade decision generated"
        );

        Some(TradeDecision {
            direction,
            token_id,
            initial_price,
            max_price: max_price_for_delta,
            delta_pct: abs_delta_pct,
            contracts,
            bet_size_usd,
            best_ask_at_entry: orderbook.best_ask,
            skip_reason: None,
        })
    }
}

/// Returns (direction, abs_delta_pct) from two BTC prices.
pub fn compute_delta(current: Decimal, open: Decimal) -> (Direction, Decimal) {
    let delta = current - open;
    let delta_pct = if open.is_zero() {
        Decimal::ZERO
    } else {
        (delta / open) * dec!(100)
    };
    let direction = if delta >= Decimal::ZERO {
        Direction::Up
    } else {
        Direction::Down
    };
    (direction, delta_pct.abs())
}
