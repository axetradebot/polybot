use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};

use crate::config::{AppConfig, EntryTier, SweepConfig};
use crate::state::BotState;
use crate::types::{Direction, MarketInfo, Signal};

pub struct SignalEngine {
    min_delta_pct: Decimal,
    max_entry_price: Decimal,
    pricing_tiers: Vec<PricingTier>,
}

struct PricingTier {
    min_delta: Decimal,
    max_delta: Decimal,
    target_price: Decimal,
}

/// Result of a tier evaluation: everything needed to place the order.
#[derive(Debug, Clone)]
pub struct TierDecision {
    pub tier_name: String,
    pub time_before_close_s: u64,
    pub direction: Direction,
    /// Final target price (estimated fair value + tier price_offset).
    pub target_price: Decimal,
    pub delta_pct: Decimal,
    pub token_id: String,
    pub contracts: Decimal,
    pub bet_size_usd: Decimal,
}

/// Output of sweep evaluation: direction + price ladder to walk through.
#[derive(Debug, Clone)]
pub struct SweepPlan {
    pub direction: Direction,
    pub delta_pct: Decimal,
    pub fair_value: Decimal,
    pub token_id: String,
    /// Ascending price levels from cheapest (most profit) to most aggressive (most fill).
    pub prices: Vec<Decimal>,
    pub max_bet_usd: Decimal,
}

impl SignalEngine {
    pub fn new(config: &AppConfig) -> Self {
        let pricing_tiers = config
            .signal
            .delta_pricing
            .tiers
            .iter()
            .map(|t| PricingTier {
                min_delta: Decimal::try_from(t[0]).unwrap_or_default(),
                max_delta: Decimal::try_from(t[1]).unwrap_or_default(),
                target_price: Decimal::try_from(t[2]).unwrap_or_default(),
            })
            .collect();

        Self {
            min_delta_pct: Decimal::try_from(config.signal.min_delta_pct)
                .unwrap_or(dec!(0.02)),
            max_entry_price: Decimal::try_from(config.signal.max_entry_price)
                .unwrap_or(dec!(0.92)),
            pricing_tiers,
        }
    }

    // ── Single-entry (legacy) path ────────────────────────────────────────────

    /// Evaluate the current state and produce a signal if conditions are met.
    /// Used when `entry_tiers.enabled = false`.
    pub fn evaluate(&self, state: &BotState, market: &MarketInfo) -> Option<Signal> {
        if state.btc_price.is_zero() || state.window_open_price.is_zero() {
            warn!("No BTC price data available");
            return None;
        }

        if !state.has_fresh_price() {
            warn!(
                age_ms = ?state.btc_price_age_ms(),
                "BTC price is stale"
            );
            return None;
        }

        let (direction, abs_delta_pct) = compute_delta(state.btc_price, state.window_open_price);

        info!(
            btc_price = %state.btc_price,
            open_price = %state.window_open_price,
            delta_pct = %abs_delta_pct,
            "Signal evaluation"
        );

        if abs_delta_pct < self.min_delta_pct {
            info!(
                abs_delta = %abs_delta_pct,
                min = %self.min_delta_pct,
                "Delta below minimum threshold, skipping"
            );
            return None;
        }

        let target_price = self.price_for_delta(abs_delta_pct)?;

        if target_price > self.max_entry_price {
            info!(
                target = %target_price,
                max = %self.max_entry_price,
                "Target price exceeds max entry, skipping"
            );
            return None;
        }

        let token_id = match direction {
            Direction::Up => market.up_token_id.clone(),
            Direction::Down => market.down_token_id.clone(),
        };

        let signal = Signal {
            direction,
            delta_pct: abs_delta_pct,
            target_price,
            token_id,
            window_ts: state.current_window_ts,
        };

        info!(
            direction = %signal.direction,
            delta_pct = %signal.delta_pct,
            target_price = %signal.target_price,
            "Signal generated"
        );

        Some(signal)
    }

    // ── Tiered-entry path ─────────────────────────────────────────────────────

    /// Determine which entry tier (if any) should fire right now.
    ///
    /// `seconds_remaining` is seconds until window close.
    /// The active tier is the one with the largest `time_before_close_s`
    /// that is still <= `seconds_remaining` (i.e. we have just crossed its threshold).
    ///
    /// Returns `None` if:
    /// - No tier threshold has been crossed yet (`seconds_remaining` > all tier times)
    /// - Past all tiers (seconds_remaining < last tier's time AND that tier already ran)
    /// - Delta is below the tier's `min_delta_pct`
    /// - Computed target price > max_entry_price or < 0.50
    pub fn evaluate_tier(
        &self,
        seconds_remaining: u64,
        state: &BotState,
        market: &MarketInfo,
        entry_tiers: &[EntryTier],
        max_bet_usd: Decimal,
    ) -> Option<TierDecision> {
        if state.btc_price.is_zero() || state.window_open_price.is_zero() {
            warn!("No BTC price data available for tier evaluation");
            return None;
        }

        if !state.has_fresh_price() {
            warn!(age_ms = ?state.btc_price_age_ms(), "BTC price is stale");
            return None;
        }

        let (direction, abs_delta_pct) = compute_delta(state.btc_price, state.window_open_price);

        // Find the "most recently crossed" tier:
        // Among all tiers whose threshold has been passed (seconds_remaining <= time),
        // return the one with the SMALLEST time (closest to now).
        // Tiers are sorted descending, so this is the rightmost qualifying tier.
        let tier = entry_tiers
            .iter()
            .filter(|t| seconds_remaining <= t.time_before_close_s)
            .min_by_key(|t| t.time_before_close_s)?;

        info!(
            tier = %tier.name,
            seconds_remaining,
            delta_pct = %abs_delta_pct,
            min_required = tier.min_delta_pct,
            "Evaluating tier"
        );

        if abs_delta_pct < Decimal::try_from(tier.min_delta_pct).unwrap_or(dec!(0.02)) {
            info!(
                tier = %tier.name,
                delta_pct = %abs_delta_pct,
                "Delta below tier threshold, skipping tier"
            );
            return None;
        }

        let fair_value = self.price_for_delta(abs_delta_pct)?;
        let offset = Decimal::try_from(tier.price_offset).unwrap_or_default();
        let mut target_price = fair_value + offset;

        // Clamp
        let min_price = dec!(0.50);
        if target_price < min_price {
            debug!(raw = %target_price, clamped = %min_price, "Target price clamped to minimum");
            target_price = min_price;
        }

        if target_price > self.max_entry_price {
            info!(
                tier = %tier.name,
                target = %target_price,
                max = %self.max_entry_price,
                "Target price exceeds max entry for tier, skipping"
            );
            return None;
        }

        let token_id = match direction {
            Direction::Up => market.up_token_id.clone(),
            Direction::Down => market.down_token_id.clone(),
        };

        let contracts = (max_bet_usd / target_price)
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero);

        let bet_size_usd = contracts * target_price;

        info!(
            tier = %tier.name,
            direction = %direction,
            fair_value = %fair_value,
            offset = %offset,
            target_price = %target_price,
            delta_pct = %abs_delta_pct,
            contracts = %contracts,
            "Tier decision"
        );

        Some(TierDecision {
            tier_name: tier.name.clone(),
            time_before_close_s: tier.time_before_close_s,
            direction,
            target_price,
            delta_pct: abs_delta_pct,
            token_id,
            contracts,
            bet_size_usd,
        })
    }

    // ── Sweep path ────────────────────────────────────────────────────────────

    /// Returns a SweepPlan with direction, fair_value and a price ladder.
    /// Called once at the start of the sweep window.
    pub fn evaluate_sweep(
        &self,
        state: &BotState,
        market: &MarketInfo,
        sweep: &SweepConfig,
        max_bet_usd: Decimal,
    ) -> Option<SweepPlan> {
        if state.btc_price.is_zero() || state.window_open_price.is_zero() {
            warn!("No BTC price for sweep evaluation");
            return None;
        }
        if !state.has_fresh_price() {
            warn!(age_ms = ?state.btc_price_age_ms(), "BTC price stale for sweep");
            return None;
        }

        let (direction, abs_delta_pct) = compute_delta(state.btc_price, state.window_open_price);
        let min_delta = Decimal::try_from(sweep.min_delta_pct).unwrap_or(dec!(0.01));

        if abs_delta_pct < min_delta {
            info!(
                delta = %abs_delta_pct,
                min = %min_delta,
                "Sweep: delta below minimum, skipping"
            );
            return None;
        }

        let fair_value = self.price_for_delta(abs_delta_pct).unwrap_or(dec!(0.55));

        let start_off = Decimal::try_from(sweep.start_offset).unwrap_or(dec!(-0.12));
        let end_off = Decimal::try_from(sweep.end_offset).unwrap_or(dec!(0.02));
        let steps = sweep.steps.max(2);

        let step_size = (end_off - start_off) / Decimal::from(steps - 1);
        let min_price = dec!(0.01);
        let max_price = self.max_entry_price;

        let prices: Vec<Decimal> = (0..steps)
            .map(|i| {
                let p = (fair_value + start_off + step_size * Decimal::from(i))
                    .round_dp(2);
                p.max(min_price).min(max_price)
            })
            .collect();

        let token_id = match direction {
            Direction::Up => market.up_token_id.clone(),
            Direction::Down => market.down_token_id.clone(),
        };

        info!(
            direction = %direction,
            delta_pct = %abs_delta_pct,
            fair_value = %fair_value,
            prices = ?prices,
            "Sweep plan generated"
        );

        Some(SweepPlan {
            direction,
            delta_pct: abs_delta_pct,
            fair_value,
            token_id,
            prices,
            max_bet_usd,
        })
    }

    // ── Shared helpers ────────────────────────────────────────────────────────

    fn price_for_delta(&self, abs_delta: Decimal) -> Option<Decimal> {
        for tier in &self.pricing_tiers {
            if abs_delta >= tier.min_delta && abs_delta < tier.max_delta {
                debug!(
                    delta = %abs_delta,
                    price = %tier.target_price,
                    "Matched pricing tier"
                );
                return Some(tier.target_price);
            }
        }

        if let Some(last) = self.pricing_tiers.last() {
            if abs_delta >= last.min_delta {
                return Some(last.target_price);
            }
        }

        warn!(delta = %abs_delta, "No pricing tier matched");
        None
    }
}

/// Returns (direction, abs_delta_pct) from two BTC prices.
fn compute_delta(current: Decimal, open: Decimal) -> (Direction, Decimal) {
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
