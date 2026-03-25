use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Instant;

use crate::feeds::TickEntry;
use crate::types::Direction;

/// Returns (direction, abs_delta_pct) from two asset prices.
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

/// Configurable weights for the composite signal score.
#[derive(Debug, Clone)]
pub struct SignalWeights {
    pub delta: f64,
    pub velocity: f64,
    pub volatility: f64,
    pub acceleration: f64,
}

/// All computed signal features for a single scan evaluation.
#[derive(Debug, Clone, Default)]
pub struct SignalBundle {
    pub direction: Option<Direction>,
    pub delta_pct: f64,
    pub velocity_5s: f64,
    pub velocity_15s: f64,
    pub acceleration: f64,
    pub range_30s: f64,
    pub signal_score: f64,
}

/// Compute velocity: percentage price change over `lookback` seconds.
/// Finds the tick closest to `lookback` seconds ago (within ±1s tolerance).
fn velocity(ticks: &[TickEntry], current: Decimal, open: Decimal, lookback_secs: u64) -> f64 {
    if open.is_zero() || ticks.is_empty() {
        return 0.0;
    }
    let now = Instant::now();
    let target = now - std::time::Duration::from_secs(lookback_secs);

    let past_price = ticks
        .iter()
        .filter(|t| t.ts <= target)
        .last()
        .map(|t| t.price)
        .unwrap_or(current);

    let delta: f64 = (current - past_price).try_into().unwrap_or(0.0);
    let open_f64: f64 = open.try_into().unwrap_or(1.0);
    (delta / open_f64) * 100.0
}

/// Compute price range over the last `lookback` seconds as a percentage of open.
fn price_range(ticks: &[TickEntry], open: Decimal, lookback_secs: u64) -> f64 {
    if open.is_zero() || ticks.is_empty() {
        return 0.0;
    }
    let cutoff = Instant::now() - std::time::Duration::from_secs(lookback_secs);
    let recent: Vec<Decimal> = ticks
        .iter()
        .filter(|t| t.ts >= cutoff)
        .map(|t| t.price)
        .collect();
    if recent.is_empty() {
        return 0.0;
    }
    let high = recent.iter().copied().max().unwrap_or(open);
    let low = recent.iter().copied().min().unwrap_or(open);
    let range: f64 = (high - low).try_into().unwrap_or(0.0);
    let open_f64: f64 = open.try_into().unwrap_or(1.0);
    (range / open_f64) * 100.0
}

/// Build a full SignalBundle from current prices and tick history.
pub fn compute_signals(
    current: Decimal,
    open: Decimal,
    ticks: &[TickEntry],
    weights: &SignalWeights,
) -> SignalBundle {
    let (direction, abs_delta) = compute_delta(current, open);
    let delta_pct: f64 = abs_delta.try_into().unwrap_or(0.0);

    let v5 = velocity(ticks, current, open, 5);
    let v15 = velocity(ticks, current, open, 15);

    // Sign the velocity according to the direction of the delta.
    // Positive velocity = price moving in the direction of the trade.
    let sign = match direction {
        Direction::Up => 1.0,
        Direction::Down => -1.0,
    };
    let signed_v5 = v5 * sign;
    let signed_v15 = v15 * sign;

    let accel = signed_v5 - signed_v15;
    let range = price_range(ticks, open, 30);

    let score = weights.delta * delta_pct
        + weights.velocity * signed_v5.abs()
        + weights.volatility * range
        + weights.acceleration * accel.max(0.0);

    SignalBundle {
        direction: Some(direction),
        delta_pct,
        velocity_5s: signed_v5,
        velocity_15s: signed_v15,
        acceleration: accel,
        range_30s: range,
        signal_score: score,
    }
}
