use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration;
use tracing::info;

use crate::signal::TierDecision;
use crate::types::{Direction, OrderResult};

/// Simulate a maker limit order fill for paper trading.
/// `tier_name` and `seconds_remaining` are recorded but don't affect simulation logic.
pub async fn place_paper_order(
    decision: &TierDecision,
    max_bet_usd: Decimal,
) -> Result<OrderResult> {
    let size = (max_bet_usd / decision.target_price)
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);

    if size < Decimal::ONE {
        anyhow::bail!("Computed size < 1 contract, skipping");
    }

    let cost = size * decision.target_price;

    info!(
        tier = %decision.tier_name,
        direction = %decision.direction,
        price = %decision.target_price,
        size = %size,
        cost = %cost,
        token = %decision.token_id,
        "[PAPER] Placing simulated maker limit order"
    );

    let order_id = format!(
        "paper-{}-{}-{}-{}",
        decision.tier_name,
        decision.direction,
        decision.time_before_close_s,
        chrono::Utc::now().timestamp_millis()
    );

    // Fill probability varies by tier timing and signal strength.
    // Earlier tiers have lower fill rates (cheaper prices), later tiers higher.
    let fill_probability = fill_prob_for_tier(&decision.tier_name, decision.delta_pct);

    // Simulate network round-trip latency.
    tokio::time::sleep(Duration::from_millis(120)).await;

    let simulated_fill = rand_fill(fill_probability);

    info!(
        order_id = %order_id,
        filled = simulated_fill,
        fill_probability,
        "[PAPER] Simulated order result"
    );

    Ok(OrderResult {
        order_id,
        filled: simulated_fill,
        fill_price: if simulated_fill {
            decision.target_price
        } else {
            Decimal::ZERO
        },
        fill_size: if simulated_fill { size } else { Decimal::ZERO },
    })
}

/// Legacy paper order using a raw Signal (backward compat for single-entry mode).
pub async fn place_paper_order_signal(
    token_id: &str,
    direction: Direction,
    target_price: Decimal,
    delta_pct: Decimal,
    window_ts: u64,
    tier_name: &str,
    max_bet_usd: Decimal,
) -> Result<OrderResult> {
    let size = (max_bet_usd / target_price)
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);

    if size < Decimal::ONE {
        anyhow::bail!("Computed size < 1 contract, skipping");
    }

    let cost = size * target_price;

    info!(
        tier = %tier_name,
        direction = %direction,
        price = %target_price,
        size = %size,
        cost = %cost,
        token = %token_id,
        "[PAPER] Placing simulated maker limit order (signal path)"
    );

    let order_id = format!(
        "paper-{tier_name}-{direction}-{window_ts}-{}",
        chrono::Utc::now().timestamp_millis()
    );

    let fill_probability = fill_prob_for_tier(tier_name, delta_pct);
    tokio::time::sleep(Duration::from_millis(120)).await;
    let simulated_fill = rand_fill(fill_probability);

    info!(order_id = %order_id, filled = simulated_fill, "[PAPER] Simulated order result");

    Ok(OrderResult {
        order_id,
        filled: simulated_fill,
        fill_price: if simulated_fill { target_price } else { Decimal::ZERO },
        fill_size: if simulated_fill { size } else { Decimal::ZERO },
    })
}

fn fill_prob_for_tier(tier_name: &str, delta_pct: Decimal) -> f64 {
    // Base probability by tier: early = cheapest price, lowest fill rate
    let base = match tier_name {
        "early" => 0.35,
        "mid" => 0.55,
        "late" => 0.72,
        _ => 0.60, // "single"
    };

    // Boost by signal strength
    let boost = if delta_pct > dec!(0.10) {
        0.15
    } else if delta_pct > dec!(0.05) {
        0.08
    } else {
        0.0
    };

    let result: f64 = base + boost;
    result.min(0.90)
}

fn rand_fill(probability: f64) -> bool {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (seed as f64 / u32::MAX as f64) < probability
}
