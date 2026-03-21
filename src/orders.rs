use anyhow::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::time::Duration;
use tracing::info;

use crate::signal::TradeDecision;
use crate::types::OrderResult;

/// Simulate a maker limit order fill for paper trading using a TradeDecision.
#[allow(dead_code)]
pub async fn place_paper_order(
    decision: &TradeDecision,
    max_bet_usd: Decimal,
) -> Result<OrderResult> {
    let size = (max_bet_usd / decision.initial_price)
        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);

    if size < Decimal::ONE {
        anyhow::bail!("Computed size < 1 contract, skipping");
    }

    let cost = size * decision.initial_price;

    info!(
        direction = %decision.direction,
        price = %decision.initial_price,
        size = %size,
        cost = %cost,
        "[PAPER] Placing simulated maker limit order"
    );

    let order_id = format!(
        "paper-ob-{}-{}",
        decision.direction,
        chrono::Utc::now().timestamp_millis()
    );

    let fill_probability = fill_prob(decision.delta_pct, decision.initial_price);

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
            decision.initial_price
        } else {
            Decimal::ZERO
        },
        fill_size: if simulated_fill { size } else { Decimal::ZERO },
    })
}

fn fill_prob(delta_pct: Decimal, price: Decimal) -> f64 {
    let base: f64 = if price >= dec!(0.80) {
        0.75
    } else if price >= dec!(0.70) {
        0.55
    } else {
        0.35
    };

    let boost: f64 = if delta_pct > dec!(0.15) {
        0.15
    } else if delta_pct > dec!(0.10) {
        0.08
    } else {
        0.0
    };

    (base + boost).min(0.90)
}

fn rand_fill(probability: f64) -> bool {
    use std::time::SystemTime;
    let seed = SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    (seed as f64 / u32::MAX as f64) < probability
}
