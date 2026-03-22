use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::types::MarketInfo;

pub fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

/// Generic window computation for any window size.
/// Returns (window_ts, seconds_remaining).
pub fn current_window(window_seconds: u64) -> (u64, u64) {
    let now = epoch_secs();
    let window_ts = now - (now % window_seconds);
    let seconds_remaining = (window_ts + window_seconds).saturating_sub(now);
    (window_ts, seconds_remaining)
}

/// Build a market slug from prefix and window timestamp.
pub fn build_slug(prefix: &str, window_ts: u64) -> String {
    format!("{}-{}", prefix, window_ts)
}

/// Resolve market metadata from Gamma API using the market slug.
pub async fn resolve_market(slug: &str) -> Result<MarketInfo> {
    use polymarket_client_sdk::gamma::types::request::MarketBySlugRequest;
    use polymarket_client_sdk::gamma::Client as GammaClient;

    let gamma = GammaClient::default();
    let request = MarketBySlugRequest::builder().slug(slug).build();
    let market = gamma
        .market_by_slug(&request)
        .await
        .with_context(|| format!("Failed to resolve market slug: {slug}"))?;

    let condition_id = market
        .condition_id
        .map(|c| format!("{c:?}"))
        .unwrap_or_default();

    let token_ids = market
        .clob_token_ids
        .as_ref()
        .context("Market has no clob_token_ids")?;

    if token_ids.len() < 2 {
        anyhow::bail!("Market has fewer than 2 token IDs: {slug}");
    }

    let accepting = market.accepting_orders.unwrap_or(false);
    let neg_risk = market.neg_risk.unwrap_or(false);
    let tick_size = market
        .order_price_min_tick_size
        .unwrap_or_else(|| Decimal::new(1, 2));

    // Determine correct UP/DOWN mapping using the outcomes field.
    // The outcomes array (e.g. ["Up","Down"] or ["Down","Up"]) corresponds
    // positionally to clob_token_ids. Without checking, we'd assume [0]=Up
    // which is WRONG if Polymarket returns ["Down","Up"].
    let (up_idx, down_idx) = determine_token_indices(market.outcomes.as_ref());

    let up_token_id = token_ids[up_idx].to_string();
    let down_token_id = token_ids[down_idx].to_string();

    info!(
        slug = %slug,
        outcomes = ?market.outcomes,
        up_idx,
        down_idx,
        up_token = %up_token_id,
        down_token = %down_token_id,
        accepting = accepting,
        "Resolved market (outcome-aware token mapping)"
    );

    let info = MarketInfo {
        condition_id,
        up_token_id,
        down_token_id,
        slug: slug.to_string(),
        accepting_orders: accepting,
        neg_risk,
        tick_size,
    };

    Ok(info)
}

/// Determine the correct array indices for UP and DOWN tokens from the outcomes list.
/// Returns (up_index, down_index). Falls back to (0, 1) if outcomes are missing or unrecognized.
fn determine_token_indices(outcomes: Option<&Vec<String>>) -> (usize, usize) {
    if let Some(outcomes) = outcomes {
        let up_pos = outcomes.iter().position(|o| {
            let lower = o.to_lowercase();
            lower == "up" || lower == "yes"
        });
        let down_pos = outcomes.iter().position(|o| {
            let lower = o.to_lowercase();
            lower == "down" || lower == "no"
        });
        match (up_pos, down_pos) {
            (Some(u), Some(d)) => {
                info!(up_idx = u, down_idx = d, outcomes = ?outcomes, "Token index mapping from outcomes");
                (u, d)
            }
            _ => {
                warn!(outcomes = ?outcomes, "Could not match Up/Down in outcomes, falling back to [0]=Up [1]=Down");
                (0, 1)
            }
        }
    } else {
        warn!("No outcomes field from Gamma API — assuming [0]=Up [1]=Down");
        (0, 1)
    }
}

/// Resolve market with retries for when the market hasn't been created yet.
pub async fn resolve_market_with_retry(slug: &str, max_retries: u32) -> Result<MarketInfo> {
    for attempt in 0..max_retries {
        match resolve_market(slug).await {
            Ok(info) => return Ok(info),
            Err(e) => {
                if attempt < max_retries - 1 {
                    warn!(
                        attempt = attempt + 1,
                        max_retries,
                        error = %e,
                        slug = %slug,
                        "Market not yet available, retrying..."
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                } else {
                    return Err(e);
                }
            }
        }
    }
    unreachable!()
}
