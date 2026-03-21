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

    let info = MarketInfo {
        condition_id,
        up_token_id: token_ids[0].to_string(),
        down_token_id: token_ids[1].to_string(),
        slug: slug.to_string(),
        accepting_orders: accepting,
        neg_risk,
        tick_size,
    };

    info!(
        slug = %slug,
        up_token = %info.up_token_id,
        down_token = %info.down_token_id,
        accepting = accepting,
        "Resolved market"
    );

    Ok(info)
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
