use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::types::{MarketInfo, WindowInfo};

const WINDOW_SECONDS: u64 = 300;

pub fn epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_secs()
}

pub fn current_window_ts() -> u64 {
    let now = epoch_secs();
    now - (now % WINDOW_SECONDS)
}

pub fn seconds_until_close() -> u64 {
    let now = epoch_secs();
    let window_ts = now - (now % WINDOW_SECONDS);
    let close_time = window_ts + WINDOW_SECONDS;
    close_time.saturating_sub(now)
}

pub fn seconds_until_next_window() -> u64 {
    seconds_until_close()
}

pub fn market_slug(window_ts: u64) -> String {
    format!("btc-updown-5m-{window_ts}")
}

pub fn current_window_info() -> WindowInfo {
    let window_ts = current_window_ts();
    WindowInfo {
        window_ts,
        slug: market_slug(window_ts),
        seconds_remaining: seconds_until_close(),
    }
}

/// Resolve market metadata from Gamma API using the market slug.
///
/// Returns token IDs and condition ID needed for order placement.
pub async fn resolve_market(slug: &str) -> Result<MarketInfo> {
    use polymarket_client_sdk::gamma::Client as GammaClient;
    use polymarket_client_sdk::gamma::types::request::MarketBySlugRequest;

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
        .unwrap_or_else(|| Decimal::new(1, 2)); // default 0.01

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
