use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

use crate::types::MarketInfo;

// ── CLOB API types for definitive token→outcome mapping ──

#[derive(Debug, Deserialize)]
struct ClobMarketResponse {
    tokens: Vec<ClobToken>,
}

#[derive(Debug, Deserialize)]
struct ClobToken {
    token_id: String,
    outcome: String,
    #[allow(dead_code)]
    winner: bool,
}

// ── Gamma API types for priceToBeat extraction ──

#[derive(Debug, Deserialize)]
struct GammaMarketResponse {
    events: Option<Vec<GammaEvent>>,
}

#[derive(Debug, Deserialize)]
struct GammaEvent {
    #[serde(rename = "eventMetadata")]
    event_metadata: Option<GammaEventMetadata>,
}

#[derive(Debug, Deserialize)]
struct GammaEventMetadata {
    #[serde(rename = "priceToBeat")]
    price_to_beat: Option<f64>,
}

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

/// Compute the next window's start timestamp for pre-resolution.
pub fn next_window_ts(window_seconds: u64) -> u64 {
    let (current_ts, _) = current_window(window_seconds);
    current_ts + window_seconds
}

/// Fetch the definitive token→outcome mapping from the CLOB API.
/// The CLOB response attaches `outcome` ("Yes"/"No") directly to each `token_id`.
/// For Up/Down markets: Yes = UP, No = DOWN.
async fn fetch_clob_token_mapping(clob_url: &str, condition_id: &str) -> Result<(String, String)> {
    let url = format!("{}/markets/{}", clob_url, condition_id);

    let resp: ClobMarketResponse = reqwest::get(&url)
        .await
        .with_context(|| format!("CLOB market fetch failed for {condition_id}"))?
        .json()
        .await
        .with_context(|| format!("CLOB market JSON parse failed for {condition_id}"))?;

    let mut up_token: Option<String> = None;
    let mut down_token: Option<String> = None;

    for token in &resp.tokens {
        match token.outcome.to_lowercase().as_str() {
            "yes" | "up" => up_token = Some(token.token_id.clone()),
            "no" | "down" => down_token = Some(token.token_id.clone()),
            _ => {}
        }
    }

    let up = up_token
        .ok_or_else(|| anyhow::anyhow!("No Yes/UP token in CLOB response for {condition_id}"))?;
    let down = down_token
        .ok_or_else(|| anyhow::anyhow!("No No/DOWN token in CLOB response for {condition_id}"))?;

    Ok((up, down))
}

/// Resolve market metadata from Gamma API, then fetch definitive token mapping from CLOB API.
pub async fn resolve_market(slug: &str, clob_url: &str) -> Result<MarketInfo> {
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

    // Strip Debug formatting quotes for CLOB API URL
    let clob_cid = condition_id.trim_matches('"').to_string();

    // PRIMARY: CLOB API provides definitive token→outcome mapping
    let (up_token_id, down_token_id) = match fetch_clob_token_mapping(clob_url, &clob_cid).await {
        Ok((up, down)) => {
            info!(
                slug = %slug,
                condition_id = %clob_cid,
                up_token = %&up[..up.len().min(16)],
                down_token = %&down[..down.len().min(16)],
                "CLOB token mapping: Yes(UP) / No(DOWN)"
            );
            (up, down)
        }
        Err(e) => {
            warn!(
                slug = %slug,
                condition_id = %clob_cid,
                error = %e,
                "CLOB token mapping failed — falling back to Gamma array order"
            );
            (token_ids[0].to_string(), token_ids[1].to_string())
        }
    };

    let accepting = market.accepting_orders.unwrap_or(false);
    let neg_risk = market.neg_risk.unwrap_or(false);
    let tick_size = market
        .order_price_min_tick_size
        .unwrap_or_else(|| Decimal::new(1, 2));

    info!(
        slug = %slug,
        up_token = %up_token_id,
        down_token = %down_token_id,
        accepting = accepting,
        "Resolved market tokens (CLOB-mapped)"
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

/// Resolve market with retries for when the market hasn't been created yet.
pub async fn resolve_market_with_retry(slug: &str, clob_url: &str, max_retries: u32) -> Result<MarketInfo> {
    for attempt in 0..max_retries {
        match resolve_market(slug, clob_url).await {
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

/// Fetch the "Price to Beat" (reference/open price) from Polymarket's Gamma API.
/// Returns `None` if the field isn't available yet or the request fails.
pub async fn fetch_price_to_beat(slug: &str) -> Option<Decimal> {
    let url = format!("https://gamma-api.polymarket.com/markets?slug={}", slug);
    let resp = match reqwest::get(&url).await {
        Ok(r) => r,
        Err(e) => {
            warn!(slug = %slug, error = %e, "Failed to fetch priceToBeat from Gamma API");
            return None;
        }
    };

    let markets: Vec<GammaMarketResponse> = match resp.json().await {
        Ok(m) => m,
        Err(e) => {
            warn!(slug = %slug, error = %e, "Failed to parse Gamma API response for priceToBeat");
            return None;
        }
    };

    let ptb = markets
        .first()?
        .events
        .as_ref()?
        .first()?
        .event_metadata
        .as_ref()?
        .price_to_beat?;

    let decimal = Decimal::from_str(&format!("{ptb}")).ok()?;
    info!(slug = %slug, price_to_beat = %decimal, "Fetched priceToBeat from Polymarket");
    Some(decimal)
}
