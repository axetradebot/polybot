use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
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

#[derive(Deserialize)]
struct ClobMarketResponse {
    #[serde(default)]
    tokens: Vec<ClobToken>,
}

#[derive(Deserialize)]
struct ClobToken {
    token_id: String,
    outcome: String,
}

static HTTP_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

fn http_client() -> &'static reqwest::Client {
    HTTP_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client")
    })
}

/// Fetch the authoritative token-to-outcome mapping from the CLOB API.
/// The CLOB `markets/{condition_id}` endpoint returns `tokens[].{token_id, outcome}`
/// which pairs each token with its outcome label directly — no index alignment guessing.
pub async fn verify_tokens_from_clob(
    clob_url: &str,
    condition_id: &str,
    slug: &str,
) -> Option<(String, String)> {
    let url = format!("{}/markets/{}", clob_url, condition_id);
    let resp = match http_client().get(&url).send().await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, slug = %slug, "CLOB market verification failed (HTTP)");
            return None;
        }
    };

    let market: ClobMarketResponse = match resp.json().await {
        Ok(m) => m,
        Err(e) => {
            warn!(error = %e, slug = %slug, "CLOB market verification failed (parse)");
            return None;
        }
    };

    let up_token = market.tokens.iter().find(|t| {
        let o = t.outcome.to_lowercase();
        o == "up" || o == "yes"
    });
    let down_token = market.tokens.iter().find(|t| {
        let o = t.outcome.to_lowercase();
        o == "down" || o == "no"
    });

    match (up_token, down_token) {
        (Some(u), Some(d)) => {
            info!(
                slug = %slug,
                clob_up_token = %u.token_id,
                clob_up_outcome = %u.outcome,
                clob_down_token = %d.token_id,
                clob_down_outcome = %d.outcome,
                "CLOB verification: authoritative token mapping"
            );
            Some((u.token_id.clone(), d.token_id.clone()))
        }
        _ => {
            warn!(
                slug = %slug,
                token_count = market.tokens.len(),
                outcomes = ?market.tokens.iter().map(|t| &t.outcome).collect::<Vec<_>>(),
                "CLOB verification: could not find Up/Down outcomes"
            );
            None
        }
    }
}

/// Resolve market metadata from Gamma API using the market slug,
/// then verify token mapping from the CLOB API as the authoritative source.
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

    let accepting = market.accepting_orders.unwrap_or(false);
    let neg_risk = market.neg_risk.unwrap_or(false);
    let tick_size = market
        .order_price_min_tick_size
        .unwrap_or_else(|| Decimal::new(1, 2));

    // Primary: use the CLOB API for authoritative token-outcome mapping.
    // The CLOB response pairs each token_id with its outcome label directly.
    let (up_token_id, down_token_id) = if !condition_id.is_empty() {
        if let Some((clob_up, clob_down)) =
            verify_tokens_from_clob(clob_url, &condition_id, slug).await
        {
            (clob_up, clob_down)
        } else {
            warn!(slug = %slug, "CLOB verification unavailable, falling back to Gamma outcomes");
            let (up_idx, down_idx) = determine_token_indices(market.outcomes.as_ref());
            (token_ids[up_idx].to_string(), token_ids[down_idx].to_string())
        }
    } else {
        warn!(slug = %slug, "No condition_id from Gamma, using Gamma outcomes for token mapping");
        let (up_idx, down_idx) = determine_token_indices(market.outcomes.as_ref());
        (token_ids[up_idx].to_string(), token_ids[down_idx].to_string())
    };

    info!(
        slug = %slug,
        gamma_outcomes = ?market.outcomes,
        up_token = %up_token_id,
        down_token = %down_token_id,
        accepting = accepting,
        "Resolved market (CLOB-verified token mapping)"
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
