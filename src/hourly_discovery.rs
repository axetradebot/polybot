use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::market::epoch_secs;
use crate::types::MarketInfo;

#[derive(Debug, Clone)]
pub struct HourlyMarket {
    pub event_slug: String,
    pub condition_id: String,
    pub up_token_id: String,
    pub down_token_id: String,
    pub end_time: u64,
    pub accepting_orders: bool,
    pub neg_risk: bool,
    pub tick_size: Decimal,
}

impl HourlyMarket {
    pub fn to_market_info(&self) -> MarketInfo {
        MarketInfo {
            condition_id: self.condition_id.clone(),
            up_token_id: self.up_token_id.clone(),
            down_token_id: self.down_token_id.clone(),
            slug: self.event_slug.clone(),
            accepting_orders: self.accepting_orders,
            neg_risk: self.neg_risk,
            tick_size: self.tick_size,
        }
    }
}

struct CacheEntry {
    markets: Vec<HourlyMarket>,
    fetched_at: Instant,
}

#[derive(Clone)]
pub struct HourlyDiscovery {
    cache: Arc<RwLock<Option<CacheEntry>>>,
    client: reqwest::Client,
    cache_duration: std::time::Duration,
}

impl HourlyDiscovery {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(None)),
            client: reqwest::Client::new(),
            cache_duration: std::time::Duration::from_secs(600),
        }
    }

    pub async fn discover(&self, slug_prefix: &str) -> Result<Vec<HourlyMarket>> {
        {
            let cache = self.cache.read().await;
            if let Some(ref entry) = *cache {
                if entry.fetched_at.elapsed() < self.cache_duration {
                    return Ok(entry.markets.clone());
                }
            }
        }

        let markets = self.fetch_from_gamma(slug_prefix).await?;

        {
            let mut cache = self.cache.write().await;
            *cache = Some(CacheEntry {
                markets: markets.clone(),
                fetched_at: Instant::now(),
            });
        }

        Ok(markets)
    }

    /// Get the hourly market whose window is currently active (closest end_time in the future).
    pub async fn get_current_market(&self, slug_prefix: &str) -> Result<Option<HourlyMarket>> {
        let markets = self.discover(slug_prefix).await?;
        let now = epoch_secs();
        Ok(markets.into_iter().find(|m| m.end_time > now))
    }

    /// Force-clear the cache so the next call re-fetches from the API.
    pub async fn invalidate_cache(&self) {
        let mut cache = self.cache.write().await;
        *cache = None;
    }

    async fn fetch_from_gamma(&self, slug_prefix: &str) -> Result<Vec<HourlyMarket>> {
        let url = format!(
            "https://gamma-api.polymarket.com/events?slug_contains={}&active=true&closed=false&limit=10",
            slug_prefix
        );

        info!(url = %url, "Querying Gamma API for hourly markets");

        let resp = self
            .client
            .get(&url)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .context("Failed to query Gamma API for hourly markets")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Gamma API returned {status}: {body}");
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .context("Failed to parse Gamma API response")?;

        let events = match body.as_array() {
            Some(arr) => arr,
            None => {
                warn!("Gamma API returned non-array response for hourly discovery");
                return Ok(vec![]);
            }
        };

        let mut result = Vec::new();

        for event in events {
            let event_slug = event["slug"].as_str().unwrap_or_default().to_string();
            if event_slug.is_empty() {
                continue;
            }

            let end_time = parse_end_time(event);
            if end_time == 0 {
                debug!(slug = %event_slug, "Skipping event with no parseable end_time");
                continue;
            }

            let markets = match event["markets"].as_array() {
                Some(arr) => arr,
                None => continue,
            };

            let mkt = match markets.first() {
                Some(m) => m,
                None => continue,
            };

            let condition_id = mkt["condition_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();

            let token_ids = parse_clob_token_ids(mkt);
            if token_ids.len() < 2 {
                debug!(slug = %event_slug, "Skipping event: fewer than 2 token IDs");
                continue;
            }

            let accepting = mkt["accepting_orders"].as_bool().unwrap_or(false);
            let neg_risk = mkt["neg_risk"].as_bool().unwrap_or(false);
            let tick_size = mkt["order_price_min_tick_size"]
                .as_str()
                .and_then(|s| s.parse::<Decimal>().ok())
                .or_else(|| {
                    mkt["order_price_min_tick_size"]
                        .as_f64()
                        .and_then(|f| Decimal::try_from(f).ok())
                })
                .unwrap_or(Decimal::new(1, 2));

            // Determine correct UP/DOWN mapping using outcome labels from the tokens array
            let (up_token_id, down_token_id) = resolve_up_down_tokens(mkt, &token_ids, &event_slug);

            result.push(HourlyMarket {
                event_slug,
                condition_id,
                up_token_id,
                down_token_id,
                end_time,
                accepting_orders: accepting,
                neg_risk,
                tick_size,
            });
        }

        result.sort_by_key(|m| m.end_time);

        info!(count = result.len(), "Discovered hourly markets from Gamma");
        for m in &result {
            debug!(
                slug = %m.event_slug,
                end_time = m.end_time,
                accepting = m.accepting_orders,
                "Hourly market"
            );
        }

        Ok(result)
    }
}

fn parse_end_time(event: &serde_json::Value) -> u64 {
    let date_str = event["end_date_iso"]
        .as_str()
        .or_else(|| event["endDate"].as_str())
        .or_else(|| event["end_date"].as_str());

    if let Some(s) = date_str {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return dt.timestamp() as u64;
        }
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.fZ") {
            return dt.and_utc().timestamp() as u64;
        }
    }

    0
}

/// Parse clob_token_ids which may be a JSON array or a JSON-encoded string containing an array.
fn parse_clob_token_ids(market: &serde_json::Value) -> Vec<String> {
    let field = &market["clob_token_ids"];

    match field {
        serde_json::Value::Array(arr) => {
            return arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        serde_json::Value::String(s) => {
            if let Ok(arr) = serde_json::from_str::<Vec<String>>(s) {
                return arr;
            }
        }
        _ => {}
    }

    // Fallback: try the tokens array
    if let Some(tokens) = market["tokens"].as_array() {
        return tokens
            .iter()
            .filter_map(|t| t["token_id"].as_str().map(|s| s.to_string()))
            .collect();
    }

    vec![]
}

/// Resolve which token ID is UP and which is DOWN by checking the `outcomes`
/// field or the `tokens[].outcome` labels from the Gamma API.
/// This prevents the bug where blindly assuming [0]=Up, [1]=Down gets it wrong.
fn resolve_up_down_tokens(
    market: &serde_json::Value,
    fallback_ids: &[String],
    slug: &str,
) -> (String, String) {
    // Strategy 1: Check the `tokens` array which has per-token `outcome` labels
    if let Some(tokens) = market["tokens"].as_array() {
        let up = tokens.iter().find(|t| {
            t["outcome"]
                .as_str()
                .map(|o| o.eq_ignore_ascii_case("Up") || o.eq_ignore_ascii_case("Yes"))
                .unwrap_or(false)
        });
        let down = tokens.iter().find(|t| {
            t["outcome"]
                .as_str()
                .map(|o| o.eq_ignore_ascii_case("Down") || o.eq_ignore_ascii_case("No"))
                .unwrap_or(false)
        });

        if let (Some(up_tok), Some(down_tok)) = (up, down) {
            let up_id = up_tok["token_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let down_id = down_tok["token_id"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if !up_id.is_empty() && !down_id.is_empty() {
                info!(
                    slug = %slug,
                    up_token = %up_id,
                    down_token = %down_id,
                    up_outcome = up_tok["outcome"].as_str().unwrap_or("?"),
                    down_outcome = down_tok["outcome"].as_str().unwrap_or("?"),
                    "Hourly token IDs resolved from outcome labels"
                );
                return (up_id, down_id);
            }
        }
    }

    // Strategy 2: Check the `outcomes` field (JSON-encoded string array like "[\"Up\",\"Down\"]")
    let outcomes: Option<Vec<String>> = market["outcomes"]
        .as_str()
        .and_then(|s| serde_json::from_str(s).ok())
        .or_else(|| {
            market["outcomes"].as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
        });

    if let Some(ref oc) = outcomes {
        let up_pos = oc.iter().position(|o| {
            o.eq_ignore_ascii_case("Up") || o.eq_ignore_ascii_case("Yes")
        });
        let down_pos = oc.iter().position(|o| {
            o.eq_ignore_ascii_case("Down") || o.eq_ignore_ascii_case("No")
        });
        if let (Some(u), Some(d)) = (up_pos, down_pos) {
            if u < fallback_ids.len() && d < fallback_ids.len() {
                info!(
                    slug = %slug,
                    outcomes = ?oc,
                    up_idx = u,
                    down_idx = d,
                    up_token = %fallback_ids[u],
                    down_token = %fallback_ids[d],
                    "Hourly token IDs resolved from outcomes field"
                );
                return (fallback_ids[u].clone(), fallback_ids[d].clone());
            }
        }
    }

    // Fallback: assume [0]=Up, [1]=Down (may be wrong!)
    warn!(
        slug = %slug,
        token_0 = %fallback_ids[0],
        token_1 = %fallback_ids[1],
        "Could not determine Up/Down from outcomes — ASSUMING [0]=Up [1]=Down (may be wrong!)"
    );
    (fallback_ids[0].clone(), fallback_ids[1].clone())
}
