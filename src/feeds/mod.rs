pub mod binance;
pub mod chainlink;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::warn;

struct PriceData {
    price: Decimal,
    updated_at: DateTime<Utc>,
}

/// Dual-source price feed: Chainlink (primary) + Binance (fallback).
///
/// Both sources write into the same `prices` map using their own symbol format
/// (Chainlink: "btc/usd", Binance: "btcusdt"). Callers should query with the
/// Chainlink symbol. If the Chainlink price is stale, `get_price_with_fallback`
/// will transparently try the Binance key.
#[derive(Clone)]
pub struct PriceFeeds {
    prices: Arc<RwLock<HashMap<String, PriceData>>>,
    window_opens: Arc<RwLock<HashMap<String, Decimal>>>,
    /// Maps Chainlink symbol → Binance symbol for fallback lookups.
    fallback_map: Arc<RwLock<HashMap<String, String>>>,
}

impl PriceFeeds {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            window_opens: Arc::new(RwLock::new(HashMap::new())),
            fallback_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a Chainlink→Binance symbol mapping for fallback lookups.
    pub async fn register_fallback(&self, chainlink_sym: &str, binance_sym: &str) {
        self.fallback_map
            .write()
            .await
            .insert(chainlink_sym.to_lowercase(), binance_sym.to_lowercase());
    }

    pub async fn set_price(&self, symbol: &str, price: Decimal) {
        let mut map = self.prices.write().await;
        map.insert(
            symbol.to_lowercase(),
            PriceData {
                price,
                updated_at: Utc::now(),
            },
        );
    }

    /// Get price for a symbol. Returns the value if present, regardless of age.
    pub async fn get_price(&self, symbol: &str) -> Option<Decimal> {
        let map = self.prices.read().await;
        map.get(&symbol.to_lowercase()).map(|d| d.price)
    }

    /// Get price with automatic Binance fallback.
    /// Tries the primary (Chainlink) symbol first. If it's missing or stale (>60s),
    /// falls back to the registered Binance symbol.
    pub async fn get_price_with_fallback(&self, chainlink_sym: &str) -> Option<Decimal> {
        let sym_lower = chainlink_sym.to_lowercase();
        let map = self.prices.read().await;

        // Try primary (Chainlink)
        if let Some(d) = map.get(&sym_lower) {
            let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
            if age_ms < 60_000 {
                return Some(d.price);
            }
        }
        drop(map);

        // Primary missing or stale — try Binance fallback
        let fb_map = self.fallback_map.read().await;
        if let Some(binance_sym) = fb_map.get(&sym_lower) {
            let map = self.prices.read().await;
            if let Some(d) = map.get(binance_sym) {
                let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
                if age_ms < 60_000 {
                    warn!(
                        chainlink = %chainlink_sym,
                        binance_fallback = %binance_sym,
                        price = %d.price,
                        "Using BINANCE FALLBACK price — Chainlink feed stale"
                    );
                    return Some(d.price);
                }
            }
        }

        None
    }

    /// Check if we have a reasonably recent price (< 60s old).
    /// Checks Chainlink first, then Binance fallback.
    pub async fn has_fresh_price(&self, symbol: &str) -> bool {
        let sym_lower = symbol.to_lowercase();
        let map = self.prices.read().await;
        if let Some(d) = map.get(&sym_lower) {
            let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
            if age_ms < 60_000 {
                return true;
            }
            warn!(
                symbol = symbol,
                age_secs = age_ms / 1000,
                price = %d.price,
                "Chainlink price stale — checking Binance fallback"
            );
        }
        drop(map);

        // Check fallback
        let fb_map = self.fallback_map.read().await;
        if let Some(binance_sym) = fb_map.get(&sym_lower) {
            let map = self.prices.read().await;
            if let Some(d) = map.get(binance_sym) {
                let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
                if age_ms < 60_000 {
                    warn!(
                        chainlink = symbol,
                        binance_fallback = %binance_sym,
                        "Using Binance fallback for freshness check"
                    );
                    return true;
                }
            }
        }

        false
    }

    /// Get the age of the primary (Chainlink) price update for a symbol (in ms).
    pub async fn price_age_ms(&self, symbol: &str) -> Option<i64> {
        let map = self.prices.read().await;
        map.get(&symbol.to_lowercase())
            .map(|d| (Utc::now() - d.updated_at).num_milliseconds())
    }

    /// Check whether the Binance fallback is currently being used for a symbol
    /// (i.e. Chainlink is stale or missing but Binance is fresh).
    pub async fn is_using_fallback(&self, chainlink_sym: &str) -> bool {
        let sym_lower = chainlink_sym.to_lowercase();
        let map = self.prices.read().await;
        let chainlink_fresh = map
            .get(&sym_lower)
            .map(|d| (Utc::now() - d.updated_at).num_milliseconds() < 60_000)
            .unwrap_or(false);
        if chainlink_fresh {
            return false;
        }
        drop(map);

        let fb_map = self.fallback_map.read().await;
        if let Some(binance_sym) = fb_map.get(&sym_lower) {
            let map = self.prices.read().await;
            return map
                .get(binance_sym)
                .map(|d| (Utc::now() - d.updated_at).num_milliseconds() < 60_000)
                .unwrap_or(false);
        }
        false
    }

    fn window_key(slug_prefix: &str, window_ts: u64) -> String {
        format!("{}:{}", slug_prefix, window_ts)
    }

    pub async fn set_window_open(&self, slug_prefix: &str, window_ts: u64, price: Decimal) {
        let key = Self::window_key(slug_prefix, window_ts);
        self.window_opens.write().await.insert(key, price);
    }

    pub async fn get_window_open(&self, slug_prefix: &str, window_ts: u64) -> Option<Decimal> {
        let key = Self::window_key(slug_prefix, window_ts);
        self.window_opens.read().await.get(&key).copied()
    }

    pub async fn prune_old_window_opens(&self, current_ts: u64, max_age_s: u64) {
        let mut map = self.window_opens.write().await;
        map.retain(|key, _| {
            key.rsplit_once(':')
                .and_then(|(_, ts_str)| ts_str.parse::<u64>().ok())
                .map(|ts| current_ts.saturating_sub(ts) <= max_age_s)
                .unwrap_or(false)
        });
    }

    /// Spawn Chainlink (primary) price feed.
    pub fn spawn_chainlink_feed(&self, symbols: Vec<String>, ws_url: &str) {
        let feeds = self.clone();
        let ws_url = ws_url.to_string();
        tokio::spawn(async move {
            if let Err(e) = chainlink::run_chainlink_feed(feeds, &symbols, &ws_url).await {
                tracing::error!(error = %e, "Chainlink feed fatal error");
            }
        });
    }

    /// Spawn Binance (fallback) price feed.
    pub fn spawn_binance_feed(&self, symbols: Vec<String>, ws_base: &str) {
        let feeds = self.clone();
        let ws_base = ws_base.to_string();
        tokio::spawn(async move {
            if let Err(e) = binance::run_combined_feed(feeds, &symbols, &ws_base).await {
                tracing::error!(error = %e, "Binance fallback feed fatal error");
            }
        });
    }
}
