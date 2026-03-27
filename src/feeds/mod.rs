pub mod binance;
pub mod chainlink;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tracing::warn;

struct PriceData {
    price: Decimal,
    updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct TickEntry {
    pub price: Decimal,
    pub ts: Instant,
}

const TICK_HISTORY_MAX_AGE_SECS: u64 = 60;
/// Chainlink updates less frequently than Binance. Use a generous stale
/// threshold so we don't fall back to Binance (USDT) prices unnecessarily.
const CHAINLINK_STALE_MS: i64 = 300_000; // 5 minutes
const BINANCE_STALE_MS: i64 = 60_000;    // 1 minute

/// Dual-source price feed: Chainlink (primary) + Binance (fallback).
///
/// Chainlink prices are stored under Chainlink keys ("btc/usd") and Binance
/// prices under Binance keys ("btcusdt"). They are NEVER mixed — this prevents
/// Binance USDT prices from overwriting Chainlink USD prices that Polymarket
/// uses for market resolution. `get_price_with_fallback` tries the Chainlink
/// key first, falling back to the Binance key only when Chainlink is stale.
#[derive(Clone)]
pub struct PriceFeeds {
    prices: Arc<RwLock<HashMap<String, PriceData>>>,
    window_opens: Arc<RwLock<HashMap<String, Decimal>>>,
    /// Maps Chainlink symbol → Binance symbol for fallback lookups.
    fallback_map: Arc<RwLock<HashMap<String, String>>>,
    /// Per-symbol ring buffer of recent ticks (last 60s) for velocity/volatility.
    tick_history: Arc<RwLock<HashMap<String, VecDeque<TickEntry>>>>,
}

impl PriceFeeds {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            window_opens: Arc::new(RwLock::new(HashMap::new())),
            fallback_map: Arc::new(RwLock::new(HashMap::new())),
            tick_history: Arc::new(RwLock::new(HashMap::new())),
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
        let key = symbol.to_lowercase();
        let now = Instant::now();

        let mut map = self.prices.write().await;
        map.insert(
            key.clone(),
            PriceData {
                price,
                updated_at: Utc::now(),
            },
        );
        drop(map);

        let mut hist = self.tick_history.write().await;
        let deque = hist.entry(key).or_insert_with(VecDeque::new);
        deque.push_back(TickEntry { price, ts: now });
        let cutoff = now - std::time::Duration::from_secs(TICK_HISTORY_MAX_AGE_SECS);
        while deque.front().map_or(false, |t| t.ts < cutoff) {
            deque.pop_front();
        }
    }

    /// Find the tick closest to a target `Instant` in a deque, within `max_diff_ms`.
    fn find_closest_tick(deque: &VecDeque<TickEntry>, target: Instant, max_diff_ms: u64) -> Option<Decimal> {
        let mut closest: Option<&TickEntry> = None;
        let mut best_diff = u64::MAX;
        for tick in deque.iter() {
            let diff = if tick.ts >= target {
                (tick.ts - target).as_millis() as u64
            } else {
                (target - tick.ts).as_millis() as u64
            };
            if diff < best_diff {
                best_diff = diff;
                closest = Some(tick);
            }
        }
        if best_diff <= max_diff_ms {
            closest.map(|t| t.price)
        } else {
            None
        }
    }

    /// Get the price closest to `secs_ago` seconds in the past from the tick history.
    /// Tries the primary key first, then falls back to the registered Binance key.
    pub async fn get_price_at_offset(&self, symbol: &str, secs_ago: u64) -> Option<Decimal> {
        let target = Instant::now() - std::time::Duration::from_secs(secs_ago);
        let hist = self.tick_history.read().await;
        let key = symbol.to_lowercase();

        // Try primary (Chainlink) key first
        if let Some(deque) = hist.get(&key) {
            if let Some(price) = Self::find_closest_tick(deque, target, 5000) {
                return Some(price);
            }
        }

        // Fallback: check the Binance key
        drop(hist);
        let fb_map = self.fallback_map.read().await;
        if let Some(binance_sym) = fb_map.get(&key) {
            let hist = self.tick_history.read().await;
            if let Some(deque) = hist.get(binance_sym) {
                return Self::find_closest_tick(deque, target, 5000);
            }
        }

        None
    }

    /// Get recent ticks for a symbol within the last `lookback_secs` seconds.
    pub async fn get_ticks(&self, symbol: &str, lookback_secs: u64) -> Vec<TickEntry> {
        let hist = self.tick_history.read().await;
        let key = symbol.to_lowercase();
        match hist.get(&key) {
            Some(deque) => {
                let cutoff = Instant::now() - std::time::Duration::from_secs(lookback_secs);
                deque.iter().filter(|t| t.ts >= cutoff).cloned().collect()
            }
            None => Vec::new(),
        }
    }

    /// Get the price symbol key for a market based on its resolution source.
    pub fn price_symbol(resolution_source: &str, chainlink_sym: &str, binance_sym: &str) -> String {
        if resolution_source == "binance" {
            binance_sym.to_lowercase()
        } else {
            chainlink_sym.to_lowercase()
        }
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

        // Try primary (Chainlink) — 5-minute stale window
        if let Some(d) = map.get(&sym_lower) {
            let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
            if age_ms < CHAINLINK_STALE_MS {
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
                if age_ms < BINANCE_STALE_MS {
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

    /// Get the correct price for a market based on its resolution source.
    /// Uses Chainlink (with Binance fallback) for chainlink-resolved markets,
    /// or Binance directly for binance-resolved markets.
    pub async fn get_market_price(
        &self,
        resolution_source: &str,
        chainlink_sym: &str,
        binance_sym: &str,
    ) -> Option<Decimal> {
        if resolution_source == "binance" {
            self.get_price(binance_sym).await
        } else {
            self.get_price_with_fallback(chainlink_sym).await
        }
    }

    /// Check freshness for a market based on its resolution source.
    pub async fn has_fresh_market_price(
        &self,
        resolution_source: &str,
        chainlink_sym: &str,
        binance_sym: &str,
    ) -> bool {
        if resolution_source == "binance" {
            let map = self.prices.read().await;
            map.get(&binance_sym.to_lowercase())
                .map(|d| (Utc::now() - d.updated_at).num_milliseconds() < BINANCE_STALE_MS)
                .unwrap_or(false)
        } else {
            self.has_fresh_price(chainlink_sym).await
        }
    }

    /// Check if we have a reasonably recent price (< 60s old).
    /// Checks Chainlink first, then Binance fallback.
    pub async fn has_fresh_price(&self, symbol: &str) -> bool {
        let sym_lower = symbol.to_lowercase();
        let map = self.prices.read().await;
        if let Some(d) = map.get(&sym_lower) {
            let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
            if age_ms < CHAINLINK_STALE_MS {
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
                if age_ms < BINANCE_STALE_MS {
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
            .map(|d| (Utc::now() - d.updated_at).num_milliseconds() < CHAINLINK_STALE_MS)
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
                .map(|d| (Utc::now() - d.updated_at).num_milliseconds() < BINANCE_STALE_MS)
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
