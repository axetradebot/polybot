pub mod binance;
pub mod bybit;
pub mod chainlink;
pub mod chainlink_streams;

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

#[derive(Clone, Debug)]
pub struct VolumeEntry {
    pub quantity: f64,
    pub ts: Instant,
}

const TICK_HISTORY_MAX_AGE_SECS: u64 = 180;
/// With Chainlink Data Streams providing sub-second updates, we tighten
/// the stale threshold. Fall back to Binance if no update for 30 seconds.
const CHAINLINK_STALE_MS: i64 = 30_000; // 30 seconds
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
    /// Binance (USDT) open prices — used for unbiased delta calculation.
    binance_window_opens: Arc<RwLock<HashMap<String, Decimal>>>,
    /// Bybit (USD) open prices — used for accurate direction determination.
    bybit_window_opens: Arc<RwLock<HashMap<String, Decimal>>>,
    /// Maps Chainlink symbol → Binance symbol for fallback lookups.
    fallback_map: Arc<RwLock<HashMap<String, String>>>,
    /// Per-symbol ring buffer of recent ticks (last 60s) for velocity/volatility.
    tick_history: Arc<RwLock<HashMap<String, VecDeque<TickEntry>>>>,
    /// Per-symbol rolling trade volume (last 120s) for volume spike detection.
    volume_history: Arc<RwLock<HashMap<String, VecDeque<VolumeEntry>>>>,
}

impl PriceFeeds {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            window_opens: Arc::new(RwLock::new(HashMap::new())),
            binance_window_opens: Arc::new(RwLock::new(HashMap::new())),
            bybit_window_opens: Arc::new(RwLock::new(HashMap::new())),
            fallback_map: Arc::new(RwLock::new(HashMap::new())),
            tick_history: Arc::new(RwLock::new(HashMap::new())),
            volume_history: Arc::new(RwLock::new(HashMap::new())),
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

    /// Backfill historical ticks from RTDS subscribe response.
    /// `ticks` is a list of (unix_timestamp_ms, price) pairs.
    /// Inserts them into the tick history with correct relative timestamps.
    pub async fn backfill_ticks(&self, symbol: &str, ticks: &[(u64, Decimal)]) {
        if ticks.is_empty() {
            return;
        }
        let key = symbol.to_lowercase();
        let now_epoch_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let now_instant = Instant::now();

        let mut hist = self.tick_history.write().await;
        let deque = hist.entry(key.clone()).or_insert_with(VecDeque::new);
        let cutoff = now_instant - std::time::Duration::from_secs(TICK_HISTORY_MAX_AGE_SECS);

        for &(ts_ms, price) in ticks {
            if ts_ms >= now_epoch_ms {
                continue;
            }
            let age_ms = now_epoch_ms - ts_ms;
            if let Some(tick_instant) = now_instant.checked_sub(std::time::Duration::from_millis(age_ms)) {
                if tick_instant >= cutoff {
                    deque.push_back(TickEntry { price, ts: tick_instant });
                }
            }
        }

        deque.make_contiguous().sort_by_key(|t| t.ts);

        // Also update the current price to the latest tick
        if let Some(last) = ticks.last() {
            let mut map = self.prices.write().await;
            map.insert(
                key,
                PriceData {
                    price: last.1,
                    updated_at: Utc::now(),
                },
            );
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
    /// Used for open price capture (Chainlink preferred).
    pub fn price_symbol(resolution_source: &str, chainlink_sym: &str, binance_sym: &str) -> String {
        if resolution_source == "binance" {
            binance_sym.to_lowercase()
        } else {
            chainlink_sym.to_lowercase()
        }
    }

    /// Get the symbol key for live/real-time price data (always Binance).
    /// Used for delta calculation and signal computation during scanning.
    pub fn live_price_symbol(_resolution_source: &str, _chainlink_sym: &str, binance_sym: &str) -> String {
        binance_sym.to_lowercase()
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

    /// Get the freshest real-time price for scanning/delta calculation.
    /// Prefers Binance (sub-second updates) over Chainlink (60-90s updates)
    /// so that delta % is responsive during the entry window.
    pub async fn get_live_price(
        &self,
        resolution_source: &str,
        chainlink_sym: &str,
        binance_sym: &str,
    ) -> Option<Decimal> {
        if resolution_source == "binance" {
            return self.get_price(binance_sym).await;
        }
        // For chainlink-resolved markets, prefer the Binance feed for live data
        let fb_map = self.fallback_map.read().await;
        let sym_lower = chainlink_sym.to_lowercase();
        if let Some(binance_key) = fb_map.get(&sym_lower) {
            let map = self.prices.read().await;
            if let Some(d) = map.get(binance_key) {
                let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
                if age_ms < BINANCE_STALE_MS {
                    return Some(d.price);
                }
            }
        }
        drop(fb_map);
        // Fall back to Chainlink if Binance unavailable
        self.get_price_with_fallback(chainlink_sym).await
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

    pub async fn set_binance_window_open(&self, slug_prefix: &str, window_ts: u64, price: Decimal) {
        let key = Self::window_key(slug_prefix, window_ts);
        self.binance_window_opens.write().await.insert(key, price);
    }

    pub async fn get_binance_window_open(&self, slug_prefix: &str, window_ts: u64) -> Option<Decimal> {
        let key = Self::window_key(slug_prefix, window_ts);
        self.binance_window_opens.read().await.get(&key).copied()
    }

    pub async fn set_bybit_window_open(&self, slug_prefix: &str, window_ts: u64, price: Decimal) {
        let key = Self::window_key(slug_prefix, window_ts);
        self.bybit_window_opens.write().await.insert(key, price);
    }

    pub async fn get_bybit_window_open(&self, slug_prefix: &str, window_ts: u64) -> Option<Decimal> {
        let key = Self::window_key(slug_prefix, window_ts);
        self.bybit_window_opens.read().await.get(&key).copied()
    }

    /// Record a trade's volume for a symbol.
    pub async fn record_volume(&self, symbol: &str, quantity: f64) {
        let key = symbol.to_lowercase();
        let now = Instant::now();
        let mut hist = self.volume_history.write().await;
        let deque = hist.entry(key).or_insert_with(VecDeque::new);
        deque.push_back(VolumeEntry { quantity, ts: now });
        let cutoff = now - std::time::Duration::from_secs(120);
        while deque.front().map_or(false, |v| v.ts < cutoff) {
            deque.pop_front();
        }
    }

    /// Compute volume ratio: volume_last_10s / (volume_last_60s / 6).
    /// Returns None if there's insufficient data (< 30s of history).
    pub async fn get_volume_ratio(&self, symbol: &str) -> Option<f64> {
        let key = symbol.to_lowercase();
        let hist = self.volume_history.read().await;
        let deque = hist.get(&key)?;
        let now = Instant::now();
        let cutoff_10 = now - std::time::Duration::from_secs(10);
        let cutoff_60 = now - std::time::Duration::from_secs(60);

        let vol_10: f64 = deque.iter().filter(|v| v.ts >= cutoff_10).map(|v| v.quantity).sum();
        let vol_60: f64 = deque.iter().filter(|v| v.ts >= cutoff_60).map(|v| v.quantity).sum();

        let avg_10s_in_60s = vol_60 / 6.0;
        if avg_10s_in_60s < 1e-12 {
            return None;
        }
        Some(vol_10 / avg_10s_in_60s)
    }

    pub async fn prune_old_window_opens(&self, current_ts: u64, max_age_s: u64) {
        let retain_fn = |key: &String, _: &mut Decimal| {
            key.rsplit_once(':')
                .and_then(|(_, ts_str)| ts_str.parse::<u64>().ok())
                .map(|ts| current_ts.saturating_sub(ts) <= max_age_s)
                .unwrap_or(false)
        };
        self.window_opens.write().await.retain(retain_fn);
        self.binance_window_opens.write().await.retain(retain_fn);
        self.bybit_window_opens.write().await.retain(retain_fn);
    }

    /// Spawn Bybit inverse perpetual feed (USD prices for direction).
    pub fn spawn_bybit_feed(&self, symbols: Vec<String>, ws_url: &str) {
        let feeds = self.clone();
        let ws_url = ws_url.to_string();
        tokio::spawn(async move {
            if let Err(e) = bybit::run_bybit_feed(feeds, &symbols, &ws_url).await {
                tracing::error!(error = %e, "Bybit feed fatal error");
            }
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

    /// Spawn Chainlink Data Streams (sub-second, resolution-aligned) price feed.
    pub fn spawn_chainlink_streams_feed(&self, assets: Vec<String>) {
        let feeds = self.clone();
        tokio::spawn(async move {
            if let Err(e) = chainlink_streams::run_chainlink_streams_feed(feeds, assets).await {
                tracing::error!(error = %e, "Chainlink Data Streams feed fatal error");
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
