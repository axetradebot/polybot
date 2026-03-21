pub mod binance;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe price data for a single symbol.
struct PriceData {
    price: Decimal,
    updated_at: DateTime<Utc>,
}

/// Multi-asset price feed manager.
///
/// Holds current prices by Binance symbol (e.g. "btcusdt") and
/// window-open prices by a compound key like "btc-updown-5m:1774090500".
#[derive(Clone)]
pub struct PriceFeeds {
    prices: Arc<RwLock<HashMap<String, PriceData>>>,
    window_opens: Arc<RwLock<HashMap<String, Decimal>>>,
}

impl PriceFeeds {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
            window_opens: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update the current price for a symbol.
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

    /// Get current price for a symbol.
    pub async fn get_price(&self, symbol: &str) -> Option<Decimal> {
        let map = self.prices.read().await;
        map.get(&symbol.to_lowercase()).map(|d| d.price)
    }

    /// Check if we have a fresh price (< 5s old) for a symbol.
    pub async fn has_fresh_price(&self, symbol: &str) -> bool {
        let map = self.prices.read().await;
        map.get(&symbol.to_lowercase())
            .map(|d| (Utc::now() - d.updated_at).num_milliseconds() < 5000)
            .unwrap_or(false)
    }

    fn window_key(slug_prefix: &str, window_ts: u64) -> String {
        format!("{}:{}", slug_prefix, window_ts)
    }

    /// Store the window-open price for a market window.
    pub async fn set_window_open(&self, slug_prefix: &str, window_ts: u64, price: Decimal) {
        let key = Self::window_key(slug_prefix, window_ts);
        self.window_opens.write().await.insert(key, price);
    }

    /// Get the window-open price for a market window.
    pub async fn get_window_open(&self, slug_prefix: &str, window_ts: u64) -> Option<Decimal> {
        let key = Self::window_key(slug_prefix, window_ts);
        self.window_opens.read().await.get(&key).copied()
    }

    /// Prune window-open prices older than `max_age_s` seconds.
    pub async fn prune_old_window_opens(&self, current_ts: u64, max_age_s: u64) {
        let mut map = self.window_opens.write().await;
        map.retain(|key, _| {
            key.rsplit_once(':')
                .and_then(|(_, ts_str)| ts_str.parse::<u64>().ok())
                .map(|ts| current_ts.saturating_sub(ts) <= max_age_s)
                .unwrap_or(false)
        });
    }

    /// Launch the Binance combined WebSocket feed for all given symbols.
    /// Spawns a background tokio task.
    pub fn spawn_binance_feed(&self, symbols: Vec<String>, ws_base: &str) {
        let feeds = self.clone();
        let ws_base = ws_base.to_string();
        tokio::spawn(async move {
            if let Err(e) = binance::run_combined_feed(feeds, &symbols, &ws_base).await {
                tracing::error!(error = %e, "Binance combined feed fatal error");
            }
        });
    }
}
