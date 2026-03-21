pub mod binance;

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

    pub async fn get_price(&self, symbol: &str) -> Option<Decimal> {
        let map = self.prices.read().await;
        map.get(&symbol.to_lowercase()).map(|d| d.price)
    }

    /// Check if we have a reasonably recent price (< 60s old).
    /// If the price exists but is very old, log a warning.
    pub async fn has_fresh_price(&self, symbol: &str) -> bool {
        let map = self.prices.read().await;
        match map.get(&symbol.to_lowercase()) {
            Some(d) => {
                let age_ms = (Utc::now() - d.updated_at).num_milliseconds();
                if age_ms < 60_000 {
                    true
                } else {
                    warn!(
                        symbol = symbol,
                        age_secs = age_ms / 1000,
                        price = %d.price,
                        "Price data is stale — Binance feed may be down"
                    );
                    false
                }
            }
            None => false,
        }
    }

    /// Get the age of the last price update for a symbol (in milliseconds).
    pub async fn price_age_ms(&self, symbol: &str) -> Option<i64> {
        let map = self.prices.read().await;
        map.get(&symbol.to_lowercase())
            .map(|d| (Utc::now() - d.updated_at).num_milliseconds())
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
