use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{debug, warn};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OrderbookState {
    pub best_bid: Decimal,
    pub best_ask: Decimal,
    pub spread: Decimal,
    pub depth_at_ask: Decimal,
    pub mid_price: Decimal,
}

#[derive(Deserialize)]
struct BookResponse {
    #[serde(default)]
    bids: Vec<BookLevel>,
    #[serde(default)]
    asks: Vec<BookLevel>,
}

#[derive(Deserialize)]
struct BookLevel {
    price: String,
    size: String,
}

static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

fn http_client() -> &'static reqwest::Client {
    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .expect("failed to build HTTP client")
    })
}

/// Fetch the current orderbook from the Polymarket CLOB REST API.
pub async fn fetch_orderbook(clob_url: &str, token_id: &str) -> Result<OrderbookState> {
    let url = format!("{}/book?token_id={}", clob_url, token_id);

    let resp: BookResponse = http_client()
        .get(&url)
        .send()
        .await
        .context("orderbook HTTP request failed")?
        .json()
        .await
        .context("failed to parse orderbook JSON")?;

    if resp.asks.is_empty() {
        anyhow::bail!("orderbook has no asks");
    }

    let best_ask = resp
        .asks
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .min()
        .unwrap_or(Decimal::ONE);

    let best_bid = resp
        .bids
        .iter()
        .filter_map(|l| l.price.parse::<Decimal>().ok())
        .max()
        .unwrap_or(Decimal::ZERO);

    let depth_threshold = best_ask + Decimal::new(3, 2); // $0.03 above best ask
    let depth_at_ask: Decimal = resp
        .asks
        .iter()
        .filter_map(|l| {
            let p = l.price.parse::<Decimal>().ok()?;
            let s = l.size.parse::<Decimal>().ok()?;
            if p <= depth_threshold { Some(s * p) } else { None }
        })
        .sum();

    let spread = best_ask - best_bid;
    let mid_price = (best_ask + best_bid) / Decimal::from(2);

    debug!(
        best_bid = %best_bid,
        best_ask = %best_ask,
        spread = %spread,
        depth_usd = %depth_at_ask,
        "Orderbook fetched"
    );

    Ok(OrderbookState {
        best_bid,
        best_ask,
        spread,
        depth_at_ask,
        mid_price,
    })
}

/// Re-fetch just the best ask for tightening. Returns None on error
/// instead of failing the whole loop.
pub async fn refresh_best_ask(clob_url: &str, token_id: &str) -> Option<Decimal> {
    match fetch_orderbook(clob_url, token_id).await {
        Ok(ob) => Some(ob.best_ask),
        Err(e) => {
            warn!(error = %e, "Failed to refresh orderbook");
            None
        }
    }
}
