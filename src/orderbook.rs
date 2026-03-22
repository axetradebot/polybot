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

    debug!(url = %url, token_id = %token_id, "Fetching orderbook");

    let resp: BookResponse = http_client()
        .get(&url)
        .send()
        .await
        .context("orderbook HTTP request failed")?
        .json()
        .await
        .context("failed to parse orderbook JSON")?;

    debug!(
        token_id = %token_id,
        ask_count = resp.asks.len(),
        bid_count = resp.bids.len(),
        "Orderbook response received"
    );

    if resp.asks.is_empty() {
        warn!(
            token_id = %token_id,
            bid_count = resp.bids.len(),
            url = %url,
            "Orderbook has NO ASKS — token may be stale/settled or wrong UP/DOWN mapping"
        );
        anyhow::bail!("orderbook has no asks (token_id={}, bids={})", token_id, resp.bids.len());
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

/// Like `fetch_orderbook` but returns Ok even when there are no asks.
/// Used for the comparison/other token where empty asks is informational, not an error.
pub async fn fetch_orderbook_lenient(clob_url: &str, token_id: &str) -> Result<OrderbookState> {
    let url = format!("{}/book?token_id={}", clob_url, token_id);

    let resp: BookResponse = http_client()
        .get(&url)
        .send()
        .await
        .context("orderbook HTTP request failed")?
        .json()
        .await
        .context("failed to parse orderbook JSON")?;

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

    let spread = best_ask - best_bid;
    let mid_price = (best_ask + best_bid) / Decimal::from(2);

    Ok(OrderbookState {
        best_bid,
        best_ask,
        spread,
        depth_at_ask: Decimal::ZERO,
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

/// Diagnostic: fetch orderbooks for BOTH tokens and report which has asks.
/// Use this when the primary token returns empty asks to diagnose token ID swaps.
pub async fn diagnose_both_tokens(
    clob_url: &str,
    up_token: &str,
    down_token: &str,
    selected_direction: &str,
    slug: &str,
) {
    let up_url = format!("{}/book?token_id={}", clob_url, up_token);
    let down_url = format!("{}/book?token_id={}", clob_url, down_token);

    let (up_result, down_result) = tokio::join!(
        http_client().get(&up_url).send(),
        http_client().get(&down_url).send(),
    );

    let up_asks = match up_result {
        Ok(resp) => match resp.json::<BookResponse>().await {
            Ok(book) => {
                let top_ask = book.asks.first().map(|a| a.price.as_str()).unwrap_or("none");
                let top_bid = book.bids.first().map(|b| b.price.as_str()).unwrap_or("none");
                warn!(
                    slug = %slug,
                    token = "UP",
                    token_id = %up_token,
                    asks = book.asks.len(),
                    bids = book.bids.len(),
                    top_ask,
                    top_bid,
                    "DIAGNOSTIC: UP token orderbook"
                );
                book.asks.len()
            }
            Err(e) => {
                warn!(error = %e, token = "UP", "DIAGNOSTIC: failed to parse UP orderbook");
                0
            }
        },
        Err(e) => {
            warn!(error = %e, token = "UP", "DIAGNOSTIC: failed to fetch UP orderbook");
            0
        }
    };

    let down_asks = match down_result {
        Ok(resp) => match resp.json::<BookResponse>().await {
            Ok(book) => {
                let top_ask = book.asks.first().map(|a| a.price.as_str()).unwrap_or("none");
                let top_bid = book.bids.first().map(|b| b.price.as_str()).unwrap_or("none");
                warn!(
                    slug = %slug,
                    token = "DOWN",
                    token_id = %down_token,
                    asks = book.asks.len(),
                    bids = book.bids.len(),
                    top_ask,
                    top_bid,
                    "DIAGNOSTIC: DOWN token orderbook"
                );
                book.asks.len()
            }
            Err(e) => {
                warn!(error = %e, token = "DOWN", "DIAGNOSTIC: failed to parse DOWN orderbook");
                0
            }
        },
        Err(e) => {
            warn!(error = %e, token = "DOWN", "DIAGNOSTIC: failed to fetch DOWN orderbook");
            0
        }
    };

    if up_asks == 0 && down_asks == 0 {
        warn!(
            slug = %slug,
            "DIAGNOSTIC: BOTH tokens have empty asks! Market may be settled or not yet live."
        );
    } else if (selected_direction == "UP" && up_asks == 0 && down_asks > 0)
        || (selected_direction == "DOWN" && down_asks == 0 && up_asks > 0)
    {
        warn!(
            slug = %slug,
            selected_direction,
            up_asks,
            down_asks,
            "DIAGNOSTIC: TOKEN IDS MAY BE SWAPPED! The OTHER token has asks but selected one doesn't."
        );
    }
}
