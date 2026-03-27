use anyhow::Result;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::PriceFeeds;

static CL_MSG_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn messages_received() -> u64 {
    CL_MSG_COUNT.load(Ordering::Relaxed)
}

#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct ChainlinkMessage {
    #[serde(default)]
    topic: String,
    #[serde(default, rename = "type")]
    msg_type: String,
    payload: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct LivePricePayload {
    symbol: String,
    value: f64,
}

#[derive(Deserialize, Debug)]
struct HistoryDataPoint {
    #[allow(dead_code)]
    timestamp: u64,
    value: f64,
}

#[derive(Deserialize, Debug)]
struct HistoryPayload {
    symbol: String,
    data: Vec<HistoryDataPoint>,
}

/// Connect to the Polymarket Chainlink price WebSocket and stream prices
/// into the primary price store. Reconnects automatically with exponential backoff.
pub async fn run_chainlink_feed(
    feeds: PriceFeeds,
    symbols: &[String],
    ws_url: &str,
) -> Result<()> {
    let subscribe_msg = build_subscribe_message(symbols);
    let mut reconnect_count = 0u32;

    loop {
        info!(
            url = %ws_url,
            symbols = ?symbols,
            reconnects = reconnect_count,
            "Connecting to Polymarket Chainlink WebSocket..."
        );

        match connect_async(ws_url).await {
            Ok((ws_stream, _response)) => {
                info!(symbols = ?symbols, "Chainlink WebSocket connected");
                reconnect_count = 0;
                let (mut write, mut read) = ws_stream.split();

                if let Err(e) = write.send(Message::Text(subscribe_msg.clone().into())).await {
                    error!(error = %e, "Failed to send Chainlink subscribe message");
                    continue;
                }
                info!("Chainlink subscribe message sent");

                // Spawn PING keepalive task
                let ping_write = std::sync::Arc::new(tokio::sync::Mutex::new(write));
                let ping_writer = ping_write.clone();
                let ping_task = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        let mut w = ping_writer.lock().await;
                        if w.send(Message::Text("PING".into())).await.is_err() {
                            break;
                        }
                    }
                });

                // Track last message time for stale detection
                let mut last_msg_at = tokio::time::Instant::now();
                let stale_timeout = Duration::from_secs(30);

                loop {
                    let read_result = tokio::time::timeout(
                        Duration::from_secs(10),
                        read.next(),
                    )
                    .await;

                    match read_result {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            last_msg_at = tokio::time::Instant::now();

                            let text_str: &str = &text;
                            if text_str.is_empty()
                                || text_str == "PONG"
                                || text_str.contains("\"type\":\"connection\"")
                            {
                                continue;
                            }

                            CL_MSG_COUNT.fetch_add(1, Ordering::Relaxed);

                            if let Err(e) = handle_chainlink_message(&feeds, text_str).await {
                                warn!(error = %e, text = %&text_str[..text_str.len().min(200)], "Failed to parse Chainlink message");
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            let mut w = ping_write.lock().await;
                            let _ = w.send(Message::Pong(data)).await;
                            last_msg_at = tokio::time::Instant::now();
                        }
                        Ok(Some(Ok(Message::Close(_)))) => {
                            warn!("Chainlink WebSocket closed by server");
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            error!(error = %e, "Chainlink WebSocket error");
                            break;
                        }
                        Ok(None) => {
                            warn!("Chainlink WebSocket stream ended");
                            break;
                        }
                        Err(_) => {
                            // Timeout — check if we've been stale too long
                            if last_msg_at.elapsed() > stale_timeout {
                                warn!(
                                    stale_secs = last_msg_at.elapsed().as_secs(),
                                    "Chainlink feed stale for >30s, forcing reconnect"
                                );
                                break;
                            }
                            if last_msg_at.elapsed() > Duration::from_secs(10) {
                                warn!(
                                    stale_secs = last_msg_at.elapsed().as_secs(),
                                    "No Chainlink price update for >10s"
                                );
                            }
                        }
                        _ => {}
                    }
                }

                ping_task.abort();
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Chainlink WebSocket");
            }
        }

        reconnect_count += 1;
        let backoff = std::cmp::min(
            1u64.checked_shl(reconnect_count).unwrap_or(30).min(30),
            30,
        );
        warn!(
            reconnect_in_secs = backoff,
            attempt = reconnect_count,
            "Chainlink WebSocket disconnected, reconnecting..."
        );
        tokio::time::sleep(Duration::from_secs(backoff)).await;
    }
}

fn build_subscribe_message(symbols: &[String]) -> String {
    let mut subscriptions: Vec<serde_json::Value> = Vec::new();

    // Subscribe to crypto_prices_chainlink for each symbol (primary source)
    for sym in symbols {
        subscriptions.push(serde_json::json!({
            "topic": "crypto_prices_chainlink",
            "type": "*",
            "filters": format!("{{\"symbol\":\"{}\"}}", sym)
        }));
    }

    // Also subscribe to crypto_prices for real-time updates on all symbols.
    // crypto_prices_chainlink only streams live data for BTC; crypto_prices
    // provides per-second updates for all symbols via Polymarket's RTDS.
    subscriptions.push(serde_json::json!({
        "topic": "crypto_prices",
        "type": "update",
        "filters": ""
    }));

    let msg = serde_json::json!({
        "action": "subscribe",
        "subscriptions": subscriptions
    });

    msg.to_string()
}

/// Map Binance-style symbol (ethusdt) to Chainlink-style (eth/usd) for storage.
fn binance_to_chainlink_symbol(sym: &str) -> Option<&'static str> {
    match sym {
        "btcusdt" => Some("btc/usd"),
        "ethusdt" => Some("eth/usd"),
        "solusdt" => Some("sol/usd"),
        "xrpusdt" => Some("xrp/usd"),
        "dogeusdt" => Some("doge/usd"),
        "bnbusdt" => Some("bnb/usd"),
        "hypeusdt" => Some("hype/usd"),
        _ => None,
    }
}

async fn handle_chainlink_message(feeds: &PriceFeeds, text: &str) -> Result<()> {
    let msg: ChainlinkMessage = serde_json::from_str(text)?;

    match msg.topic.as_str() {
        "crypto_prices_chainlink" => {
            // Authoritative Chainlink oracle price — always overwrite
            if let Some(payload_val) = msg.payload {
                let payload: LivePricePayload = serde_json::from_value(payload_val)?;
                let price = Decimal::try_from(payload.value)?;
                let symbol = payload.symbol.to_lowercase();
                feeds.set_price(&symbol, price).await;
            }
        }
        "crypto_prices" => {
            if let Some(payload_val) = msg.payload {
                if msg.msg_type == "subscribe" {
                    // Initial subscribe response with historical data — take the last value.
                    // Store under the Binance key so it doesn't overwrite Chainlink prices.
                    // Only seed the Chainlink key if no Chainlink price exists yet.
                    let payload: HistoryPayload = serde_json::from_value(payload_val)?;
                    if let Some(last) = payload.data.last() {
                        let price = Decimal::try_from(last.value)?;
                        let binance_sym = payload.symbol.to_lowercase();
                        feeds.set_price(&binance_sym, price).await;
                        if let Some(cl_key) = binance_to_chainlink_symbol(&binance_sym) {
                            if feeds.get_price(cl_key).await.is_none() {
                                info!(symbol = %cl_key, price = %price, "Seeding Chainlink key from Binance (no Chainlink data yet)");
                                feeds.set_price(cl_key, price).await;
                            }
                        }
                    }
                } else {
                    // Real-time Binance update — store under the BINANCE key only.
                    // This prevents Binance (USDT) prices from overwriting
                    // Chainlink (USD) prices that Polymarket uses for resolution.
                    let payload: LivePricePayload = serde_json::from_value(payload_val)?;
                    let price = Decimal::try_from(payload.value)?;
                    let binance_sym = payload.symbol.to_lowercase();
                    feeds.set_price(&binance_sym, price).await;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_message() {
        let symbols = vec!["btc/usd".to_string(), "eth/usd".to_string()];
        let msg = build_subscribe_message(&symbols);
        assert!(msg.contains("btc/usd"));
        assert!(msg.contains("eth/usd"));
        assert!(msg.contains("crypto_prices_chainlink"));
    }
}
