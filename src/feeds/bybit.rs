use anyhow::Result;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::PriceFeeds;

static BYBIT_MSG_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn messages_received() -> u64 {
    BYBIT_MSG_COUNT.load(Ordering::Relaxed)
}

#[derive(Deserialize, Debug)]
struct TickerMsg {
    #[serde(default)]
    topic: String,
    data: Option<TickerData>,
}

#[derive(Deserialize, Debug)]
#[allow(non_snake_case, dead_code)]
struct TickerData {
    symbol: String,
    lastPrice: Option<String>,
}

pub fn bybit_price_key(bybit_symbol: &str) -> String {
    format!("{}_bybit", bybit_symbol.to_lowercase())
}

pub async fn run_bybit_feed(
    feeds: PriceFeeds,
    symbols: &[String],
    ws_url: &str,
) -> Result<()> {
    let args: Vec<String> = symbols.iter().map(|s| format!("tickers.{}", s)).collect();
    let subscribe_msg = serde_json::json!({
        "op": "subscribe",
        "args": args
    })
    .to_string();

    let mut reconnect_count = 0u32;

    loop {
        info!(
            url = %ws_url,
            symbols = ?symbols,
            reconnects = reconnect_count,
            "Connecting to Bybit inverse WebSocket..."
        );

        match connect_async(ws_url).await {
            Ok((ws_stream, _response)) => {
                info!(symbols = ?symbols, "Bybit WebSocket connected");
                reconnect_count = 0;
                let (mut write, mut read) = ws_stream.split();

                if let Err(e) = write.send(Message::Text(subscribe_msg.clone().into())).await
                {
                    error!(error = %e, "Failed to send Bybit subscribe");
                    continue;
                }
                info!("Bybit subscribe sent for {} symbols", symbols.len());

                let ping_write = std::sync::Arc::new(tokio::sync::Mutex::new(write));
                let ping_writer = ping_write.clone();
                let ping_task = tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(Duration::from_secs(20)).await;
                        let mut w = ping_writer.lock().await;
                        let ping = serde_json::json!({"op": "ping"}).to_string();
                        if w.send(Message::Text(ping.into())).await.is_err() {
                            break;
                        }
                    }
                });

                let mut last_msg_at = tokio::time::Instant::now();

                loop {
                    let read_result = tokio::time::timeout(
                        Duration::from_secs(30),
                        read.next(),
                    )
                    .await;

                    match read_result {
                        Ok(Some(Ok(Message::Text(text)))) => {
                            last_msg_at = tokio::time::Instant::now();
                            let text_str: &str = &text;

                            if text_str.contains("\"op\"") {
                                continue;
                            }

                            if let Err(e) = handle_ticker(&feeds, text_str).await {
                                warn!(error = %e, "Failed to parse Bybit ticker");
                            }
                        }
                        Ok(Some(Ok(Message::Ping(data)))) => {
                            let mut w = ping_write.lock().await;
                            let _ = w.send(Message::Pong(data)).await;
                            last_msg_at = tokio::time::Instant::now();
                        }
                        Ok(Some(Ok(Message::Close(_)))) => {
                            warn!("Bybit WebSocket closed by server");
                            break;
                        }
                        Ok(Some(Err(e))) => {
                            error!(error = %e, "Bybit WebSocket error");
                            break;
                        }
                        Ok(None) => {
                            warn!("Bybit WebSocket stream ended");
                            break;
                        }
                        Err(_) => {
                            if last_msg_at.elapsed() > Duration::from_secs(60) {
                                warn!(
                                    stale_secs = last_msg_at.elapsed().as_secs(),
                                    "Bybit connection dead for >60s, forcing reconnect"
                                );
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                ping_task.abort();
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Bybit WebSocket");
            }
        }

        reconnect_count += 1;
        let backoff = std::cmp::min(3 * reconnect_count, 30);
        warn!(
            reconnect_in_secs = backoff,
            attempt = reconnect_count,
            "Bybit WebSocket disconnected, reconnecting..."
        );
        tokio::time::sleep(Duration::from_secs(backoff as u64)).await;
    }
}

async fn handle_ticker(feeds: &PriceFeeds, text: &str) -> Result<()> {
    let msg: TickerMsg = serde_json::from_str(text)?;

    if let Some(data) = msg.data {
        if let Some(ref price_str) = data.lastPrice {
            if let Ok(price) = Decimal::from_str(price_str) {
                let key = bybit_price_key(&data.symbol);
                BYBIT_MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                feeds.set_price(&key, price).await;
            }
        }
    }

    Ok(())
}
