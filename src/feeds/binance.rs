use anyhow::Result;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{error, info, warn};

use super::PriceFeeds;
use crate::types::{BinanceCombinedMessage, BinanceTrade};

static MSG_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn messages_received() -> u64 {
    MSG_COUNT.load(Ordering::Relaxed)
}

pub async fn run_combined_feed(
    feeds: PriceFeeds,
    symbols: &[String],
    ws_base: &str,
) -> Result<()> {
    let streams: Vec<String> = symbols.iter().map(|s| format!("{}@trade", s)).collect();
    let streams_param = streams.join("/");

    let base = ws_base.trim_end_matches('/');
    let url = if symbols.len() == 1 {
        format!("{}/{}@trade", base, symbols[0])
    } else {
        let base_for_combined = base.replace("/ws", "/stream");
        format!("{}?streams={}", base_for_combined, streams_param)
    };

    let mut reconnect_count = 0u32;

    loop {
        info!(url = %url, symbols = ?symbols, reconnects = reconnect_count, "Connecting to Binance WebSocket...");

        match connect_async(&url).await {
            Ok((ws_stream, _response)) => {
                info!(symbols = ?symbols, "Binance WebSocket connected");
                reconnect_count = 0;
                let (mut write, mut read) = ws_stream.split();

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            MSG_COUNT.fetch_add(1, Ordering::Relaxed);
                            if symbols.len() == 1 {
                                if let Err(e) = handle_single_trade(&feeds, &text).await {
                                    warn!(error = %e, "Failed to parse single-stream trade");
                                }
                            } else if let Err(e) = handle_combined_trade(&feeds, &text).await {
                                warn!(error = %e, text = %text.chars().take(200).collect::<String>(), "Failed to parse combined-stream trade");
                            }
                        }
                        Ok(Message::Ping(data)) => {
                            if let Err(e) = write.send(Message::Pong(data)).await {
                                warn!(error = %e, "Failed to send pong");
                            }
                        }
                        Ok(Message::Close(_)) => {
                            warn!("Binance WebSocket closed by server");
                            break;
                        }
                        Err(e) => {
                            error!(error = %e, "Binance WebSocket error");
                            break;
                        }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to connect to Binance WebSocket");
            }
        }

        reconnect_count += 1;
        let backoff = std::cmp::min(3 * reconnect_count, 30);
        warn!(reconnect_in_secs = backoff, attempt = reconnect_count, "Binance WebSocket disconnected, reconnecting...");
        tokio::time::sleep(std::time::Duration::from_secs(backoff as u64)).await;
    }
}

async fn handle_single_trade(feeds: &PriceFeeds, text: &str) -> Result<()> {
    let trade: BinanceTrade = serde_json::from_str(text)?;
    let price = Decimal::from_str(&trade.price)?;
    let symbol = trade.symbol.to_lowercase();
    feeds.set_price(&symbol, price).await;
    if let Ok(qty) = trade.quantity.parse::<f64>() {
        if let Ok(px) = trade.price.parse::<f64>() {
            feeds.record_volume(&symbol, qty * px).await;
        }
    }
    Ok(())
}

async fn handle_combined_trade(feeds: &PriceFeeds, text: &str) -> Result<()> {
    let msg: BinanceCombinedMessage = serde_json::from_str(text)?;
    let price = Decimal::from_str(&msg.data.price)?;
    let symbol = msg.data.symbol.to_lowercase();
    feeds.set_price(&symbol, price).await;
    if let Ok(qty) = msg.data.quantity.parse::<f64>() {
        if let Ok(px) = msg.data.price.parse::<f64>() {
            feeds.record_volume(&symbol, qty * px).await;
        }
    }
    Ok(())
}
