use anyhow::Result;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use super::PriceFeeds;
use crate::types::{BinanceCombinedMessage, BinanceTrade};

/// Run a Binance combined WebSocket stream for multiple symbols.
///
/// Uses the combined-stream endpoint:
///   wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade/...
///
/// Each message wraps the trade data with a `stream` field identifying the symbol.
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

    loop {
        info!(url = %url, symbols = ?symbols, "Connecting to Binance WebSocket...");

        match connect_async(&url).await {
            Ok((ws_stream, _response)) => {
                info!(symbols = ?symbols, "Binance WebSocket connected");
                let (mut write, mut read) = ws_stream.split();

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if symbols.len() == 1 {
                                if let Err(e) = handle_single_trade(&feeds, &text).await {
                                    debug!(error = %e, "Failed to parse single-stream trade");
                                }
                            } else if let Err(e) = handle_combined_trade(&feeds, &text).await {
                                debug!(error = %e, "Failed to parse combined-stream trade");
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

        warn!("Binance WebSocket disconnected, reconnecting in 3s...");
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

/// Handle a single-symbol stream message (legacy format, one symbol only).
async fn handle_single_trade(feeds: &PriceFeeds, text: &str) -> Result<()> {
    let trade: BinanceTrade = serde_json::from_str(text)?;
    let price = Decimal::from_str(&trade.price)?;
    let symbol = trade.symbol.to_lowercase();
    feeds.set_price(&symbol, price).await;
    Ok(())
}

/// Handle a combined-stream message with the `{stream, data}` wrapper.
async fn handle_combined_trade(feeds: &PriceFeeds, text: &str) -> Result<()> {
    let msg: BinanceCombinedMessage = serde_json::from_str(text)?;
    let price = Decimal::from_str(&msg.data.price)?;
    let symbol = msg.data.symbol.to_lowercase();
    feeds.set_price(&symbol, price).await;
    Ok(())
}
