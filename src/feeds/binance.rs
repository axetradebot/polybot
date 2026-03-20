use anyhow::Result;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use std::str::FromStr;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use crate::state::SharedState;
use crate::types::BinanceTrade;

/// Spawn a Binance WebSocket task that streams BTC/USDT trades
/// and updates the shared state with the latest price.
pub async fn run_binance_feed(state: SharedState, ws_url: &str) -> Result<()> {
    loop {
        info!(url = %ws_url, "Connecting to Binance WebSocket...");

        match connect_async(ws_url).await {
            Ok((ws_stream, _response)) => {
                info!("Binance WebSocket connected");
                let (mut write, mut read) = ws_stream.split();

                while let Some(msg_result) = read.next().await {
                    match msg_result {
                        Ok(Message::Text(text)) => {
                            if let Err(e) = handle_trade_message(&state, &text).await {
                                debug!(error = %e, "Failed to parse Binance trade");
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

async fn handle_trade_message(state: &SharedState, text: &str) -> Result<()> {
    let trade: BinanceTrade = serde_json::from_str(text)?;
    let price = Decimal::from_str(&trade.price)?;

    let mut st = state.write().await;
    st.btc_price = price;
    st.btc_price_updated_at = Some(Utc::now());

    Ok(())
}
