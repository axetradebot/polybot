use anyhow::Result;
use futures::StreamExt;
use polymarket_client_sdk::types::U256;
use tracing::{debug, error, info, warn};

use crate::state::SharedState;

/// Spawn a Polymarket CLOB WebSocket task for orderbook updates.
pub async fn run_orderbook_feed(_state: SharedState, token_ids: Vec<U256>) -> Result<()> {
    use polymarket_client_sdk::clob::ws::Client as WsClient;

    info!(?token_ids, "Subscribing to Polymarket orderbook WebSocket...");

    let ws_client = WsClient::default();
    let stream = ws_client.subscribe_orderbook(token_ids)?;
    let mut stream = Box::pin(stream);

    while let Some(book_result) = stream.next().await {
        match book_result {
            Ok(book) => {
                debug!(
                    asset = %book.asset_id,
                    bids = book.bids.len(),
                    asks = book.asks.len(),
                    "Orderbook update"
                );
            }
            Err(e) => {
                error!(error = %e, "Polymarket orderbook WebSocket error");
                break;
            }
        }
    }

    warn!("Polymarket orderbook WebSocket disconnected");
    Ok(())
}

/// Subscribe to price changes for the given token IDs.
pub async fn run_price_feed(token_ids: Vec<U256>) -> Result<()> {
    use polymarket_client_sdk::clob::ws::Client as WsClient;

    info!(?token_ids, "Subscribing to Polymarket price feed...");

    let ws_client = WsClient::default();
    let stream = ws_client.subscribe_prices(token_ids)?;
    let mut stream = Box::pin(stream);

    while let Some(price_result) = stream.next().await {
        match price_result {
            Ok(price) => {
                debug!("Price update: {:?}", price);
            }
            Err(e) => {
                error!(error = %e, "Polymarket price WebSocket error");
                break;
            }
        }
    }

    warn!("Polymarket price WebSocket disconnected");
    Ok(())
}
