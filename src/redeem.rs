use anyhow::Result;
use tracing::{info, warn};

use crate::types::MarketInfo;

/// Check if a market has been resolved and whether our position won.
///
/// Uses the Gamma API to check resolution status.
pub async fn check_settlement(market_slug: &str) -> Result<Option<bool>> {
    use polymarket_client_sdk::gamma::Client as GammaClient;
    use polymarket_client_sdk::gamma::types::request::MarketBySlugRequest;

    let gamma = GammaClient::default();
    let request = MarketBySlugRequest::builder().slug(market_slug).build();

    match gamma.market_by_slug(&request).await {
        Ok(market) => {
            let is_closed = market.closed.unwrap_or(false);
            if !is_closed {
                return Ok(None); // Not yet resolved
            }

            let outcome_prices = market.outcome_prices.unwrap_or_default();
            if outcome_prices.len() >= 2 {
                // $1.00 outcome = winning side
                let up_won = outcome_prices[0] == rust_decimal::Decimal::ONE;
                info!(
                    slug = %market_slug,
                    up_won = up_won,
                    "Market resolved"
                );
                Ok(Some(up_won))
            } else {
                warn!(slug = %market_slug, "Market resolved but no outcome prices");
                Ok(None)
            }
        }
        Err(e) => {
            warn!(error = %e, slug = %market_slug, "Failed to check settlement");
            Err(e.into())
        }
    }
}

/// Determine if our trade won based on direction and market resolution.
pub fn did_we_win(our_direction_is_up: bool, up_won: bool) -> bool {
    our_direction_is_up == up_won
}

/// Auto-redemption of winning positions.
///
/// For v1, this logs the winning position for manual redemption.
/// CTF contract interaction for on-chain redemption can be added when
/// a Polygon RPC endpoint is configured.
pub async fn redeem_position(market: &MarketInfo, won: bool) -> Result<()> {
    if won {
        info!(
            slug = %market.slug,
            condition_id = %market.condition_id,
            "Position won! Redemption available."
        );
        // On-chain redemption via CTF contract requires a Polygon provider.
        // For now, winning positions are redeemed automatically by Polymarket
        // after a short delay, or can be claimed via the web UI.
    } else {
        info!(slug = %market.slug, "Position lost, no redemption needed");
    }
    Ok(())
}
