use anyhow::{Context, Result};
use tracing::{info, warn};

/// Check if a market has been resolved and whether our position won.
pub async fn check_settlement(market_slug: &str) -> Result<Option<bool>> {
    use polymarket_client_sdk::gamma::Client as GammaClient;
    use polymarket_client_sdk::gamma::types::request::MarketBySlugRequest;

    let gamma = GammaClient::default();
    let request = MarketBySlugRequest::builder().slug(market_slug).build();

    match gamma.market_by_slug(&request).await {
        Ok(market) => {
            let is_closed = market.closed.unwrap_or(false);
            if !is_closed {
                return Ok(None);
            }

            let outcome_prices = market.outcome_prices.unwrap_or_default();
            if outcome_prices.len() >= 2 {
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

pub fn did_we_win(our_direction_is_up: bool, up_won: bool) -> bool {
    our_direction_is_up == up_won
}

/// Auto-redeem winning positions via the CTF contract on Polygon.
///
/// Returns the transaction hash on success.
pub async fn auto_redeem(
    rpc_url: &str,
    private_key: &str,
    condition_id_hex: &str,
    neg_risk: bool,
) -> Result<String> {
    use alloy::primitives::B256;
    use alloy::providers::ProviderBuilder;
    use alloy::signers::local::PrivateKeySigner;
    use polymarket_client_sdk::ctf::types::RedeemPositionsRequest;
    use polymarket_client_sdk::POLYGON;
    use std::str::FromStr;

    let signer: PrivateKeySigner = private_key.parse()
        .context("Failed to parse private key for CTF redemption")?;

    let provider = ProviderBuilder::new()
        .wallet(alloy::network::EthereumWallet::from(signer))
        .connect_http(rpc_url.parse().context("Invalid polygon RPC URL")?);

    // USDC.e on Polygon
    let usdc = alloy::primitives::address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

    // Parse the condition_id — it may be prefixed with "0x" or not
    let cid_clean = condition_id_hex.trim().trim_start_matches("0x");
    let condition_id = B256::from_str(cid_clean)
        .with_context(|| format!("Invalid condition_id hex: {condition_id_hex}"))?;

    info!(
        condition_id = %condition_id,
        neg_risk = neg_risk,
        "Sending CTF redeem transaction"
    );

    if neg_risk {
        let ctf_client = polymarket_client_sdk::ctf::Client::with_neg_risk(provider, POLYGON)
            .context("Failed to create neg-risk CTF client")?;

        let request = polymarket_client_sdk::ctf::types::RedeemNegRiskRequest::builder()
            .condition_id(condition_id)
            .amounts(vec![]) // empty = redeem all
            .build();

        let resp = ctf_client.redeem_neg_risk(&request).await
            .context("CTF neg-risk redeem transaction failed")?;

        Ok(format!("{:?}", resp.transaction_hash))
    } else {
        let ctf_client = polymarket_client_sdk::ctf::Client::new(provider, POLYGON)
            .context("Failed to create CTF client")?;

        let request = RedeemPositionsRequest::for_binary_market(usdc, condition_id);

        let resp = ctf_client.redeem_positions(&request).await
            .context("CTF redeem transaction failed")?;

        Ok(format!("{:?}", resp.transaction_hash))
    }
}
