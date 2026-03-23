use anyhow::{Context, Result};
use tracing::{info, warn};

/// Check if a market has been resolved and whether the token we bought won.
///
/// Matches `our_token_id` against `clob_token_ids` to find which index we
/// bought, then checks if `outcome_prices[our_index] == 1.0`. This is the
/// only reliable way to determine win/loss — never infer from price feeds.
pub async fn check_settlement(market_slug: &str, our_token_id: &str) -> Result<Option<bool>> {
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
            let token_ids = market.clob_token_ids.unwrap_or_default();

            if outcome_prices.len() < 2 || token_ids.len() < 2 {
                warn!(
                    slug = %market_slug,
                    outcomes = outcome_prices.len(),
                    tokens = token_ids.len(),
                    "Market resolved but missing outcome_prices or token_ids"
                );
                return Ok(None);
            }

            let our_index = token_ids.iter().position(|t| t.to_string() == our_token_id);

            match our_index {
                Some(idx) => {
                    let won = outcome_prices[idx] == rust_decimal::Decimal::ONE;
                    info!(
                        slug = %market_slug,
                        our_token = %our_token_id,
                        our_index = idx,
                        our_outcome_price = %outcome_prices[idx],
                        won = won,
                        "Market resolved — token-based settlement"
                    );
                    Ok(Some(won))
                }
                None => {
                    warn!(
                        slug = %market_slug,
                        our_token = %our_token_id,
                        api_tokens = ?token_ids.iter().map(|t| t.to_string()).collect::<Vec<_>>(),
                        "Our token_id not found in market's clob_token_ids"
                    );
                    Ok(None)
                }
            }
        }
        Err(e) => {
            warn!(error = %e, slug = %market_slug, "Failed to check settlement");
            Err(e.into())
        }
    }
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
