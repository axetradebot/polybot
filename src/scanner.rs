use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::debug;

use crate::config::AppConfig;
use crate::feeds::PriceFeeds;
use crate::market;
use crate::orderbook;
use crate::positions::PositionTracker;
use crate::signal;
use crate::types::{Direction, MarketInfo};

/// A scored opportunity found by the scanner.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MarketOpportunity {
    pub market_name: String,
    pub asset: String,
    pub window_seconds: u64,
    pub window_ts: u64,
    pub slug: String,
    pub direction: Direction,
    pub delta_pct: f64,
    pub edge_score: f64,
    pub best_ask: Decimal,
    pub suggested_entry: Decimal,
    pub max_entry: Decimal,
    pub seconds_remaining: u64,
    pub token_id: String,
    pub condition_id: String,
    pub neg_risk: bool,
    pub depth_usd: Decimal,
    pub open_price: Decimal,
    pub contracts: Decimal,
    pub bet_size_usd: Decimal,
    pub entry_start_s: u64,
}

/// Compute the edge score for an opportunity.
///
/// `edge_score = delta_pct × (1.0 / entry_price) × liquidity_factor × time_factor`
fn compute_edge_score(
    delta_pct: f64,
    entry_price: f64,
    depth_usd: f64,
    seconds_remaining: u64,
    entry_start_s: u64,
    entry_cutoff_s: u64,
) -> f64 {
    if entry_price <= 0.0 {
        return 0.0;
    }

    let liquidity = (depth_usd / 1000.0).min(1.0);

    let time_range = (entry_start_s as f64) - (entry_cutoff_s as f64);
    let time_factor = if time_range > 0.0 {
        let elapsed = (entry_start_s as f64) - (seconds_remaining.min(entry_start_s) as f64);
        1.0 - 0.5 * (elapsed / time_range).min(1.0)
    } else {
        1.0
    };

    delta_pct * (1.0 / entry_price) * liquidity * time_factor
}

/// Scan all enabled markets and return opportunities sorted by edge_score (descending).
///
/// Only returns opportunities that pass all filters:
/// - Within entry window
/// - Delta above market's min_delta_pct
/// - Entry price below max for delta tier
/// - Not already holding a position in this window
pub async fn scan_all_markets(
    config: &AppConfig,
    price_feeds: &PriceFeeds,
    token_cache: &std::collections::HashMap<String, MarketInfo>,
    positions: &PositionTracker,
) -> Vec<MarketOpportunity> {
    let mut opportunities = Vec::new();
    let bet_size = Decimal::try_from(config.bankroll.bet_size_usd).unwrap_or(dec!(5));
    let undercut = Decimal::try_from(config.pricing.undercut_offset).unwrap_or(dec!(0.03));
    let min_entry = Decimal::try_from(config.pricing.min_entry_price).unwrap_or(dec!(0.55));
    let entry_cutoff_s = config.pricing.entry_cutoff_s;

    for mkt in config.enabled_markets() {
        let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);

        // Must be within entry window
        if secs_rem > mkt.entry_start_s || secs_rem < entry_cutoff_s {
            continue;
        }

        // Get current asset price
        let current_price = match price_feeds.get_price(&mkt.binance_symbol).await {
            Some(p) => p,
            None => continue,
        };

        if !price_feeds.has_fresh_price(&mkt.binance_symbol).await {
            continue;
        }

        // Get window open price
        let open_price = match price_feeds.get_window_open(&mkt.slug_prefix, window_ts).await {
            Some(p) => p,
            None => continue,
        };

        // Compute delta
        let (direction, abs_delta) = signal::compute_delta(current_price, open_price);
        let delta_f64: f64 = abs_delta.try_into().unwrap_or(0.0);

        if delta_f64 < mkt.min_delta_pct {
            continue;
        }

        // Check for existing position
        let slug = market::build_slug(&mkt.slug_prefix, window_ts);
        if positions.has_position_in_window(&slug).await {
            continue;
        }

        // Look up token info
        let market_info = match token_cache.get(&slug) {
            Some(info) if info.accepting_orders => info,
            _ => continue,
        };

        let token_id = match direction {
            Direction::Up => market_info.up_token_id.clone(),
            Direction::Down => market_info.down_token_id.clone(),
        };

        // Fetch orderbook for this token
        let ob = match orderbook::fetch_orderbook(&config.infra.polymarket_clob_url, &token_id)
            .await
        {
            Ok(ob) => ob,
            Err(e) => {
                debug!(market = %mkt.name, error = %e, "Orderbook fetch failed during scan");
                continue;
            }
        };

        // Compute entry price
        let mut suggested_entry = (ob.best_ask - undercut).round_dp(2);
        if suggested_entry < min_entry {
            suggested_entry = min_entry;
        }

        let max_entry =
            Decimal::try_from(mkt.max_price_for_delta(delta_f64)).unwrap_or(dec!(0.85));
        if suggested_entry > max_entry {
            continue;
        }

        // Compute contracts
        let contracts = (bet_size / suggested_entry)
            .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);
        if contracts < Decimal::ONE {
            continue;
        }
        let actual_bet = contracts * suggested_entry;

        let depth_f64: f64 = ob.depth_at_ask.try_into().unwrap_or(0.0);
        let entry_f64: f64 = suggested_entry.try_into().unwrap_or(0.5);

        let edge_score = compute_edge_score(
            delta_f64,
            entry_f64,
            depth_f64,
            secs_rem,
            mkt.entry_start_s,
            entry_cutoff_s,
        );

        if edge_score < config.scanner.min_edge_score {
            continue;
        }

        opportunities.push(MarketOpportunity {
            market_name: mkt.name.clone(),
            asset: mkt.asset.clone(),
            window_seconds: mkt.window_seconds,
            window_ts,
            slug,
            direction,
            delta_pct: delta_f64,
            edge_score,
            best_ask: ob.best_ask,
            suggested_entry,
            max_entry,
            seconds_remaining: secs_rem,
            token_id,
            condition_id: market_info.condition_id.clone(),
            neg_risk: market_info.neg_risk,
            depth_usd: ob.depth_at_ask,
            open_price,
            contracts,
            bet_size_usd: actual_bet,
            entry_start_s: mkt.entry_start_s,
        });
    }

    // Sort by edge_score descending
    opportunities.sort_by(|a, b| b.edge_score.partial_cmp(&a.edge_score).unwrap_or(std::cmp::Ordering::Equal));

    if !opportunities.is_empty() {
        debug!(
            count = opportunities.len(),
            top_score = opportunities[0].edge_score,
            top_market = %opportunities[0].market_name,
            "Scanner found opportunities"
        );
    }

    opportunities
}
