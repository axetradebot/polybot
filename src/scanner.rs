use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info};

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

/// Result of scanning all markets: opportunities + per-market skip reasons.
pub struct ScanResult {
    pub opportunities: Vec<MarketOpportunity>,
    /// Per-market skip reason for markets that were evaluated but didn't produce an opportunity.
    /// Key: market name, Value: human-readable reason with detail.
    pub skip_reasons: std::collections::HashMap<String, String>,
}

/// Scan all enabled markets and return opportunities sorted by edge_score (descending),
/// plus skip reasons for markets that didn't produce opportunities.
pub async fn scan_all_markets(
    config: &AppConfig,
    price_feeds: &PriceFeeds,
    token_cache: &std::collections::HashMap<String, MarketInfo>,
    positions: &PositionTracker,
) -> ScanResult {
    let mut opportunities = Vec::new();
    let mut skip_reasons = std::collections::HashMap::new();
    let bet_size = Decimal::try_from(config.bankroll.bet_size_usd).unwrap_or(dec!(5));
    let undercut = Decimal::try_from(config.pricing.undercut_offset).unwrap_or(dec!(0.03));
    let min_entry = Decimal::try_from(config.pricing.min_entry_price).unwrap_or(dec!(0.55));
    let entry_cutoff_s = config.pricing.entry_cutoff_s;

    for mkt in config.enabled_markets() {
        let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);

        // Must be within entry window
        if secs_rem > mkt.entry_start_s || secs_rem < entry_cutoff_s {
            debug!(
                market = %mkt.name,
                secs_remaining = secs_rem,
                entry_window = %format!("{}s-{}s", entry_cutoff_s, mkt.entry_start_s),
                "Outside entry window"
            );
            skip_reasons.entry(mkt.name.clone()).or_insert_with(|| {
                format!("Outside entry window (T-{secs_rem}s, need {}-{}s)", entry_cutoff_s, mkt.entry_start_s)
            });
            continue;
        }

        info!(
            market = %mkt.name,
            secs_remaining = secs_rem,
            "In entry window — evaluating"
        );

        // Get current asset price
        let current_price = match price_feeds.get_price(&mkt.binance_symbol).await {
            Some(p) => p,
            None => {
                info!(market = %mkt.name, "Skip: no price data");
                skip_reasons.insert(mkt.name.clone(), "No price data from Binance".into());
                continue;
            }
        };

        if !price_feeds.has_fresh_price(&mkt.binance_symbol).await {
            info!(market = %mkt.name, "Skip: stale price data");
            skip_reasons.insert(mkt.name.clone(), "Stale price data".into());
            continue;
        }

        // Get window open price
        let open_price = match price_feeds.get_window_open(&mkt.slug_prefix, window_ts).await {
            Some(p) => p,
            None => {
                info!(market = %mkt.name, "Skip: no open price for window");
                skip_reasons.insert(mkt.name.clone(), "No open price captured for window".into());
                continue;
            }
        };

        // Compute delta
        let (direction, abs_delta) = signal::compute_delta(current_price, open_price);
        let delta_f64: f64 = abs_delta.try_into().unwrap_or(0.0);

        if delta_f64 < mkt.min_delta_pct {
            info!(
                market = %mkt.name,
                direction = %direction,
                delta = delta_f64,
                min_delta = mkt.min_delta_pct,
                open = %open_price,
                current = %current_price,
                "Skip: delta too low"
            );
            skip_reasons.insert(mkt.name.clone(), format!(
                "Delta too low: {:.4}% < {:.2}% min ({} ${open_price} → ${current_price})",
                delta_f64, mkt.min_delta_pct, direction
            ));
            continue;
        }

        // Check for existing position
        let slug = market::build_slug(&mkt.slug_prefix, window_ts);
        if positions.has_position_in_window(&slug).await {
            debug!(market = %mkt.name, slug = %slug, "Skip: already have position");
            skip_reasons.insert(mkt.name.clone(), "Already have position in this window".into());
            continue;
        }

        // Look up token info
        let market_info = match token_cache.get(&slug) {
            Some(info) if info.accepting_orders => info,
            Some(_) => {
                info!(market = %mkt.name, slug = %slug, "Skip: market not accepting orders");
                skip_reasons.insert(mkt.name.clone(), "Market not accepting orders".into());
                continue;
            }
            None => {
                info!(market = %mkt.name, slug = %slug, "Skip: token not resolved yet");
                skip_reasons.insert(mkt.name.clone(), "Token not resolved from Gamma API".into());
                continue;
            }
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
                info!(market = %mkt.name, error = %e, "Skip: orderbook fetch failed");
                skip_reasons.insert(mkt.name.clone(), format!("Orderbook fetch failed: {e}"));
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
            info!(
                market = %mkt.name,
                direction = %direction,
                delta = delta_f64,
                best_ask = %ob.best_ask,
                suggested = %suggested_entry,
                ceiling = %max_entry,
                "Skip: entry price too high for delta"
            );
            skip_reasons.insert(mkt.name.clone(), format!(
                "Entry price too high: ${suggested_entry} > ${max_entry} ceiling (delta {delta_f64:.4}%, ask ${:.2})",
                ob.best_ask
            ));
            continue;
        }

        // Compute contracts
        let contracts = (bet_size / suggested_entry)
            .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);
        if contracts < Decimal::ONE {
            info!(market = %mkt.name, entry = %suggested_entry, "Skip: bet too small for 1 contract");
            skip_reasons.insert(mkt.name.clone(), format!("Bet too small at ${suggested_entry} entry"));
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
            info!(
                market = %mkt.name,
                edge = edge_score,
                min_edge = config.scanner.min_edge_score,
                delta = delta_f64,
                entry = %suggested_entry,
                depth = depth_f64,
                "Skip: edge score too low"
            );
            skip_reasons.insert(mkt.name.clone(), format!(
                "Edge too low: {edge_score:.4} < {:.2} min (delta {delta_f64:.4}%)",
                config.scanner.min_edge_score
            ));
            continue;
        }

        info!(
            market = %mkt.name,
            direction = %direction,
            delta = delta_f64,
            edge = edge_score,
            entry = %suggested_entry,
            best_ask = %ob.best_ask,
            contracts = %contracts,
            "OPPORTUNITY FOUND"
        );

        // Found an opportunity — remove any skip reason for this market
        skip_reasons.remove(&mkt.name);

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
    opportunities.sort_by(|a, b| {
        b.edge_score
            .partial_cmp(&a.edge_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    if !opportunities.is_empty() {
        info!(
            count = opportunities.len(),
            top_score = opportunities[0].edge_score,
            top_market = %opportunities[0].market_name,
            "Scanner found opportunities"
        );
    }

    ScanResult {
        opportunities,
        skip_reasons,
    }
}
