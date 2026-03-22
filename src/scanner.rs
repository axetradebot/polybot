use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::feeds::PriceFeeds;
use crate::market;
use crate::orderbook;
use crate::positions::PositionTracker;
use crate::signal;
use crate::types::{Direction, MarketInfo};

const SANITY_MIN_ASK: &str = "0.30";

/// A scored opportunity found by the scanner.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MarketOpportunity {
    pub market_name: String,
    pub asset: String,
    pub market_type: String,
    pub window_seconds: u64,
    pub window_ts: u64,
    pub slug: String,
    pub direction: Direction,
    pub delta_pct: f64,
    pub edge_score: f64,
    pub best_ask: Decimal,
    pub best_bid: Decimal,
    pub spread: Decimal,
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
    pub is_taker: bool,
}

/// A single scanner evaluation record for analytics logging.
#[derive(Debug, Clone)]
pub struct ScanEvaluation {
    pub market_name: String,
    pub window_ts: u64,
    pub secs_remaining: u64,
    pub direction: Option<String>,
    pub delta_pct: Option<f64>,
    pub open_price: Option<Decimal>,
    pub current_price: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub best_bid: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub depth_at_ask: Option<Decimal>,
    pub suggested_entry: Option<Decimal>,
    pub max_entry: Option<Decimal>,
    pub edge_score: Option<f64>,
    pub result: String,
    pub detail: Option<String>,
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

/// Result of scanning all markets: opportunities + per-market skip reasons + evaluation log.
pub struct ScanResult {
    pub opportunities: Vec<MarketOpportunity>,
    pub skip_reasons: std::collections::HashMap<String, String>,
    pub evaluations: Vec<ScanEvaluation>,
}

/// Scan all enabled markets and return opportunities sorted by edge_score (descending),
/// plus skip reasons for markets that didn't produce opportunities.
pub async fn scan_all_markets(
    config: &AppConfig,
    price_feeds: &PriceFeeds,
    token_cache: &std::collections::HashMap<String, MarketInfo>,
    positions: &PositionTracker,
    hourly_slugs: &std::collections::HashMap<String, String>,
) -> ScanResult {
    let mut opportunities = Vec::new();
    let mut skip_reasons = std::collections::HashMap::new();
    let mut evaluations = Vec::new();
    let bet_size = Decimal::try_from(config.bankroll.bet_size_usd).unwrap_or(dec!(5));
    let min_entry = Decimal::try_from(config.pricing.min_entry_price).unwrap_or(dec!(0.55));

    for mkt in config.enabled_markets() {
        let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);
        let entry_cutoff_s = mkt.effective_entry_cutoff(config.pricing.entry_cutoff_s);

        if secs_rem > mkt.entry_start_s || secs_rem < entry_cutoff_s {
            debug!(
                market = %mkt.name,
                secs_remaining = secs_rem,
                entry_window = %format!("{}s-{}s", entry_cutoff_s, mkt.entry_start_s),
                "Outside entry window"
            );
            continue;
        }

        info!(
            market = %mkt.name,
            secs_remaining = secs_rem,
            "In entry window — evaluating"
        );

        let current_price = match price_feeds.get_price(&mkt.binance_symbol).await {
            Some(p) => p,
            None => {
                info!(market = %mkt.name, "Skip: no price data");
                skip_reasons.insert(mkt.name.clone(), "No price data from Binance".into());
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: None, delta_pct: None, open_price: None, current_price: None,
                    best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                    suggested_entry: None, max_entry: None, edge_score: None,
                    result: "SKIP_NO_DATA".into(), detail: Some("No price data from Binance".into()),
                });
                continue;
            }
        };

        if !price_feeds.has_fresh_price(&mkt.binance_symbol).await {
            info!(market = %mkt.name, "Skip: stale price data");
            skip_reasons.insert(mkt.name.clone(), "Stale price data".into());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: None, delta_pct: None, open_price: None, current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                result: "SKIP_STALE".into(), detail: Some("Stale price data".into()),
            });
            continue;
        }

        let open_price = match price_feeds.get_window_open(&mkt.slug_prefix, window_ts).await {
            Some(p) => p,
            None => {
                info!(market = %mkt.name, "Skip: no open price for window");
                skip_reasons.insert(mkt.name.clone(), "No open price captured for window".into());
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: None, delta_pct: None, open_price: None, current_price: Some(current_price),
                    best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                    suggested_entry: None, max_entry: None, edge_score: None,
                    result: "SKIP_NO_OPEN".into(), detail: Some("No open price for window".into()),
                });
                continue;
            }
        };

        let (direction, abs_delta) = signal::compute_delta(current_price, open_price);
        let delta_f64: f64 = abs_delta.try_into().unwrap_or(0.0);

        if delta_f64 < mkt.min_delta_pct {
            info!(market = %mkt.name, direction = %direction, delta = delta_f64, min_delta = mkt.min_delta_pct, "Skip: delta too low");
            let detail = format!("Delta too low: {:.4}% < {:.2}% min ({} ${open_price} → ${current_price})", delta_f64, mkt.min_delta_pct, direction);
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                result: "SKIP_DELTA".into(), detail: Some(detail),
            });
            continue;
        }

        let slug = if mkt.is_hourly() {
            match hourly_slugs.get(&mkt.name) {
                Some(s) => s.clone(),
                None => {
                    debug!(market = %mkt.name, "Skip: no active hourly market discovered");
                    continue;
                }
            }
        } else {
            market::build_slug(&mkt.slug_prefix, window_ts)
        };
        if positions.has_position_in_window(&slug).await {
            debug!(market = %mkt.name, slug = %slug, "Skip: already have position");
            skip_reasons.insert(mkt.name.clone(), "Already have position in this window".into());
            continue;
        }

        let market_info = match token_cache.get(&slug) {
            Some(info) if info.accepting_orders => info,
            Some(_) => {
                info!(market = %mkt.name, slug = %slug, "Skip: market not accepting orders");
                skip_reasons.insert(mkt.name.clone(), "Market not accepting orders".into());
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                    open_price: Some(open_price), current_price: Some(current_price),
                    best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                    suggested_entry: None, max_entry: None, edge_score: None,
                    result: "SKIP_NOT_ACCEPTING".into(), detail: None,
                });
                continue;
            }
            None => {
                info!(market = %mkt.name, slug = %slug, "Skip: token not resolved yet");
                skip_reasons.insert(mkt.name.clone(), "Token not resolved from Gamma API".into());
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                    open_price: Some(open_price), current_price: Some(current_price),
                    best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                    suggested_entry: None, max_entry: None, edge_score: None,
                    result: "SKIP_TOKEN".into(), detail: None,
                });
                continue;
            }
        };

        // ── Price-based token detection ──
        // Fetch BOTH tokens' orderbooks (lenient — need both for price comparison).
        // The token with the higher mid-price is the one the market thinks will win.
        // This replaces string-based "Up"/"Down"/"Yes"/"No" label matching entirely.
        let clob = &config.infra.polymarket_clob_url;
        let token_a_id = &market_info.up_token_id;
        let token_b_id = &market_info.down_token_id;
        let (ob_a, ob_b) = tokio::join!(
            orderbook::fetch_orderbook_lenient(clob, token_a_id),
            orderbook::fetch_orderbook_lenient(clob, token_b_id),
        );

        // Settled market detection: if BOTH tokens have best_ask > $0.95, skip.
        let settled_threshold = Decimal::new(95, 2);
        if let (Ok(ref a), Ok(ref b)) = (&ob_a, &ob_b) {
            if a.best_ask > settled_threshold && b.best_ask > settled_threshold {
                let detail = format!(
                    "Market settled: both asks > $0.95 (A={}, B={})",
                    a.best_ask, b.best_ask
                );
                warn!(market = %mkt.name, slug = %slug, ask_a = %a.best_ask, ask_b = %b.best_ask, "Skip: market appears SETTLED");
                skip_reasons.insert(mkt.name.clone(), detail.clone());
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                    open_price: Some(open_price), current_price: Some(current_price),
                    best_ask: Some(a.best_ask), best_bid: Some(a.best_bid),
                    spread: Some(a.spread), depth_at_ask: Some(a.depth_at_ask),
                    suggested_entry: None, max_entry: None, edge_score: None,
                    result: "SKIP_SETTLED".into(), detail: Some(detail),
                });
                continue;
            }
        }

        // Determine which token to buy using mid-price comparison.
        // Higher mid-price = the token the market believes will win.
        // We always buy the higher-priced token (it matches our directional signal).
        let (token_id, _other_token_id, our_ob, other_ob) = {
            let mid_a = ob_a.as_ref().ok().map(|a| (a.best_bid + a.best_ask) / Decimal::from(2));
            let mid_b = ob_b.as_ref().ok().map(|b| (b.best_bid + b.best_ask) / Decimal::from(2));

            let swap = match (mid_a, mid_b) {
                (Some(ma), Some(mb)) => {
                    info!(
                        market = %mkt.name,
                        slug = %slug,
                        direction = %direction,
                        mid_a = %ma,
                        mid_b = %mb,
                        selected = if ma >= mb { "A" } else { "B" },
                        "Price-based token detection"
                    );
                    ma < mb
                }
                _ => false,
            };

            if swap {
                (token_b_id.clone(), token_a_id.clone(), ob_b, ob_a)
            } else {
                (token_a_id.clone(), token_b_id.clone(), ob_a, ob_b)
            }
        };

        // Validate our token has real asks (lenient fetch defaults best_ask to $1.00 when empty)
        let ob = match our_ob {
            Ok(ob) if ob.best_ask < Decimal::ONE => ob,
            Ok(_) | Err(_) => {
                let detail = match &other_ob {
                    Ok(other) => format!(
                        "No asks on selected token; other: ask=${:.2} bid=${:.2}",
                        other.best_ask, other.best_bid
                    ),
                    Err(_) => "Both orderbook fetches failed".to_string(),
                };
                info!(market = %mkt.name, slug = %slug, "Skip: selected token has no asks");
                skip_reasons.insert(mkt.name.clone(), format!("Orderbook: {detail}"));
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                    open_price: Some(open_price), current_price: Some(current_price),
                    best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                    suggested_entry: None, max_entry: None, edge_score: None,
                    result: "SKIP_ORDERBOOK".into(), detail: Some(detail),
                });
                continue;
            }
        };

        // Sanity: if our token's best_ask is below $0.30, the market strongly
        // disagrees with our signal — DO NOT trade.
        let sanity_floor: Decimal = SANITY_MIN_ASK.parse().unwrap();
        if ob.best_ask < sanity_floor {
            let detail = format!(
                "SANITY FAIL: {direction} signal but selected token ask=${:.2} < ${sanity_floor}",
                ob.best_ask
            );
            warn!(market = %mkt.name, slug = %slug, direction = %direction, best_ask = %ob.best_ask, delta = delta_f64, "{detail}");
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid), spread: Some(ob.spread),
                depth_at_ask: Some(ob.depth_at_ask), suggested_entry: None,
                max_entry: None, edge_score: None,
                result: "SKIP_SANITY".into(), detail: Some(detail),
            });
            continue;
        }

        let undercut = Decimal::try_from(mkt.effective_undercut_offset(config.pricing.undercut_offset)).unwrap_or(dec!(0.03));
        let taker_threshold = config.pricing.taker_delta_threshold;
        let is_taker = taker_threshold > 0.0 && delta_f64 >= taker_threshold;
        let mut suggested_entry = if is_taker {
            ob.best_ask
        } else {
            (ob.best_ask - undercut).round_dp(2)
        };
        // Never post above the best ask — that would overpay
        if suggested_entry > ob.best_ask {
            suggested_entry = ob.best_ask;
        }
        if suggested_entry < min_entry {
            suggested_entry = min_entry;
        }
        // Final guard: if min_entry pushed us above best_ask, cap at best_ask
        if suggested_entry > ob.best_ask {
            suggested_entry = ob.best_ask;
        }

        let max_entry = Decimal::try_from(mkt.max_price_for_delta(delta_f64)).unwrap_or(dec!(0.85));
        if suggested_entry > max_entry {
            info!(market = %mkt.name, direction = %direction, delta = delta_f64, best_ask = %ob.best_ask, suggested = %suggested_entry, ceiling = %max_entry, "Skip: entry price too high for delta");
            let detail = format!("Entry price too high: ${suggested_entry} > ${max_entry} ceiling (delta {delta_f64:.4}%, ask ${:.2})", ob.best_ask);
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid), spread: Some(ob.spread),
                depth_at_ask: Some(ob.depth_at_ask), suggested_entry: Some(suggested_entry),
                max_entry: Some(max_entry), edge_score: None,
                result: "SKIP_PRICE".into(), detail: Some(detail),
            });
            continue;
        }

        let contracts = (bet_size / suggested_entry).round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);
        if contracts < Decimal::ONE {
            info!(market = %mkt.name, entry = %suggested_entry, "Skip: bet too small for 1 contract");
            skip_reasons.insert(mkt.name.clone(), format!("Bet too small at ${suggested_entry} entry"));
            continue;
        }
        let actual_bet = contracts * suggested_entry;

        let depth_f64: f64 = ob.depth_at_ask.try_into().unwrap_or(0.0);
        let entry_f64: f64 = suggested_entry.try_into().unwrap_or(0.5);

        let edge_score = compute_edge_score(delta_f64, entry_f64, depth_f64, secs_rem, mkt.entry_start_s, entry_cutoff_s);

        if edge_score < config.scanner.min_edge_score {
            info!(market = %mkt.name, edge = edge_score, min_edge = config.scanner.min_edge_score, delta = delta_f64, "Skip: edge score too low");
            let detail = format!("Edge too low: {edge_score:.4} < {:.2} min (delta {delta_f64:.4}%)", config.scanner.min_edge_score);
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid), spread: Some(ob.spread),
                depth_at_ask: Some(ob.depth_at_ask), suggested_entry: Some(suggested_entry),
                max_entry: Some(max_entry), edge_score: Some(edge_score),
                result: "SKIP_EDGE".into(), detail: Some(detail),
            });
            continue;
        }

        let order_type = if is_taker { "TAKER" } else { "MAKER" };
        info!(market = %mkt.name, direction = %direction, delta = delta_f64, edge = edge_score, entry = %suggested_entry, best_ask = %ob.best_ask, contracts = %contracts, order_type, "OPPORTUNITY FOUND");

        skip_reasons.remove(&mkt.name);

        evaluations.push(ScanEvaluation {
            market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
            direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
            open_price: Some(open_price), current_price: Some(current_price),
            best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid), spread: Some(ob.spread),
            depth_at_ask: Some(ob.depth_at_ask), suggested_entry: Some(suggested_entry),
            max_entry: Some(max_entry), edge_score: Some(edge_score),
            result: "OPPORTUNITY".into(), detail: None,
        });

        opportunities.push(MarketOpportunity {
            market_name: mkt.name.clone(),
            asset: mkt.asset.clone(),
            market_type: mkt.market_type.clone(),
            window_seconds: mkt.window_seconds,
            window_ts,
            slug,
            direction,
            delta_pct: delta_f64,
            edge_score,
            best_ask: ob.best_ask,
            best_bid: ob.best_bid,
            spread: ob.spread,
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
            is_taker,
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
        evaluations,
    }
}
