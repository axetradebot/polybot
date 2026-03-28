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
    pub velocity_5s: f64,
    pub velocity_15s: f64,
    pub acceleration: f64,
    pub range_30s: f64,
    pub signal_score: f64,
    pub is_early_limit: bool,
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
    pub velocity_5s: Option<f64>,
    pub range_30s: Option<f64>,
    pub signal_score: Option<f64>,
    pub result: String,
    pub detail: Option<String>,
}

/// Compute the edge score for an opportunity.
///
/// `edge_score = signal_score × (1.0 / entry_price) × liquidity_factor × time_factor`
fn compute_edge_score(
    signal_score: f64,
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

    signal_score * (1.0 / entry_price) * liquidity * time_factor
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

    let sig_weights = signal::SignalWeights {
        delta: config.scanner.signal_weight_delta,
        velocity: config.scanner.signal_weight_velocity,
        volatility: config.scanner.signal_weight_volatility,
        acceleration: config.scanner.signal_weight_accel,
    };

    for mkt in config.enabled_markets() {
        let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);
        let entry_cutoff_s = mkt.effective_entry_cutoff(config.pricing.entry_cutoff_s);

        // T-120 early entries disabled — 25% win rate at $0.30 is unprofitable
        let effective_start = mkt.entry_start_s;
        let in_early_window = false;

        if secs_rem > effective_start || secs_rem < entry_cutoff_s {
            debug!(
                market = %mkt.name,
                secs_remaining = secs_rem,
                entry_window = %format!("{}s-{}s", entry_cutoff_s, effective_start),
                "Outside entry window"
            );
            continue;
        }

        info!(
            market = %mkt.name,
            secs_remaining = secs_rem,
            early = in_early_window,
            "In entry window — evaluating"
        );

        let current_price = match price_feeds.get_live_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
            Some(p) => p,
            None => {
                info!(market = %mkt.name, source = %mkt.resolution_source, "Skip: no price data");
                skip_reasons.insert(mkt.name.clone(), "No price data".into());
                evaluations.push(ScanEvaluation {
                    market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                    direction: None, delta_pct: None, open_price: None, current_price: None,
                    best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                    suggested_entry: None, max_entry: None, edge_score: None,
                    velocity_5s: None, range_30s: None, signal_score: None,
                    result: "SKIP_NO_DATA".into(), detail: Some("No price data".into()),
                });
                continue;
            }
        };

        if !price_feeds.has_fresh_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
            info!(market = %mkt.name, "Skip: stale price data");
            skip_reasons.insert(mkt.name.clone(), "Stale price data".into());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: None, delta_pct: None, open_price: None, current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                velocity_5s: None, range_30s: None, signal_score: None,
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
                    velocity_5s: None, range_30s: None, signal_score: None,
                    result: "SKIP_NO_OPEN".into(), detail: Some("No open price for window".into()),
                });
                continue;
            }
        };

        // Use Binance open for delta calculation so USDT premium cancels out.
        // Both current_price (Binance live) and delta_open (Binance at T-300)
        // are in USDT, giving an unbiased delta magnitude.
        // Falls back to Chainlink open if Binance open wasn't captured.
        let delta_open = price_feeds
            .get_binance_window_open(&mkt.slug_prefix, window_ts)
            .await
            .unwrap_or(open_price);

        // Compute full signal bundle (delta + velocity + volatility + score)
        let live_sym = PriceFeeds::live_price_symbol(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol);
        let ticks = price_feeds.get_ticks(&live_sym, 60).await;
        let sig = signal::compute_signals(current_price, delta_open, &ticks, &sig_weights);

        // Direction must be determined from Chainlink (same source as open price
        // and Polymarket resolution) to avoid systematic USDT premium bias.
        let chainlink_price = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await;
        let direction = if let Some(cl) = chainlink_price {
            if cl >= open_price { Direction::Up } else { Direction::Down }
        } else {
            sig.direction.unwrap_or(Direction::Up)
        };
        let delta_f64 = sig.delta_pct;

        let required_delta = if in_early_window {
            mkt.early_entry_min_delta_pct
        } else {
            mkt.min_delta_pct
        };

        if delta_f64 < required_delta {
            info!(market = %mkt.name, direction = %direction, delta = delta_f64, min_delta = required_delta, early = in_early_window, "Skip: delta too low");
            let window_label = if in_early_window { "early" } else { "normal" };
            let detail = format!("Delta too low: {:.4}% < {:.2}% {window_label} min ({} ${open_price} → ${current_price})", delta_f64, required_delta, direction);
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_DELTA".into(), detail: Some(detail),
            });
            continue;
        }

        if config.scanner.min_signal_score > 0.0 && sig.signal_score < config.scanner.min_signal_score {
            let detail = format!("Signal too weak: {:.4} < {:.2} min", sig.signal_score, config.scanner.min_signal_score);
            info!(market = %mkt.name, signal = sig.signal_score, min = config.scanner.min_signal_score, delta = delta_f64, "Skip: signal score too low");
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_SIGNAL".into(), detail: Some(detail),
            });
            continue;
        }

        // ── Early limit order fast path ──
        // At T-120, orderbooks are often empty on the direction token.
        // Place a fixed-price limit bid at $0.30 and let it sit until filled.
        if in_early_window {
            let slug = if mkt.is_hourly() {
                match hourly_slugs.get(&mkt.name) {
                    Some(s) => s.clone(),
                    None => { continue; }
                }
            } else {
                market::build_slug(&mkt.slug_prefix, window_ts)
            };

            if positions.has_early_position_in_window(&slug).await {
                debug!(market = %mkt.name, "Skip: already have early limit in window");
                continue;
            }

            let market_info = match token_cache.get(&slug) {
                Some(info) if info.accepting_orders => info,
                Some(_) => {
                    debug!(market = %mkt.name, "Early limit: market not accepting orders yet");
                    continue;
                }
                None => {
                    debug!(market = %mkt.name, "Early limit: token not resolved yet");
                    continue;
                }
            };

            let trade_token_id = match direction {
                Direction::Up => market_info.up_token_id.clone(),
                Direction::Down => market_info.down_token_id.clone(),
            };

            let early_price = dec!(0.30);
            let early_contracts = Decimal::from_str_exact(&format!("{}", (config.bankroll.bet_size_usd / 0.30).floor() as u64)).unwrap_or(dec!(50));
            let early_contracts = early_contracts.max(dec!(5));
            let early_bet = early_contracts * early_price;

            info!(
                market = %mkt.name, direction = %direction, delta = delta_f64,
                price = %early_price, contracts = %early_contracts,
                secs_remaining = secs_rem,
                "EARLY LIMIT — placing $0.30 limit bid at T-{}", secs_rem
            );

            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: Some(early_price), max_entry: Some(early_price),
                edge_score: Some(sig.signal_score),
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "EARLY_LIMIT".into(), detail: None,
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
                edge_score: sig.signal_score,
                best_ask: early_price,
                best_bid: Decimal::ZERO,
                spread: Decimal::ZERO,
                suggested_entry: early_price,
                max_entry: early_price,
                seconds_remaining: secs_rem,
                token_id: trade_token_id,
                condition_id: market_info.condition_id.clone(),
                neg_risk: market_info.neg_risk,
                depth_usd: Decimal::ZERO,
                open_price,
                contracts: early_contracts,
                bet_size_usd: early_bet,
                entry_start_s: mkt.entry_start_s,
                is_taker: false,
                velocity_5s: sig.velocity_5s,
                velocity_15s: sig.velocity_15s,
                acceleration: sig.acceleration,
                range_30s: sig.range_30s,
                signal_score: sig.signal_score,
                is_early_limit: true,
            });

            continue;
        }

        // ── Normal entry flow (T-30 to T-5) ──

        // Volatility filter: skip if price range over last 30s is too low
        if config.scanner.min_volatility_pct > 0.0 && sig.range_30s < config.scanner.min_volatility_pct {
            let detail = format!("Volatility too low: {:.4}% range < {:.2}% min", sig.range_30s, config.scanner.min_volatility_pct);
            info!(market = %mkt.name, range_30s = sig.range_30s, min = config.scanner.min_volatility_pct, "Skip: low volatility");
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_LOW_VOLATILITY".into(), detail: Some(detail),
            });
            continue;
        }

        // Acceleration filter: require recent velocity to exceed longer-term velocity
        if config.scanner.require_acceleration && sig.acceleration <= 0.0 {
            let detail = format!("No acceleration: v5={:.4}% v15={:.4}% accel={:.4}%", sig.velocity_5s, sig.velocity_15s, sig.acceleration);
            info!(market = %mkt.name, v5 = sig.velocity_5s, v15 = sig.velocity_15s, accel = sig.acceleration, "Skip: no acceleration");
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                suggested_entry: None, max_entry: None, edge_score: None,
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_NO_ACCELERATION".into(), detail: Some(detail),
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
        // For normal entries: allow if the only position in window is an early limit
        if positions.has_normal_position_in_window(&slug).await {
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
                    velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                    signal_score: Some(sig.signal_score),
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
                    velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                    signal_score: Some(sig.signal_score),
                    result: "SKIP_TOKEN".into(), detail: None,
                });
                continue;
            }
        };

        // ── Direction-only token selection via CLOB mapping ──
        let clob = &config.infra.polymarket_clob_url;
        let sanity_floor: Decimal = SANITY_MIN_ASK.parse().unwrap();

        let trade_token_id = match direction {
            Direction::Up => market_info.up_token_id.clone(),
            Direction::Down => market_info.down_token_id.clone(),
        };

        let complement_token_id = match direction {
            Direction::Up => market_info.down_token_id.clone(),
            Direction::Down => market_info.up_token_id.clone(),
        };

        let ob = match orderbook::fetch_orderbook(clob, &trade_token_id).await {
            Ok(ob) => ob,
            Err(e) => {
                debug!(market = %mkt.name, direction = %direction,
                    "No direct asks on {} token, trying complement...", direction);

                match orderbook::fetch_orderbook_via_complement(clob, &complement_token_id).await {
                    Ok(comp_ob) => {
                        info!(market = %mkt.name, direction = %direction,
                            effective_ask = %comp_ob.best_ask,
                            "Using complement pricing for {} token", direction);
                        comp_ob
                    }
                    Err(ce) => {
                        let detail = format!(
                            "No asks on {} token: {e} (complement also empty: {ce})",
                            direction
                        );
                        debug!(market = %mkt.name, slug = %slug, direction = %direction,
                            token = %&trade_token_id[..trade_token_id.len().min(12)],
                            secs_rem = secs_rem, "{detail}");
                        skip_reasons.insert(mkt.name.clone(), format!("Orderbook: {detail}"));
                        evaluations.push(ScanEvaluation {
                            market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                            direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                            open_price: Some(open_price), current_price: Some(current_price),
                            best_ask: None, best_bid: None, spread: None, depth_at_ask: None,
                            suggested_entry: None, max_entry: None, edge_score: None,
                            velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                            signal_score: Some(sig.signal_score),
                            result: "SKIP_ORDERBOOK".into(), detail: Some(detail),
                        });
                        continue;
                    }
                }
            }
        };

        if ob.best_ask < sanity_floor {
            let detail = format!(
                "{direction} signal but trade token ask=${:.2} < ${sanity_floor} — market disagrees",
                ob.best_ask
            );
            debug!(market = %mkt.name, slug = %slug, direction = %direction,
                best_ask = %ob.best_ask, delta = delta_f64, "{detail}");
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid),
                spread: Some(ob.spread), depth_at_ask: Some(ob.depth_at_ask),
                suggested_entry: None, max_entry: None, edge_score: None,
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_SANITY".into(), detail: Some(detail),
            });
            continue;
        }

        let token_id = trade_token_id;

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

        let tier_ceiling = mkt.max_price_for_delta(delta_f64);
        let breakeven_ceiling = config.pricing.max_profitable_price();
        let max_entry = Decimal::try_from(tier_ceiling.min(breakeven_ceiling)).unwrap_or(dec!(0.85));
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
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_PRICE".into(), detail: Some(detail),
            });
            continue;
        }

        let effective_bet = if config.bankroll.dynamic_sizing && config.bankroll.baseline_signal > 0.0 {
            let mult = (sig.signal_score / config.bankroll.baseline_signal)
                .clamp(config.bankroll.min_bet_multiplier, config.bankroll.max_bet_multiplier);
            Decimal::try_from(config.bankroll.bet_size_usd * mult).unwrap_or(bet_size)
        } else {
            bet_size
        };
        // Scale down bet for low-delta trades (< 0.075%) to reduce risk
        // on marginal direction calls close to the price offset threshold.
        let effective_bet = if delta_f64 < 0.075 {
            let scale = (delta_f64 / 0.075).max(0.2);
            let scaled = Decimal::try_from(scale).unwrap_or(dec!(1)) * effective_bet;
            info!(market = %mkt.name, delta = delta_f64, scale, "Low-delta bet reduction");
            scaled
        } else {
            effective_bet
        };

        let min_contracts = dec!(5);
        let mut contracts = (effective_bet / suggested_entry).round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);
        if contracts < min_contracts {
            contracts = min_contracts;
        }
        let actual_bet = contracts * suggested_entry;

        let depth_f64: f64 = ob.depth_at_ask.try_into().unwrap_or(0.0);
        let entry_f64: f64 = suggested_entry.try_into().unwrap_or(0.5);

        let edge_score = compute_edge_score(sig.signal_score, entry_f64, depth_f64, secs_rem, mkt.entry_start_s, entry_cutoff_s);

        if edge_score < config.scanner.min_edge_score {
            info!(market = %mkt.name, edge = edge_score, min_edge = config.scanner.min_edge_score, delta = delta_f64, signal = sig.signal_score, "Skip: edge score too low");
            let detail = format!("Edge too low: {edge_score:.4} < {:.2} min (delta {delta_f64:.4}%, signal {:.4})", config.scanner.min_edge_score, sig.signal_score);
            skip_reasons.insert(mkt.name.clone(), detail.clone());
            evaluations.push(ScanEvaluation {
                market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
                direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
                open_price: Some(open_price), current_price: Some(current_price),
                best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid), spread: Some(ob.spread),
                depth_at_ask: Some(ob.depth_at_ask), suggested_entry: Some(suggested_entry),
                max_entry: Some(max_entry), edge_score: Some(edge_score),
                velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
                signal_score: Some(sig.signal_score),
                result: "SKIP_EDGE".into(), detail: Some(detail),
            });
            continue;
        }

        let order_type = if is_taker { "TAKER" } else { "MAKER" };
        info!(
            market = %mkt.name, direction = %direction, delta = delta_f64,
            edge = edge_score, signal = sig.signal_score,
            v5 = sig.velocity_5s, range = sig.range_30s, accel = sig.acceleration,
            entry = %suggested_entry, best_ask = %ob.best_ask,
            contracts = %contracts, order_type, "OPPORTUNITY FOUND"
        );

        skip_reasons.remove(&mkt.name);

        evaluations.push(ScanEvaluation {
            market_name: mkt.name.clone(), window_ts, secs_remaining: secs_rem,
            direction: Some(direction.to_string()), delta_pct: Some(delta_f64),
            open_price: Some(open_price), current_price: Some(current_price),
            best_ask: Some(ob.best_ask), best_bid: Some(ob.best_bid), spread: Some(ob.spread),
            depth_at_ask: Some(ob.depth_at_ask), suggested_entry: Some(suggested_entry),
            max_entry: Some(max_entry), edge_score: Some(edge_score),
            velocity_5s: Some(sig.velocity_5s), range_30s: Some(sig.range_30s),
            signal_score: Some(sig.signal_score),
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
            velocity_5s: sig.velocity_5s,
            velocity_15s: sig.velocity_15s,
            acceleration: sig.acceleration,
            range_30s: sig.range_30s,
            signal_score: sig.signal_score,
            is_early_limit: false,
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
