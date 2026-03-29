use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rust_decimal::Decimal;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::AppConfig;
use crate::db::TradeDb;
use crate::feeds::PriceFeeds;
use crate::market;
use crate::orderbook::OrderbookCache;
use crate::signal;
use crate::types::MarketInfo;

pub type SharedTokenCache = Arc<RwLock<HashMap<String, MarketInfo>>>;
pub type SharedSlugMap = Arc<RwLock<HashMap<String, String>>>;

const SNAPSHOT_TARGETS: [u64; 8] = [120, 100, 60, 40, 30, 20, 10, 5];
const DELTA_THRESHOLD_PCT: f64 = 0.04;

fn nearest_snapshot(seconds_remaining: u64) -> Option<u64> {
    SNAPSHOT_TARGETS
        .iter()
        .find(|&&t| (seconds_remaining as i64 - t as i64).abs() <= 1)
        .copied()
}

/// Build a unique window_id string for the shadow_timing table.
fn build_window_id(slug_prefix: &str, window_seconds: u64, window_ts: u64) -> String {
    format!(
        "{}-{}m-{}",
        slug_prefix.split('-').next().unwrap_or(slug_prefix),
        window_seconds / 60,
        window_ts
    )
}

/// Runs the shadow timing observation loop as a separate async task.
/// Records price and orderbook snapshots at T-40, T-30, T-20, T-10, T-5
/// for every active window. Never places orders.
pub async fn run_shadow_timing(
    config: AppConfig,
    price_feeds: PriceFeeds,
    db: Arc<TradeDb>,
    token_cache: SharedTokenCache,
    hourly_slug_map: SharedSlugMap,
    ob_cache: OrderbookCache,
) {
    let clob_url = config.infra.polymarket_clob_url.clone();

    let sig_weights = signal::SignalWeights {
        delta: config.scanner.signal_weight_delta,
        velocity: config.scanner.signal_weight_velocity,
        volatility: config.scanner.signal_weight_volatility,
        acceleration: config.scanner.signal_weight_accel,
    };

    // Track snapshots we've already recorded: (market_name, window_ts, t_seconds)
    let mut recorded: HashSet<(String, u64, u64)> = HashSet::new();
    // Markets that had delta >= threshold at T-40 or T-30 (qualified for later snapshots)
    // Key: (market_name, window_ts), Value: true if qualified
    let mut qualified_early: HashMap<(String, u64), bool> = HashMap::new();

    info!("Shadow timing tracker started");

    loop {
        let cycle_start = tokio::time::Instant::now();
        let enabled = config.enabled_markets();

        for mkt in &enabled {
            let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);

            let t_sec = match nearest_snapshot(secs_rem) {
                Some(t) => t,
                None => continue,
            };

            // T-120/T-100/T-60 are early shadow snapshots — record all for analysis.
            // Normal snapshots (T-40 and below) are gated by entry_start_s.
            if t_sec > 60 {
                // Early shadow snapshots — always record (no delta filter)
            } else if t_sec > mkt.entry_start_s + 2 {
                continue;
            }

            let key = (mkt.name.clone(), window_ts, t_sec);
            if recorded.contains(&key) {
                continue;
            }

            let current_price = match price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
                Some(p) => p,
                None => continue,
            };
            let open_price = match price_feeds.get_window_open(&mkt.slug_prefix, window_ts).await
            {
                Some(p) => p,
                None => continue,
            };

            let (direction, abs_delta) = signal::compute_delta(current_price, open_price);
            let delta_f64: f64 = abs_delta.try_into().unwrap_or(0.0);
            let dir_str = direction.to_string();
            let current_f64: f64 = current_price.try_into().unwrap_or(0.0);
            let open_f64: f64 = open_price.try_into().unwrap_or(0.0);

            let window_id = build_window_id(&mkt.slug_prefix, mkt.window_seconds, window_ts);

            // Determine if we should fetch orderbooks for this snapshot.
            // For markets with entry_start_s <= 25, T-20 is the earliest snapshot
            // and acts as the qualifying "early" check (since T-40/T-30 are skipped).
            let is_early = t_sec >= 20;
            let market_window_key = (mkt.name.clone(), window_ts);

            let should_fetch_book = if delta_f64 >= DELTA_THRESHOLD_PCT {
                if is_early {
                    qualified_early.insert(market_window_key.clone(), true);
                }
                qualified_early.get(&market_window_key).copied().unwrap_or(true)
            } else {
                if is_early && !qualified_early.contains_key(&market_window_key) {
                    qualified_early.insert(market_window_key.clone(), false);
                }
                false
            };

            let (best_ask_winner, best_ask_loser, depth_w, depth_l, total_size_w, could_fill, hypo_entry) =
                if should_fetch_book {
                    match fetch_book_snapshot(
                        &clob_url,
                        mkt,
                        &token_cache,
                        &hourly_slug_map,
                        window_ts,
                        current_price,
                        open_price,
                        &ob_cache,
                    )
                    .await
                    {
                        Some(snap) => snap,
                        None => (None, None, 0, 0, 0.0, false, None),
                    }
                } else {
                    (None, None, 0, 0, 0.0, false, None)
                };

            let live_sym = PriceFeeds::live_price_symbol(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol);
            let ticks = price_feeds.get_ticks(&live_sym, 60).await;
            let tick_count_30s = {
                let cutoff = std::time::Instant::now() - Duration::from_secs(30);
                ticks.iter().filter(|t| t.ts >= cutoff).count() as i64
            };
            // Use Binance open for signal computation (consistent with scanner)
            let delta_open = price_feeds
                .get_binance_window_open(&mkt.slug_prefix, window_ts)
                .await
                .unwrap_or(open_price);
            let binance_live = price_feeds
                .get_price(&mkt.binance_symbol.to_lowercase())
                .await
                .unwrap_or(current_price);
            let (v5, v15, accel, range, score) = if !ticks.is_empty() {
                let sig = signal::compute_signals(binance_live, delta_open, &ticks, &sig_weights);
                (Some(sig.velocity_5s), Some(sig.velocity_15s), Some(sig.acceleration), Some(sig.range_30s), Some(sig.signal_score))
            } else {
                (None, None, None, None, None)
            };

            if let Err(e) = db.insert_shadow_timing(
                &window_id,
                &mkt.name,
                t_sec,
                delta_f64,
                &dir_str,
                current_f64,
                open_f64,
                best_ask_winner,
                best_ask_loser,
                depth_w,
                depth_l,
                total_size_w,
                could_fill,
                hypo_entry,
                v5,
                v15,
                accel,
                range,
                score,
                tick_count_30s,
            ) {
                warn!(error = %e, market = %mkt.name, t = t_sec, "Failed to insert shadow timing");
            } else {
                debug!(
                    market = %mkt.name,
                    t = t_sec,
                    delta = delta_f64,
                    direction = %dir_str,
                    book_fetched = should_fetch_book,
                    signal_score = score.unwrap_or(0.0),
                    tick_count = tick_count_30s,
                    "Shadow timing snapshot recorded"
                );
            }

            recorded.insert(key);
        }

        // Prune tracking data for windows that have closed
        let now_epoch = market::epoch_secs();
        recorded.retain(|(_name, wts, _t)| {
            // Keep entries for windows that haven't ended yet (max window = 3600s + margin)
            now_epoch.saturating_sub(*wts) < 7200
        });
        qualified_early.retain(|(_name, wts), _| now_epoch.saturating_sub(*wts) < 7200);

        // Prune stale orderbook cache entries periodically
        ob_cache.prune_stale().await;

        let elapsed = cycle_start.elapsed();
        let sleep_dur = Duration::from_millis(500).saturating_sub(elapsed);
        if !sleep_dur.is_zero() {
            tokio::time::sleep(sleep_dur).await;
        }
    }
}

/// Fetch both token orderbooks and compute the winner/loser breakdown.
/// Returns (best_ask_winner, best_ask_loser, depth_winner, depth_loser,
///          total_ask_size_winner, could_have_filled, hypothetical_entry_price)
#[allow(clippy::too_many_arguments)]
async fn fetch_book_snapshot(
    clob_url: &str,
    mkt: &crate::config::MarketConfig,
    token_cache: &SharedTokenCache,
    hourly_slug_map: &SharedSlugMap,
    window_ts: u64,
    _current_price: Decimal,
    _open_price: Decimal,
    ob_cache: &OrderbookCache,
) -> Option<(Option<f64>, Option<f64>, i64, i64, f64, bool, Option<f64>)> {
    // Resolve the slug for this market/window
    let slug = if mkt.is_hourly() {
        let map = hourly_slug_map.read().await;
        map.get(&mkt.name)?.clone()
    } else {
        market::build_slug(&mkt.slug_prefix, window_ts)
    };

    // Look up token IDs from the shared cache
    let cache = token_cache.read().await;
    let market_info = cache.get(&slug)?;
    let token_a = market_info.up_token_id.clone();
    let token_b = market_info.down_token_id.clone();
    drop(cache);

    // Fetch both books (via cache to avoid double-fetching with the live trader)
    let (book_a, book_b) = tokio::join!(
        ob_cache.get_or_fetch(clob_url, &token_a),
        ob_cache.get_or_fetch(clob_url, &token_b),
    );

    let book_a = book_a.ok()?;
    let book_b = book_b.ok()?;

    // Determine winner using mid-price comparison (same logic as scanner.rs):
    // the token with the higher mid-price is the "winner" side.
    // Since DetailedOrderbook only has ask-side data, use best_ask as a proxy.
    // A lower best_ask means the loser side (cheap shares = market thinks it loses).
    // If one side has asks and the other doesn't, the one with asks is likely tradeable
    let (winner_book, loser_book) = match (book_a.best_ask, book_b.best_ask) {
        (Some(a_ask), Some(b_ask)) => {
            // Higher ask = winner side (more expensive = market expects it to win)
            if a_ask >= b_ask {
                (book_a, book_b)
            } else {
                (book_b, book_a)
            }
        }
        (Some(_), None) => (book_a, book_b),
        (None, Some(_)) => (book_b, book_a),
        (None, None) => return Some((None, None, 0, 0, 0.0, false, None)),
    };

    let max_entry = mkt.max_entry_price;
    let undercut = mkt.undercut_offset.unwrap_or(0.03);

    let could_fill = winner_book
        .best_ask
        .map(|p| p <= max_entry)
        .unwrap_or(false);
    let hypo_entry = winner_book.best_ask.map(|p| {
        let entry = p - undercut;
        if entry < 0.30 { 0.30 } else { entry }
    });

    Some((
        winner_book.best_ask,
        loser_book.best_ask,
        winner_book.ask_depth,
        loser_book.ask_depth,
        winner_book.total_ask_size,
        could_fill,
        hypo_entry,
    ))
}
