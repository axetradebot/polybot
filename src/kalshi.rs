use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use kalshi::{
    Action, Kalshi, Market, OrderType, SettlementResult, Side, TradingEnvironment,
};
use rust_decimal::Decimal;
use tracing::{debug, error, info, warn};

use crate::config::KalshiConfig;
use crate::db::TradeDb;
use crate::feeds::PriceFeeds;
use crate::telegram::TelegramNotifier;

#[allow(dead_code)]
fn kalshi_fee(price_cents: i64) -> f64 {
    let p = price_cents as f64 / 100.0;
    (0.07 * p * (1.0 - p) * 100.0).ceil() / 100.0
}

/// Convert a decimal price (0.0–1.0) to Kalshi cents (1–99).
fn price_to_cents(price: f64) -> i64 {
    (price * 100.0).round() as i64
}

/// Convert Kalshi cents (1–99) to a decimal price (0.0–1.0).
fn cents_to_price(cents: i64) -> f64 {
    cents as f64 / 100.0
}

/// Extract the best ask (lowest offer price) from a Kalshi orderbook side.
/// Each level is `[price_cents, quantity]`.
fn best_ask_from_levels(levels: &Option<Vec<Vec<i32>>>) -> Option<(i32, i32)> {
    levels.as_ref().and_then(|lvls| {
        lvls.iter()
            .filter(|l| l.len() >= 2 && l[1] > 0)
            .min_by_key(|l| l[0])
            .map(|l| (l[0], l[1]))
    })
}

/// Parse an ISO 8601 / RFC 3339 close_time string into seconds remaining.
fn secs_until_close(close_time: &str) -> Option<i64> {
    let parsed = close_time
        .parse::<DateTime<Utc>>()
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(close_time, "%Y-%m-%dT%H:%M:%SZ")
                .map(|ndt| ndt.and_utc())
        })
        .ok()?;
    let remaining = (parsed - Utc::now()).num_seconds();
    Some(remaining)
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ActiveKalshiOrder {
    ticker: String,
    direction: String,
    price_cents: i64,
    contracts: i32,
    order_id: String,
    floor_strike: f64,
    placed_at: DateTime<Utc>,
    mode: String,
}

#[allow(dead_code)]
pub async fn run_kalshi_scanner(
    config: &KalshiConfig,
    price_feeds: &PriceFeeds,
    telegram: &Arc<TelegramNotifier>,
    db: &Arc<TradeDb>,
) -> Result<()> {
    if !config.enabled {
        info!("Kalshi scanner disabled");
        return Ok(());
    }

    let email =
        std::env::var("KALSHI_EMAIL").context("KALSHI_EMAIL must be set for Kalshi scanner")?;
    let password = std::env::var("KALSHI_PASSWORD")
        .context("KALSHI_PASSWORD must be set for Kalshi scanner")?;

    let is_paper = config.mode.eq_ignore_ascii_case("paper");
    let mode_str = if is_paper { "paper" } else { "live" };
    info!(
        mode = mode_str,
        series = %config.series_ticker,
        asset = %config.asset,
        bet_size = config.bet_size_usd,
        "Starting Kalshi scanner"
    );

    let _ = telegram
        .send_message(&format!(
            "🎯 Kalshi scanner started [{mode_str}] — {} / ${:.0}",
            config.series_ticker, config.bet_size_usd
        ))
        .await;

    let scan_interval = Duration::from_millis(config.scan_interval_ms);
    let binance_symbol = format!("{}usdt", config.asset.to_lowercase());

    let mut active_orders: HashMap<String, ActiveKalshiOrder> = HashMap::new();
    let mut settled_tickers: HashMap<String, DateTime<Utc>> = HashMap::new();

    loop {
        if let Err(e) = run_kalshi_session(
            config,
            price_feeds,
            telegram,
            db,
            &email,
            &password,
            env_for_mode(is_paper),
            is_paper,
            mode_str,
            scan_interval,
            &binance_symbol,
            &mut active_orders,
            &mut settled_tickers,
        )
        .await
        {
            error!(error = %e, "Kalshi session error — restarting in 10s");
            let _ = telegram
                .send_message(&format!("🚨 Kalshi session error: {e}\nRestarting in 10s…"))
                .await;
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    }
}

fn env_for_mode(is_paper: bool) -> TradingEnvironment {
    if is_paper {
        TradingEnvironment::DemoMode
    } else {
        TradingEnvironment::LiveMarketMode
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_kalshi_session(
    config: &KalshiConfig,
    price_feeds: &PriceFeeds,
    telegram: &Arc<TelegramNotifier>,
    db: &Arc<TradeDb>,
    email: &str,
    password: &str,
    env: TradingEnvironment,
    is_paper: bool,
    mode_str: &str,
    scan_interval: Duration,
    binance_symbol: &str,
    active_orders: &mut HashMap<String, ActiveKalshiOrder>,
    settled_tickers: &mut HashMap<String, DateTime<Utc>>,
) -> Result<()> {
    let mut kalshi = Kalshi::new(env);

    info!("Logging in to Kalshi…");
    kalshi
        .login(email, password)
        .await
        .context("Kalshi login failed")?;
    info!("Kalshi login successful");

    if !is_paper {
        match kalshi.get_balance().await {
            Ok(balance_cents) => {
                info!(balance_usd = balance_cents as f64 / 100.0, "Kalshi balance");
            }
            Err(e) => warn!(error = %e, "Failed to fetch Kalshi balance"),
        }
    }

    let mut last_auth = std::time::Instant::now();
    let auth_refresh = Duration::from_secs(config.auth_refresh_seconds);

    loop {
        // Refresh auth token if needed
        if last_auth.elapsed() >= auth_refresh {
            info!("Refreshing Kalshi auth token…");
            match kalshi.login(email, password).await {
                Ok(()) => {
                    last_auth = std::time::Instant::now();
                    info!("Kalshi auth refreshed");
                }
                Err(e) => {
                    error!(error = %e, "Kalshi auth refresh failed");
                    return Err(anyhow::anyhow!("Auth refresh failed: {e}"));
                }
            }
        }

        if let Err(e) = scan_cycle(
            &kalshi,
            config,
            price_feeds,
            telegram,
            db,
            is_paper,
            mode_str,
            binance_symbol,
            active_orders,
            settled_tickers,
        )
        .await
        {
            debug!(error = %e, "Kalshi scan cycle error");
        }

        tokio::time::sleep(scan_interval).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn scan_cycle(
    kalshi: &Kalshi<'_>,
    config: &KalshiConfig,
    price_feeds: &PriceFeeds,
    telegram: &Arc<TelegramNotifier>,
    db: &Arc<TradeDb>,
    is_paper: bool,
    mode_str: &str,
    binance_symbol: &str,
    active_orders: &mut HashMap<String, ActiveKalshiOrder>,
    settled_tickers: &mut HashMap<String, DateTime<Utc>>,
) -> Result<()> {
    // Fetch open markets for the series
    let (_, markets) = kalshi
        .get_multiple_markets(
            Some(100),
            None,
            None,
            Some(config.series_ticker.clone()),
            None,
            None,
            Some("open".to_string()),
            None,
        )
        .await
        .context("Failed to fetch Kalshi markets")?;

    let current_price = match price_feeds.get_price(binance_symbol).await {
        Some(p) => p,
        None => {
            debug!(symbol = binance_symbol, "No Binance price available for Kalshi");
            return Ok(());
        }
    };
    let current_f64 = decimal_to_f64(current_price);

    for market in &markets {
        if let Err(e) = evaluate_market(
            kalshi,
            config,
            market,
            current_f64,
            telegram,
            db,
            is_paper,
            mode_str,
            active_orders,
        )
        .await
        {
            debug!(
                ticker = %market.ticker,
                error = %e,
                "Error evaluating Kalshi market"
            );
        }
    }

    // Check for settlements on recently closed markets
    check_settlements(
        kalshi,
        config,
        telegram,
        db,
        mode_str,
        active_orders,
        settled_tickers,
    )
    .await;

    // Prune old settled entries (older than 2 hours)
    let cutoff = Utc::now() - chrono::Duration::hours(2);
    settled_tickers.retain(|_, ts| *ts > cutoff);

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn evaluate_market(
    kalshi: &Kalshi<'_>,
    config: &KalshiConfig,
    market: &Market,
    current_price: f64,
    telegram: &Arc<TelegramNotifier>,
    db: &Arc<TradeDb>,
    is_paper: bool,
    _mode_str: &str,
    active_orders: &mut HashMap<String, ActiveKalshiOrder>,
) -> Result<()> {
    let floor_strike = match market.floor_strike {
        Some(fs) => fs,
        None => return Ok(()),
    };

    let secs_remaining = match secs_until_close(&market.close_time) {
        Some(s) => s,
        None => {
            debug!(ticker = %market.ticker, close_time = %market.close_time, "Cannot parse close_time");
            return Ok(());
        }
    };

    // Skip if outside entry window
    if secs_remaining < config.entry_cutoff_s as i64 || secs_remaining > config.entry_start_s as i64
    {
        return Ok(());
    }

    // Skip if we already have an order on this ticker
    if active_orders.contains_key(&market.ticker) {
        return Ok(());
    }

    // Calculate delta
    let delta_pct = ((current_price - floor_strike) / floor_strike * 100.0).abs();
    let direction = if current_price >= floor_strike {
        "UP"
    } else {
        "DOWN"
    };
    let side = if direction == "UP" {
        Side::Yes
    } else {
        Side::No
    };

    // Check minimum delta
    if delta_pct < config.min_delta_pct {
        debug!(
            ticker = %market.ticker,
            delta = delta_pct,
            min = config.min_delta_pct,
            "Delta below minimum"
        );
        return Ok(());
    }

    // Determine max entry price from delta tiers
    let max_entry = config.max_price_for_delta(delta_pct);

    // Fetch orderbook
    let orderbook = kalshi
        .get_market_orderbook(&market.ticker, Some(10))
        .await
        .context("Failed to fetch orderbook")?;

    let (best_ask_cents, depth) = match &side {
        Side::Yes => best_ask_from_levels(&orderbook.yes),
        Side::No => best_ask_from_levels(&orderbook.no),
    }
    .unwrap_or((0, 0));

    if best_ask_cents == 0 {
        debug!(ticker = %market.ticker, direction, "No asks on orderbook");
        return Ok(());
    }

    let ask_price = cents_to_price(best_ask_cents as i64);

    // Check if ask is within our ceiling
    if ask_price > max_entry {
        debug!(
            ticker = %market.ticker,
            ask = ask_price,
            ceiling = max_entry,
            delta = delta_pct,
            "Ask above ceiling"
        );
        return Ok(());
    }

    // Calculate contracts from bet size
    let cost_per_contract = ask_price;
    let fee_per_contract = kalshi_fee(best_ask_cents as i64);
    let total_per_contract = cost_per_contract + fee_per_contract;
    let contracts = ((config.bet_size_usd / total_per_contract).floor() as i32).max(1);
    let total_cost = contracts as f64 * total_per_contract;

    info!(
        ticker = %market.ticker,
        direction,
        delta = format!("{delta_pct:.4}%"),
        floor_strike,
        current = current_price,
        ask = format!("${ask_price:.2}"),
        ceiling = format!("${max_entry:.2}"),
        contracts,
        total_cost = format!("${total_cost:.2}"),
        fee = format!("${fee_per_contract:.4}"),
        secs_remaining,
        depth,
        "Kalshi entry signal"
    );

    if is_paper {
        paper_trade(
            config,
            market,
            direction,
            ask_price,
            contracts,
            delta_pct,
            floor_strike,
            current_price,
            secs_remaining,
            telegram,
            db,
            active_orders,
        )
        .await;
    } else {
        live_trade(
            kalshi,
            config,
            market,
            direction,
            &side,
            best_ask_cents as i64,
            contracts,
            delta_pct,
            floor_strike,
            current_price,
            secs_remaining,
            telegram,
            db,
            active_orders,
        )
        .await;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn paper_trade(
    config: &KalshiConfig,
    market: &Market,
    direction: &str,
    ask_price: f64,
    contracts: i32,
    delta_pct: f64,
    floor_strike: f64,
    current_price: f64,
    secs_remaining: i64,
    telegram: &Arc<TelegramNotifier>,
    _db: &Arc<TradeDb>,
    active_orders: &mut HashMap<String, ActiveKalshiOrder>,
) {
    let order_id = format!("paper-{}-{}", market.ticker, Utc::now().timestamp());
    let total_cost = contracts as f64 * ask_price;

    info!(
        ticker = %market.ticker,
        direction,
        price = format!("${ask_price:.2}"),
        contracts,
        total = format!("${total_cost:.2}"),
        "[PAPER] Would place Kalshi order"
    );

    active_orders.insert(
        market.ticker.clone(),
        ActiveKalshiOrder {
            ticker: market.ticker.clone(),
            direction: direction.to_string(),
            price_cents: price_to_cents(ask_price),
            contracts,
            order_id: order_id.clone(),
            floor_strike,
            placed_at: Utc::now(),
            mode: "paper".to_string(),
        },
    );

    let _ = telegram
        .send_message(&format!(
            "📝 [PAPER] Kalshi order — {}\n\
             \u{200b}  {direction} @ ${ask_price:.2} × {contracts} = ${total_cost:.2}\n\
             \u{200b}  Δ {delta_pct:.4}% | strike {floor_strike:.0} | spot {current_price:.0}\n\
             \u{200b}  T-{secs_remaining}s | series {}",
            market.ticker, config.series_ticker,
        ))
        .await;
}

#[allow(clippy::too_many_arguments)]
async fn live_trade(
    kalshi: &Kalshi<'_>,
    config: &KalshiConfig,
    market: &Market,
    direction: &str,
    side: &Side,
    ask_cents: i64,
    contracts: i32,
    delta_pct: f64,
    floor_strike: f64,
    current_price: f64,
    secs_remaining: i64,
    telegram: &Arc<TelegramNotifier>,
    _db: &Arc<TradeDb>,
    active_orders: &mut HashMap<String, ActiveKalshiOrder>,
) {
    let (yes_price, no_price) = match side {
        Side::Yes => (Some(ask_cents), None),
        Side::No => (None, Some(ask_cents)),
    };

    match kalshi
        .create_order(
            Action::Buy,
            None,
            contracts,
            match side {
                Side::Yes => Side::Yes,
                Side::No => Side::No,
            },
            market.ticker.clone(),
            OrderType::Limit,
            None,
            None,
            no_price,
            None,
            yes_price,
        )
        .await
    {
        Ok(order) => {
            let ask_price = cents_to_price(ask_cents);
            let total_cost = contracts as f64 * ask_price;

            info!(
                order_id = %order.order_id,
                ticker = %market.ticker,
                direction,
                price = format!("${ask_price:.2}"),
                contracts,
                status = %order.status,
                "Kalshi order placed"
            );

            active_orders.insert(
                market.ticker.clone(),
                ActiveKalshiOrder {
                    ticker: market.ticker.clone(),
                    direction: direction.to_string(),
                    price_cents: ask_cents,
                    contracts,
                    order_id: order.order_id.clone(),
                    floor_strike,
                    placed_at: Utc::now(),
                    mode: "live".to_string(),
                },
            );

            let _ = telegram
                .send_message(&format!(
                    "📤 Kalshi order placed — {}\n\
                     \u{200b}  {direction} @ ${ask_price:.2} × {contracts} = ${total_cost:.2}\n\
                     \u{200b}  Δ {delta_pct:.4}% | strike {floor_strike:.0} | spot {current_price:.0}\n\
                     \u{200b}  T-{secs_remaining}s | order {}\n\
                     \u{200b}  Series {}",
                    market.ticker, order.order_id, config.series_ticker,
                ))
                .await;
        }
        Err(e) => {
            error!(
                ticker = %market.ticker,
                error = %e,
                "Failed to place Kalshi order"
            );
            let _ = telegram
                .send_message(&format!(
                    "❌ Kalshi order FAILED — {}\n\
                     \u{200b}  {direction} | Δ {delta_pct:.4}%\n\
                     \u{200b}  Error: {e}",
                    market.ticker,
                ))
                .await;
        }
    }
}

async fn check_settlements(
    kalshi: &Kalshi<'_>,
    config: &KalshiConfig,
    telegram: &Arc<TelegramNotifier>,
    _db: &Arc<TradeDb>,
    mode_str: &str,
    active_orders: &mut HashMap<String, ActiveKalshiOrder>,
    settled_tickers: &mut HashMap<String, DateTime<Utc>>,
) {
    let tickers_to_check: Vec<String> = active_orders.keys().cloned().collect();
    if tickers_to_check.is_empty() {
        return;
    }

    for ticker in &tickers_to_check {
        if settled_tickers.contains_key(ticker) {
            continue;
        }

        let market = match kalshi.get_single_market(&ticker).await {
            Ok(m) => m,
            Err(e) => {
                debug!(ticker = %ticker, error = %e, "Failed to fetch market for settlement check");
                continue;
            }
        };

        // Only process settled/closed markets
        if market.status != "settled" && market.status != "closed" {
            // Check if market has passed close time
            if let Some(secs) = secs_until_close(&market.close_time) {
                if secs > -60 {
                    continue;
                }
            } else {
                continue;
            }
        }

        let order_info = match active_orders.get(ticker) {
            Some(o) => o.clone(),
            None => continue,
        };

        let (result_str, won) = match &market.result {
            SettlementResult::Yes => {
                let w = order_info.direction == "UP";
                ("YES", w)
            }
            SettlementResult::No => {
                let w = order_info.direction == "DOWN";
                ("NO", w)
            }
            SettlementResult::AllNo => {
                let w = order_info.direction == "DOWN";
                ("ALL_NO", w)
            }
            SettlementResult::AllYes => {
                let w = order_info.direction == "UP";
                ("ALL_YES", w)
            }
            SettlementResult::Void => ("VOID", false),
        };

        let entry_price = cents_to_price(order_info.price_cents);
        let pnl = if won {
            (1.0 - entry_price) * order_info.contracts as f64
                - kalshi_fee(order_info.price_cents) * order_info.contracts as f64
        } else {
            -(entry_price * order_info.contracts as f64)
                - kalshi_fee(order_info.price_cents) * order_info.contracts as f64
        };

        let emoji = if won { "✅" } else { "❌" };
        let outcome = if won { "WIN" } else { "LOSS" };
        let mode_tag = if mode_str == "paper" {
            "[PAPER] "
        } else {
            ""
        };

        info!(
            ticker = %ticker,
            result = result_str,
            direction = %order_info.direction,
            outcome,
            pnl = format!("${pnl:.2}"),
            "Kalshi settlement"
        );

        let _ = telegram
            .send_message(&format!(
                "{emoji} {mode_tag}Kalshi {outcome} — {ticker}\n\
                 \u{200b}  Direction: {dir} | Result: {result_str}\n\
                 \u{200b}  Entry: ${entry_price:.2} × {contracts}\n\
                 \u{200b}  P&L: ${pnl:.2}\n\
                 \u{200b}  Series: {series}",
                dir = order_info.direction,
                contracts = order_info.contracts,
                series = config.series_ticker,
            ))
            .await;

        settled_tickers.insert(ticker.clone(), Utc::now());
        active_orders.remove(ticker);
    }
}

fn decimal_to_f64(d: Decimal) -> f64 {
    use std::str::FromStr;
    f64::from_str(&d.to_string()).unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kalshi_fee() {
        assert_eq!(kalshi_fee(50), 0.02); // 7% * 0.5 * 0.5 * 100 = 1.75 → ceil = 2 → $0.02
        assert_eq!(kalshi_fee(90), 0.01); // 7% * 0.9 * 0.1 * 100 = 0.63 → ceil = 1 → $0.01
        assert_eq!(kalshi_fee(10), 0.01); // 7% * 0.1 * 0.9 * 100 = 0.63 → ceil = 1 → $0.01
    }

    #[test]
    fn test_price_conversions() {
        assert_eq!(price_to_cents(0.45), 45);
        assert_eq!(price_to_cents(0.90), 90);
        assert_eq!(cents_to_price(45), 0.45);
        assert_eq!(cents_to_price(90), 0.90);
    }

    #[test]
    fn test_best_ask() {
        let levels = Some(vec![vec![45, 100], vec![42, 50], vec![50, 200]]);
        let (price, qty) = best_ask_from_levels(&levels).unwrap();
        assert_eq!(price, 42);
        assert_eq!(qty, 50);

        let empty: Option<Vec<Vec<i32>>> = None;
        assert!(best_ask_from_levels(&empty).is_none());
    }
}
