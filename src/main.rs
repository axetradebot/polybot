mod cli;
mod config;
mod db;
mod feeds;
mod market;
mod orders;
mod redeem;
mod risk;
mod signal;
mod state;
mod telegram;
mod types;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rust_decimal::Decimal;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::cli::Cli;
use crate::config::AppConfig;
use crate::db::TradeDb;
use crate::risk::RiskManager;
use crate::signal::{SignalEngine, SweepPlan, TierDecision};
use crate::state::{new_shared_state, SharedState};
use crate::telegram::TelegramNotifier;
use crate::types::{BotMode, Direction, MarketInfo, OrderResult, TradeOutcome, TradeRecord};

/// Everything needed to run post-window settlement for a filled trade.
struct PendingSettle {
    slug: String,
    order_id: String,
    direction: Direction,
    fill_price: Decimal,
    fill_size: Decimal,
    tier_name: String,
    btc_open_price: Decimal,
    condition_id: String,
    neg_risk: bool,
}

/// Returned by paper tier / single-entry loops when an order fills.
struct FillInfo {
    order_id: String,
    direction: Direction,
    fill_price: Decimal,
    fill_size: Decimal,
    tier_name: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Prevent SDK background thread panics from killing the process
    std::panic::set_hook(Box::new(|info| {
        eprintln!("Panic caught (non-fatal): {info}");
    }));

    let cli = Cli::parse();
    let mut config = AppConfig::load(&cli.config).context("Failed to load config")?;
    config.apply_cli_overrides(&cli);

    let log_file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open("bot.log")
        .expect("Failed to open log file");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| {
                    // Suppress noisy "unknown field" warnings from SDK deserialization.
                    format!("{},polymarket_client_sdk=error", config.logging.level).into()
                }),
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_ansi(false)
        .with_writer(std::sync::Mutex::new(log_file))
        .init();

    info!(
        mode = %config.mode,
        bankroll = %config.trading.starting_bankroll,
        max_bet = %config.trading.max_bet_usd,
        tiers_enabled = config.signal.entry_tiers.enabled,
        "Polymarket BTC Maker Bot starting"
    );

    let state = new_shared_state(config.starting_bankroll_decimal());

    let db = Arc::new(
        TradeDb::open(config.db_path()).context("Failed to open trade database")?,
    );

    let telegram = Arc::new(TelegramNotifier::new(
        config.telegram_bot_token.clone(),
        config.telegram_chat_id.clone(),
        config.telegram_enabled(),
        config.mode,
    ));

    telegram
        .send_startup(config.starting_bankroll_decimal())
        .await
        .ok();

    let signal_engine = SignalEngine::new(&config);
    let risk_manager = RiskManager::new(&config);

    // Spawn Binance price feed
    let binance_state = state.clone();
    let binance_url = config.infra.binance_ws_url.clone();
    tokio::spawn(async move {
        if let Err(e) = feeds::binance::run_binance_feed(binance_state, &binance_url).await {
            error!(error = %e, "Binance feed fatal error");
        }
    });

    // Wait for initial BTC price
    info!("Waiting for initial BTC price from Binance...");
    loop {
        if state.read().await.has_fresh_price() {
            let price = state.read().await.btc_price;
            info!(price = %price, "Initial BTC price received");
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    match config.mode {
        BotMode::Paper => {
            info!("[PAPER] Running in paper trading mode");
            run_paper_loop(state, &config, &signal_engine, &risk_manager, &db, &telegram).await
        }
        BotMode::Live => {
            info!("Initializing live CLOB connection...");
            let result =
                run_live_mode(state, &config, &signal_engine, &risk_manager, &db, &telegram)
                    .await;
            if let Err(ref e) = result {
                let msg = format!("LIVE MODE CRASHED: {e}");
                error!("{}", msg);
                telegram.send_error(&msg).await.ok();
            }
            result
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Paper trading loop
// ─────────────────────────────────────────────────────────────────────────────

async fn run_paper_loop(
    state: SharedState,
    config: &AppConfig,
    signal_engine: &SignalEngine,
    risk_manager: &RiskManager,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) -> Result<()> {
    let watch_start = config.watch_start_s();
    let mode = config.mode;
    let mut last_window_ts: u64 = 0;
    let mut last_pending_settle: Option<PendingSettle> = None;

    loop {
        state.write().await.reset_daily_if_needed();

        let window = market::current_window_info();
        let secs_left = window.seconds_remaining;

        if window.window_ts != last_window_ts {
            let btc_now = state.read().await.btc_price;
            {
                let mut st = state.write().await;
                st.current_window_ts = window.window_ts;
                st.window_open_price = btc_now;
                st.window_open_captured = true;
                st.current_market = None;
            }

            info!(
                window_ts = window.window_ts,
                slug = %window.slug,
                open_price = %btc_now,
                "[PAPER] New window started"
            );

            if let Some(ps) = last_pending_settle.take() {
                let db2 = db.clone();
                let tg2 = telegram.clone();
                let st2 = state.clone();
                let rpc = config.infra.polygon_rpc_url.clone();
                let pk = config.private_key.clone();
                let do_redeem = config.infra.auto_redeem;
                tokio::spawn(async move {
                    settle_trade(ps, mode, db2, tg2, st2, &rpc, &pk, do_redeem).await.ok();
                });
            }

            last_window_ts = window.window_ts;
        }

        if secs_left > watch_start + 1 {
            let sleep_secs = secs_left - watch_start - 1;
            info!(sleep_secs, "[PAPER] Waiting for entry window");
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
            continue;
        }

        if secs_left < 2 {
            tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
            continue;
        }

        let market_info = match resolve_market_for_window(&state, &window.slug, secs_left).await {
            Some(m) => m,
            None => {
                tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
                continue;
            }
        };

        {
            let st = state.read().await;
            if let Err(veto) = risk_manager.check(&st) {
                warn!(reason = %veto, "[PAPER] Risk vetoed");
                tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
                continue;
            }
        }

        let btc_open_price = state.read().await.window_open_price;
        let max_bet = risk_manager.position_size(&*state.read().await);
        let sweep_cfg = &config.signal.sweep;

        let plan = {
            let st = state.read().await;
            signal_engine.evaluate_sweep(&st, &market_info, sweep_cfg, max_bet)
        };

        let fill_info = match plan {
            None => {
                info!("[PAPER] Sweep skipped — delta too low");
                None
            }
            Some(plan) => {
                run_paper_sweep(&plan, max_bet, config, db, telegram).await
            }
        };

        if let Some(fi) = fill_info {
            last_pending_settle = Some(PendingSettle {
                slug: window.slug.clone(),
                order_id: fi.order_id,
                direction: fi.direction,
                fill_price: fi.fill_price,
                fill_size: fi.fill_size,
                tier_name: fi.tier_name,
                btc_open_price,
                condition_id: market_info.condition_id.clone(),
                neg_risk: market_info.neg_risk,
            });
        }

        let remaining = market::seconds_until_close();
        tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
    }
}

/// Paper sweep: simulate the price sweep and pick the best fill.
async fn run_paper_sweep(
    plan: &SweepPlan,
    max_bet: Decimal,
    config: &AppConfig,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) -> Option<FillInfo> {
    // In paper mode, simulate by picking the most aggressive price
    // (last step) and assuming it fills, since that mirrors real behavior.
    let best_price = *plan.prices.last()?;
    let decision = sweep_to_decision(plan, best_price);

    match orders::place_paper_order(&decision, max_bet).await {
        Ok(result) if result.filled => {
            let secs = market::seconds_until_close();
            record_paper_fill(&result, &decision, secs, config, db, telegram).await;
            Some(FillInfo {
                order_id: result.order_id,
                direction: plan.direction,
                fill_price: result.fill_price,
                fill_size: result.fill_size,
                tier_name: "sweep".to_string(),
            })
        }
        Ok(result) => {
            let secs = market::seconds_until_close();
            log_unfilled_paper_order(&result.order_id, &decision, secs, config, db, telegram).await;
            None
        }
        Err(e) => {
            error!(error = %e, "[PAPER] Sweep order error");
            None
        }
    }
}

async fn log_unfilled_paper_order(
    order_id: &str,
    decision: &TierDecision,
    secs_at_entry: u64,
    config: &AppConfig,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) {
    let fake_result = OrderResult {
        order_id: order_id.to_string(),
        filled: false,
        fill_price: Decimal::ZERO,
        fill_size: Decimal::ZERO,
    };
    let trade = build_trade_record(&fake_result, decision, secs_at_entry, config, false);
    if let Err(e) = db.insert_trade(&trade) {
        error!(error = %e, "Failed to log unfilled paper order");
    }
    info!(
        tier = %decision.tier_name,
        "[PAPER] Order cancelled/expired (not filled)"
    );
    let _ = telegram; // could send a notification here if desired
}

// ─────────────────────────────────────────────────────────────────────────────
// Live trading loop
// ─────────────────────────────────────────────────────────────────────────────

async fn run_live_mode(
    state: SharedState,
    config: &AppConfig,
    signal_engine: &SignalEngine,
    risk_manager: &RiskManager,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) -> Result<()> {
    use polymarket_client_sdk::auth::{LocalSigner, Signer as _};
    use polymarket_client_sdk::clob::types::{Side, SignatureType};
    use polymarket_client_sdk::clob::{Client, Config};
    use polymarket_client_sdk::types::U256;
    use polymarket_client_sdk::POLYGON;

    let signer = match LocalSigner::from_str(&config.private_key) {
        Ok(s) => s.with_chain_id(Some(POLYGON)),
        Err(e) => {
            let msg = format!("Invalid private key: {e}");
            error!("{}", msg);
            telegram.send_error(&msg).await.ok();
            anyhow::bail!(msg);
        }
    };

    info!("Signer created, authenticating with CLOB...");

    let mut builder = Client::new(&config.infra.polymarket_clob_url, Config::default())?
        .authentication_builder(&signer);

    match config.signature_type()? {
        crate::types::SignatureType::GnosisSafe => {
            builder = builder.signature_type(SignatureType::GnosisSafe);
        }
        crate::types::SignatureType::Proxy => {
            builder = builder.signature_type(SignatureType::Proxy);
        }
        crate::types::SignatureType::Eoa => {}
    }

    let client = match builder.authenticate().await {
        Ok(c) => c,
        Err(e) => {
            let msg = format!("CLOB authentication FAILED: {e}");
            error!("{}", msg);
            telegram.send_error(&msg).await.ok();
            anyhow::bail!(msg);
        }
    };

    info!("CLOB client authenticated, entering live trading loop");

    let watch_start = config.watch_start_s();
    let mode = config.mode;
    let mut last_window_ts: u64 = 0;
    let mut last_pending_settle: Option<PendingSettle> = None;

    loop {
        state.write().await.reset_daily_if_needed();

        let window = market::current_window_info();
        let secs_left = window.seconds_remaining;

        if window.window_ts != last_window_ts {
            let btc_now = state.read().await.btc_price;
            {
                let mut st = state.write().await;
                st.current_window_ts = window.window_ts;
                st.window_open_price = btc_now;
                st.window_open_captured = true;
                st.current_market = None;
            }

            info!(
                window_ts = window.window_ts,
                slug = %window.slug,
                open_price = %btc_now,
                "New window started"
            );

            if let Some(ps) = last_pending_settle.take() {
                let db2 = db.clone();
                let tg2 = telegram.clone();
                let st2 = state.clone();
                let rpc = config.infra.polygon_rpc_url.clone();
                let pk = config.private_key.clone();
                let do_redeem = config.infra.auto_redeem;
                tokio::spawn(async move {
                    settle_trade(ps, mode, db2, tg2, st2, &rpc, &pk, do_redeem).await.ok();
                });
            }

            last_window_ts = window.window_ts;
        }

        if secs_left > watch_start + 1 {
            let sleep_secs = secs_left - watch_start - 1;
            info!(sleep_secs, window = %window.slug, "Waiting for entry window");
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
            continue;
        }

        if secs_left < 2 {
            tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
            continue;
        }

        let market_info = match resolve_market_for_window(&state, &window.slug, secs_left).await {
            Some(m) => m,
            None => {
                tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
                continue;
            }
        };

        {
            let st = state.read().await;
            if let Err(veto) = risk_manager.check(&st) {
                warn!(reason = %veto, "Risk vetoed");
                tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
                continue;
            }
        }

        // ── Sweep loop ──
        let max_bet = risk_manager.position_size(&*state.read().await);
        let sweep_cfg = &config.signal.sweep;

        let plan = {
            let st = state.read().await;
            signal_engine.evaluate_sweep(&st, &market_info, sweep_cfg, max_bet)
        };

        let plan = match plan {
            Some(p) => {
                let msg = format!(
                    "Sweep starting: {} delta={}% prices={:?}",
                    p.direction, p.delta_pct, p.prices
                );
                info!("{}", msg);
                telegram.send_error(&msg).await.ok();
                p
            }
            None => {
                let st = state.read().await;
                let delta = if st.window_open_price > Decimal::ZERO {
                    ((st.btc_price - st.window_open_price).abs() / st.window_open_price * Decimal::from(100)).to_string()
                } else {
                    "N/A".to_string()
                };
                let msg = format!("Sweep skipped — delta {}% (need {}%)", delta, sweep_cfg.min_delta_pct);
                info!("{}", msg);
                telegram.send_error(&msg).await.ok();
                let remaining = market::seconds_until_close();
                tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
                continue;
            }
        };

        let token_id_u256 = match U256::from_str(&plan.token_id) {
            Ok(id) => id,
            Err(e) => {
                let msg = format!("Invalid token ID: {e}");
                error!("{}", msg);
                telegram.send_error(&msg).await.ok();
                let remaining = market::seconds_until_close();
                tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
                continue;
            }
        };

        let num_steps = plan.prices.len();
        let sweep_duration_s = sweep_cfg.start_s.saturating_sub(1);
        let step_interval_ms = if num_steps > 1 {
            ((sweep_duration_s as f64 / (num_steps - 1) as f64) * 1000.0) as u64
        } else {
            1000
        };

        let mut active_order_id: Option<String> = None;
        let mut last_price = Decimal::ZERO;
        let mut filled = false;

        'sweep: for (step_idx, &price) in plan.prices.iter().enumerate() {
            let secs = market::seconds_until_close();
            if secs < 1 {
                break 'sweep;
            }

            let contracts = (plan.max_bet_usd / price)
                .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero);

            info!(
                step = step_idx + 1,
                of = num_steps,
                price = %price,
                contracts = %contracts,
                secs_left = secs,
                "Sweep step"
            );

            // Cancel previous order if any
            if active_order_id.is_some() {
                if let Err(e) = client.cancel_all_orders().await {
                    warn!(error = %e, "Cancel failed during sweep");
                }
                active_order_id = None;
            }

            // Build -> Sign -> Post
            let order = match client
                .limit_order()
                .token_id(token_id_u256)
                .size(contracts)
                .price(price)
                .side(Side::Buy)
                .build()
                .await
            {
                Ok(o) => o,
                Err(e) => {
                    warn!(step = step_idx, error = ?e, "Build order failed");
                    continue;
                }
            };

            let signed = match client.sign(&signer, order).await {
                Ok(s) => s,
                Err(e) => {
                    warn!(step = step_idx, error = ?e, "Sign order failed");
                    continue;
                }
            };

            match client.post_order(signed).await {
                Ok(resp) => {
                    let oid = resp.order_id.clone();
                    info!(step = step_idx, order_id = %oid, price = %price, "Order posted");
                    active_order_id = Some(oid);
                    last_price = price;
                }
                Err(e) => {
                    warn!(step = step_idx, price = %price, error = ?e, "Post order rejected");
                    continue;
                }
            }

            // Wait for step interval, polling for fill
            let poll_deadline = tokio::time::Instant::now() + Duration::from_millis(step_interval_ms);
            while tokio::time::Instant::now() < poll_deadline {
                tokio::time::sleep(Duration::from_millis(300)).await;

                if let Some(oid) = &active_order_id {
                    let oid_clone = oid.clone();
                    if let Ok(Ok(page)) = tokio::time::timeout(
                        Duration::from_secs(3),
                        client.orders(&Default::default(), None),
                    ).await {
                        for o in &page.data {
                            if o.id == oid_clone && o.size_matched > Decimal::ZERO {
                                info!(order_id = %oid_clone, matched = %o.size_matched, price = %o.price, "FILLED during sweep!");
                                telegram.send_error(&format!(
                                    "FILLED! step={}/{} price=${} matched={}",
                                    step_idx + 1, num_steps, o.price, o.size_matched
                                )).await.ok();

                                let decision = sweep_to_decision(&plan, o.price);
                                let btc_open = state.read().await.window_open_price;
                                let result = OrderResult {
                                    order_id: oid_clone.clone(),
                                    filled: true,
                                    fill_price: o.price,
                                    fill_size: o.size_matched,
                                };
                                record_live_order(&result, &decision, secs, config, db, telegram).await;
                                last_pending_settle = Some(PendingSettle {
                                    slug: window.slug.clone(),
                                    order_id: oid_clone,
                                    direction: plan.direction,
                                    fill_price: o.price,
                                    fill_size: o.size_matched,
                                    tier_name: format!("sweep_{}", step_idx + 1),
                                    btc_open_price: btc_open,
                                    condition_id: market_info.condition_id.clone(),
                                    neg_risk: market_info.neg_risk,
                                });
                                filled = true;
                                break 'sweep;
                            }
                        }
                    }
                }
            }
        }

        // After sweep completes, if there's still an active order, wait for window close
        if !filled {
            if let Some(oid) = &active_order_id {
                let close_ts = window.close_ts();
                let now = chrono::Utc::now().timestamp() as u64;
                if now < close_ts {
                    let wait = close_ts.saturating_sub(now) + 1;
                    info!(wait_secs = wait, "Sweep done, waiting for window close with last order");
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                }

                // Final fill check
                let oid_clone = oid.clone();
                info!(order_id = %oid_clone, "Final fill check at window close");
                if let Ok(Ok(page)) = tokio::time::timeout(
                    Duration::from_secs(5),
                    client.orders(&Default::default(), None),
                ).await {
                    for o in &page.data {
                        if o.id == oid_clone && o.size_matched > Decimal::ZERO {
                            info!(order_id = %oid_clone, matched = %o.size_matched, "Filled at window close!");
                            telegram.send_error(&format!(
                                "FILLED at close! price=${} matched={}",
                                o.price, o.size_matched
                            )).await.ok();

                            let decision = sweep_to_decision(&plan, o.price);
                            let btc_open = state.read().await.window_open_price;
                            let result = OrderResult {
                                order_id: oid_clone.clone(),
                                filled: true,
                                fill_price: o.price,
                                fill_size: o.size_matched,
                            };
                            record_live_order(&result, &decision, 0, config, db, telegram).await;
                            last_pending_settle = Some(PendingSettle {
                                slug: window.slug.clone(),
                                order_id: oid_clone.clone(),
                                direction: plan.direction,
                                fill_price: o.price,
                                fill_size: o.size_matched,
                                tier_name: "sweep_final".to_string(),
                                btc_open_price: btc_open,
                                condition_id: market_info.condition_id.clone(),
                                neg_risk: market_info.neg_risk,
                            });
                            filled = true;
                            break;
                        }
                    }
                }

                if !filled {
                    info!(order_id = %oid_clone, "Sweep order expired unfilled");
                    telegram.send_error(&format!("Sweep expired unfilled (last price ${})", last_price)).await.ok();
                    if let Err(e) = client.cancel_all_orders().await {
                        warn!(error = %e, "Cancel failed at window close");
                    }
                    let decision = sweep_to_decision(&plan, last_price);
                    let fake = OrderResult {
                        order_id: oid_clone,
                        filled: false,
                        fill_price: Decimal::ZERO,
                        fill_size: Decimal::ZERO,
                    };
                    record_live_order(&fake, &decision, 0, config, db, telegram).await;
                }
            } else {
                info!("Sweep: no orders placed this window");
                telegram.send_error("Sweep: no orders placed this window").await.ok();
            }
        }

        // Wait for next window
        let remaining = market::seconds_until_close();
        tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared helpers
// ─────────────────────────────────────────────────────────────────────────────

async fn resolve_market_for_window(
    state: &SharedState,
    slug: &str,
    secs_left: u64,
) -> Option<MarketInfo> {
    if let Some(m) = state.read().await.current_market.clone() {
        return Some(m);
    }

    match market::resolve_market_with_retry(slug, 3).await {
        Ok(m) if m.accepting_orders => {
            state.write().await.current_market = Some(m.clone());
            Some(m)
        }
        Ok(_) => {
            warn!("Market not accepting orders");
            None
        }
        Err(e) => {
            warn!(error = %e, secs_left, "Failed to resolve market");
            None
        }
    }
}

fn sweep_to_decision(plan: &SweepPlan, price: Decimal) -> TierDecision {
    let contracts = (plan.max_bet_usd / price)
        .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero);
    TierDecision {
        tier_name: "sweep".to_string(),
        time_before_close_s: 0,
        direction: plan.direction,
        target_price: price,
        delta_pct: plan.delta_pct,
        token_id: plan.token_id.clone(),
        contracts,
        bet_size_usd: contracts * price,
    }
}

fn build_trade_record(
    result: &OrderResult,
    decision: &TierDecision,
    secs_at_entry: u64,
    config: &AppConfig,
    filled: bool,
) -> TradeRecord {
    let size = result.fill_size;
    let price = if filled { result.fill_price } else { decision.target_price };
    TradeRecord {
        id: None,
        timestamp: Utc::now(),
        window_ts: 0, // caller can patch if needed — set below
        market_slug: String::new(),
        direction: decision.direction.to_string(),
        token_id: decision.token_id.clone(),
        order_id: result.order_id.clone(),
        tier_name: decision.tier_name.clone(),
        entry_price: price,
        size,
        cost_usd: size * price,
        outcome: TradeOutcome::Pending.to_string(),
        pnl: Decimal::ZERO,
        btc_open_price: Decimal::ZERO, // set by caller
        btc_close_price: Decimal::ZERO,
        delta_pct: decision.delta_pct,
        seconds_at_entry: Decimal::from(secs_at_entry),
        mode: config.mode.to_string(),
        filled,
    }
}

async fn record_paper_fill(
    result: &OrderResult,
    decision: &TierDecision,
    secs_at_entry: u64,
    config: &AppConfig,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) {
    // We need the BTC open price — not easily available here; use zero as placeholder.
    // TODO: thread open price into this helper if needed for precise reporting.
    let mut trade = build_trade_record(result, decision, secs_at_entry, config, true);
    trade.market_slug = format!("btc-updown-5m-window");
    trade.btc_open_price = Decimal::ZERO;

    if let Err(e) = db.insert_trade(&trade) {
        error!(error = %e, "Failed to log paper fill");
    }
    telegram.send_fill(&trade).await.ok();
    info!(
        tier = %decision.tier_name,
        direction = %decision.direction,
        price = %decision.target_price,
        "[PAPER] FILLED"
    );
}

async fn record_live_order(
    result: &OrderResult,
    decision: &TierDecision,
    secs_at_entry: u64,
    config: &AppConfig,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) {
    let mut trade = build_trade_record(result, decision, secs_at_entry, config, result.filled);
    trade.market_slug = format!("btc-updown-5m-live");
    if result.filled {
        telegram.send_fill(&trade).await.ok();
    }
    if let Err(e) = db.insert_trade(&trade) {
        error!(error = %e, "Failed to log live order");
    }
}

async fn settle_trade(
    ps: PendingSettle,
    mode: BotMode,
    db: Arc<TradeDb>,
    telegram: Arc<TelegramNotifier>,
    state: SharedState,
    polygon_rpc_url: &str,
    private_key: &str,
    auto_redeem: bool,
) -> Result<()> {
    // Wait for the window to fully close and BTC to stabilise.
    tokio::time::sleep(Duration::from_secs(25)).await;

    let btc_close = state.read().await.btc_price;
    let our_bet_is_up = ps.direction == Direction::Up;

    // Determine outcome: paper mode uses live BTC prices; live mode uses Gamma API
    // with a long retry window and BTC-price fallback.
    let (won, outcome_source) = match mode {
        BotMode::Paper => {
            let up_won = btc_close > ps.btc_open_price;
            let won = redeem::did_we_win(our_bet_is_up, up_won);
            (won, "btc-price")
        }
        BotMode::Live => {
            let mut resolved: Option<bool> = None;
            // Retry up to 20 times (every 30 s = up to 10 minutes after close)
            for attempt in 0..20 {
                match redeem::check_settlement(&ps.slug).await {
                    Ok(Some(up_won)) => {
                        resolved = Some(redeem::did_we_win(our_bet_is_up, up_won));
                        break;
                    }
                    Ok(None) => {
                        info!(attempt, slug = %ps.slug, "Settlement pending, retrying...");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, attempt, "Settlement check error");
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }
                }
            }
            // Fall back to BTC price if API never resolved
            let won = resolved.unwrap_or_else(|| {
                warn!(slug = %ps.slug, "Using BTC price fallback for settlement");
                redeem::did_we_win(our_bet_is_up, btc_close > ps.btc_open_price)
            });
            (won, if resolved.is_some() { "gamma-api" } else { "btc-fallback" })
        }
    };

    // P&L based on actual fill price and size.
    let pnl = if won {
        ps.fill_size * (Decimal::ONE - ps.fill_price)  // profit per contract
    } else {
        -(ps.fill_size * ps.fill_price)                 // total cost = loss
    };

    let outcome = if won { TradeOutcome::Win } else { TradeOutcome::Loss };

    // Persist to DB
    db.update_outcome(&ps.order_id, &outcome.to_string(), pnl, btc_close)?;

    // Update in-memory state
    let bankroll = {
        let mut st = state.write().await;
        if won {
            st.record_win(pnl);
        } else {
            st.record_loss(pnl);
        }
        st.bankroll
    };

    info!(
        slug = %ps.slug,
        tier = %ps.tier_name,
        direction = %ps.direction,
        btc_open = %ps.btc_open_price,
        btc_close = %btc_close,
        outcome = %outcome,
        pnl = %pnl,
        bankroll = %bankroll,
        source = outcome_source,
        "Trade settled"
    );

    telegram.send_settlement(
        &ps.slug,
        &ps.tier_name,
        ps.direction,
        ps.fill_price,
        ps.fill_size,
        ps.btc_open_price,
        btc_close,
        won,
        pnl,
        bankroll,
    ).await.ok();

    // Auto-redeem winning positions on-chain
    if won && auto_redeem && mode == BotMode::Live && !ps.condition_id.is_empty() {
        info!(slug = %ps.slug, "Attempting auto-redeem of winning position...");
        match redeem::auto_redeem(
            polygon_rpc_url,
            private_key,
            &ps.condition_id,
            ps.neg_risk,
        ).await {
            Ok(tx_hash) => {
                let msg = format!("Auto-redeemed! tx: {}", tx_hash);
                info!("{}", msg);
                telegram.send_error(&msg).await.ok();
            }
            Err(e) => {
                let msg = format!("Auto-redeem failed: {e:#}");
                warn!("{}", msg);
                telegram.send_error(&msg).await.ok();
            }
        }
    }

    Ok(())
}
