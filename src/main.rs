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
use crate::config::{AppConfig, EntryTier};
use crate::db::TradeDb;
use crate::risk::RiskManager;
use crate::signal::{SignalEngine, TierDecision};
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
    let use_tiers = config.signal.entry_tiers.enabled;
    let mode = config.mode;
    let mut last_window_ts: u64 = 0;
    let mut last_pending_settle: Option<PendingSettle> = None;

    loop {
        state.write().await.reset_daily_if_needed();

        let window = market::current_window_info();
        let secs_left = window.seconds_remaining;

        // ── New window: set open price, spawn settlement for previous ──
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
                tokio::spawn(async move {
                    settle_trade(ps, mode, db2, tg2, st2).await.ok();
                });
            }

            last_window_ts = window.window_ts;
        }

        // ── Sleep until watch window begins ──
        if secs_left > watch_start + 1 {
            let sleep_secs = secs_left - watch_start - 1;
            info!(sleep_secs, "[PAPER] Waiting for entry window");
            tokio::time::sleep(Duration::from_secs(sleep_secs)).await;
            continue;
        }

        // Skip if window basically over
        if secs_left < 2 {
            tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
            continue;
        }

        // ── Resolve market ──
        let market_info = match resolve_market_for_window(&state, &window.slug, secs_left).await {
            Some(m) => m,
            None => {
                tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
                continue;
            }
        };

        // ── Risk check ──
        {
            let st = state.read().await;
            if let Err(veto) = risk_manager.check(&st) {
                warn!(reason = %veto, "[PAPER] Risk vetoed");
                tokio::time::sleep(Duration::from_secs(secs_left + 1)).await;
                continue;
            }
        }

        // Capture open price now, before the tier loop might straddle a window boundary.
        let btc_open_price = state.read().await.window_open_price;

        // ── Tier or single-entry loop ──
        let fill_info = if use_tiers {
            run_paper_tier_loop(
                &state,
                config,
                signal_engine,
                risk_manager,
                &market_info,
                db,
                telegram,
            )
            .await
        } else {
            run_paper_single_entry(
                &state,
                config,
                signal_engine,
                risk_manager,
                &market_info,
                db,
                telegram,
                secs_left,
            )
            .await
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
            });
        }

        // ── Sleep until next window ──
        let remaining = market::seconds_until_close();
        tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
    }
}

/// Tier loop for paper trading. Returns fill info if an order was filled.
async fn run_paper_tier_loop(
    state: &SharedState,
    config: &AppConfig,
    signal_engine: &SignalEngine,
    risk_manager: &RiskManager,
    market_info: &MarketInfo,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
) -> Option<FillInfo> {
    let tiers: Vec<EntryTier> = config.signal.entry_tiers.tiers.clone();
    let max_bet = risk_manager.position_size(&*state.read().await);

    let mut next_tier_idx = 0usize;
    let mut last_order: Option<(String, TierDecision)> = None; // (order_id, decision)

    loop {
        let secs = market::seconds_until_close();

        if secs < 1 {
            // Window closing — last order didn't fill
            if let Some((oid, dec)) = last_order {
                log_unfilled_paper_order(&oid, &dec, secs, config, db, telegram).await;
            }
            info!("[PAPER] Window closing — no fill");
            return None;
        }

        // Advance to the next tier whose threshold we've just crossed
        while next_tier_idx < tiers.len() {
            let tier = &tiers[next_tier_idx];
            if secs > tier.time_before_close_s {
                break; // Haven't reached this tier's time yet
            }

            info!(
                tier = %tier.name,
                secs_left = secs,
                "Tier threshold crossed"
            );

            // Evaluate signal for this specific tier
            let decision = {
                let st = state.read().await;
                signal_engine.evaluate_tier(secs, &st, market_info, &tiers, max_bet)
            };

            match decision {
                    None => {
                        info!(tier = %tier.name, "Tier skipped (insufficient delta or price)");
                        next_tier_idx += 1;
                    }
                    Some(d) => {
                        // Cancel previous pending order (paper: log as unfilled)
                        if let Some((prev_oid, prev_dec)) = last_order.take() {
                            info!(
                                cancelled_tier = %prev_dec.tier_name,
                                new_tier = %d.tier_name,
                                "Cancel/replace: moving to next tier"
                            );
                            log_unfilled_paper_order(
                                &prev_oid, &prev_dec, secs, config, db, telegram,
                            )
                            .await;
                        }

                        // Place new paper order for this tier
                        match orders::place_paper_order(&d, max_bet).await {
                            Ok(result) => {
                                if result.filled {
                                    let fi = FillInfo {
                                        order_id: result.order_id.clone(),
                                        direction: d.direction,
                                        fill_price: result.fill_price,
                                        fill_size: result.fill_size,
                                        tier_name: d.tier_name.clone(),
                                    };
                                    record_paper_fill(&result, &d, secs, config, db, telegram)
                                        .await;
                                    return Some(fi);
                                } else {
                                    // Order pending — hold until next tier
                                    last_order = Some((result.order_id, d));
                                    next_tier_idx += 1;
                                }
                            }
                            Err(e) => {
                                error!(tier = %tier.name, error = %e, "Paper order error");
                                next_tier_idx += 1;
                            }
                        }
                    }
            }
        }

        if next_tier_idx >= tiers.len() {
            // All tiers exhausted
            if let Some((oid, dec)) = last_order.take() {
                log_unfilled_paper_order(&oid, &dec, secs, config, db, telegram).await;
            }
            info!("[PAPER] All tiers exhausted — no fill this window");
            return None;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Single-entry paper path (tiers disabled).
async fn run_paper_single_entry(
    state: &SharedState,
    config: &AppConfig,
    signal_engine: &SignalEngine,
    risk_manager: &RiskManager,
    market_info: &MarketInfo,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
    secs_left: u64,
) -> Option<FillInfo> {
    let entry_time = config.signal.single_entry.entry_time_before_close_s;

    if secs_left > entry_time + 1 {
        let sleep = secs_left - entry_time - 1;
        tokio::time::sleep(Duration::from_secs(sleep)).await;
    }

    let secs = market::seconds_until_close();
    let max_bet = risk_manager.position_size(&*state.read().await);

    let signal = {
        let st = state.read().await;
        signal_engine.evaluate(&st, market_info)
    };

    let signal = match signal {
        Some(s) => s,
        None => {
            info!("[PAPER] No signal for single-entry window");
            return None;
        }
    };

    let decision = TierDecision {
        tier_name: "single".to_string(),
        time_before_close_s: entry_time,
        direction: signal.direction,
        target_price: signal.target_price,
        delta_pct: signal.delta_pct,
        token_id: signal.token_id.clone(),
        contracts: (max_bet / signal.target_price)
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::ToZero),
        bet_size_usd: max_bet,
    };

    match orders::place_paper_order(&decision, max_bet).await {
        Ok(result) => {
            if result.filled {
                let fi = FillInfo {
                    order_id: result.order_id.clone(),
                    direction: decision.direction,
                    fill_price: result.fill_price,
                    fill_size: result.fill_size,
                    tier_name: decision.tier_name.clone(),
                };
                record_paper_fill(&result, &decision, secs, config, db, telegram).await;
                Some(fi)
            } else {
                log_unfilled_paper_order(&result.order_id, &decision, secs, config, db, telegram)
                    .await;
                None
            }
        }
        Err(e) => {
            error!(error = %e, "[PAPER] Single-entry order error");
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
    let use_tiers = config.signal.entry_tiers.enabled;
    let order_lifetime_ms = config.signal.order_lifetime_ms;
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
                tokio::spawn(async move {
                    settle_trade(ps, mode, db2, tg2, st2).await.ok();
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

        // ── Tier loop ──
        let tiers: Vec<EntryTier> = if use_tiers {
            config.signal.entry_tiers.tiers.clone()
        } else {
            // Synthesise a single pseudo-tier from single_entry config
            vec![crate::config::EntryTier {
                name: "single".to_string(),
                time_before_close_s: config.signal.single_entry.entry_time_before_close_s,
                price_offset: 0.0,
                min_delta_pct: config.signal.min_delta_pct,
            }]
        };

        let max_bet = risk_manager.position_size(&*state.read().await);
        let mut next_tier_idx = 0usize;
        let mut active_order_id: Option<String> = None;
        let mut active_decision: Option<TierDecision> = None;

        'tier_loop: loop {
            let secs = market::seconds_until_close();

            if secs < 1 {
                // Cancel any dangling order
                if active_order_id.is_some() {
                    if let Err(e) = client.cancel_all_orders().await {
                        warn!(error = %e, "Cancel failed at window close");
                    }
                    if let (Some(oid), Some(dec)) = (active_order_id.take(), active_decision.take()) {
                        let fake = OrderResult {
                            order_id: oid,
                            filled: false,
                            fill_price: Decimal::ZERO,
                            fill_size: Decimal::ZERO,
                        };
                        record_live_order(&fake, &dec, secs, config, db, telegram).await;
                    }
                }
                break 'tier_loop;
            }

            // Advance tiers
            while next_tier_idx < tiers.len() {
                let tier = &tiers[next_tier_idx];
                if secs > tier.time_before_close_s {
                    break;
                }

                info!(tier = %tier.name, secs_left = secs, "Tier threshold crossed");

                let decision = {
                    let st = state.read().await;
                    signal_engine.evaluate_tier(secs, &st, &market_info, &tiers, max_bet)
                };

                match decision {
                    None => {
                        let st = state.read().await;
                        let delta = if st.window_open_price > Decimal::ZERO {
                            ((st.btc_price - st.window_open_price).abs() / st.window_open_price * Decimal::from(100)).to_string()
                        } else {
                            "N/A".to_string()
                        };
                        let msg = format!(
                            "Tier '{}' skipped — delta {}% < min {}%",
                            tier.name, delta, tier.min_delta_pct
                        );
                        info!("{}", msg);
                        telegram.send_error(&msg).await.ok();
                        next_tier_idx += 1;
                    }
                    Some(d) => {
                        info!(
                            tier = %d.tier_name, direction = %d.direction,
                            delta = %d.delta_pct, target = %d.target_price,
                            contracts = %d.contracts, "Tier signal fired"
                        );

                        // Cancel previous order
                        if active_order_id.is_some() {
                            let cancel_start = std::time::Instant::now();
                            if let Err(e) = client.cancel_all_orders().await {
                                let msg = format!("Cancel failed before replace: {e}");
                                warn!("{}", msg);
                                telegram.send_error(&msg).await.ok();
                            } else {
                                info!(elapsed_ms = cancel_start.elapsed().as_millis(), "Order cancelled for tier replace");
                            }
                            if let (Some(oid), Some(prev_dec)) =
                                (active_order_id.take(), active_decision.take())
                            {
                                let fake = OrderResult {
                                    order_id: oid,
                                    filled: false,
                                    fill_price: Decimal::ZERO,
                                    fill_size: Decimal::ZERO,
                                };
                                record_live_order(&fake, &prev_dec, secs, config, db, telegram)
                                    .await;
                            }
                        }

                        // Place new order
                        let token_id_res = U256::from_str(&d.token_id);
                        let token_id = match token_id_res {
                            Ok(id) => id,
                            Err(e) => {
                                let msg = format!("Invalid token ID: {e}");
                                error!("{}", msg);
                                telegram.send_error(&msg).await.ok();
                                next_tier_idx += 1;
                                continue;
                            }
                        };

                        let size = d.contracts;
                        let price = d.target_price;

                        // Step 1: Build order
                        let order = match client
                            .limit_order()
                            .token_id(token_id)
                            .size(size)
                            .price(price)
                            .side(Side::Buy)
                            .build()
                            .await
                        {
                            Ok(o) => o,
                            Err(e) => {
                                let msg = format!(
                                    "BUILD ORDER FAILED: tier={} err={:?}",
                                    d.tier_name, e
                                );
                                error!("{}", msg);
                                telegram.send_error(&msg).await.ok();
                                next_tier_idx += 1;
                                active_decision = Some(d);
                                continue;
                            }
                        };

                        // Step 2: Sign order
                        let signed = match client.sign(&signer, order).await {
                            Ok(s) => s,
                            Err(e) => {
                                let msg = format!(
                                    "SIGN ORDER FAILED: tier={} err={:?}",
                                    d.tier_name, e
                                );
                                error!("{}", msg);
                                telegram.send_error(&msg).await.ok();
                                next_tier_idx += 1;
                                active_decision = Some(d);
                                continue;
                            }
                        };

                        // Step 3: Post order to CLOB
                        match client.post_order(signed).await {
                            Ok(resp) => {
                                let oid = resp.order_id.clone();
                                let msg = format!(
                                    "Order SUBMITTED: tier={} id={} price=${}",
                                    d.tier_name, oid, price
                                );
                                info!("{}", msg);
                                telegram.send_error(&msg).await.ok();
                                active_order_id = Some(oid);
                                active_decision = Some(d);
                                next_tier_idx += 1;
                            }
                            Err(e) => {
                                let msg = format!(
                                    "POST ORDER REJECTED: tier={} price={} size={} token={}... err={:?}",
                                    d.tier_name, price, size,
                                    &d.token_id[..20.min(d.token_id.len())],
                                    e
                                );
                                error!("{}", msg);
                                telegram.send_error(&msg).await.ok();
                                next_tier_idx += 1;
                            }
                        }
                    }
                }
            }

            // Check for fill on active order
            if let Some(oid) = &active_order_id {
                let oid_clone = oid.clone();
                match client.orders(&Default::default(), None).await {
                    Ok(page) => {
                        for o in &page.data {
                            if o.id == oid_clone && o.size_matched > Decimal::ZERO {
                                info!(order_id = %oid_clone, "Live order filled!");
                                let result = OrderResult {
                                    order_id: oid_clone.clone(),
                                    filled: true,
                                    fill_price: o.price,
                                    fill_size: o.size_matched,
                                };
                                let dec = active_decision.take().unwrap();
                                let secs_entry = market::seconds_until_close();
                                let btc_open = state.read().await.window_open_price;
                                record_live_order(&result, &dec, secs_entry, config, db, telegram)
                                    .await;
                                last_pending_settle = Some(PendingSettle {
                                    slug: window.slug.clone(),
                                    order_id: oid_clone.clone(),
                                    direction: dec.direction,
                                    fill_price: o.price,
                                    fill_size: o.size_matched,
                                    tier_name: dec.tier_name.clone(),
                                    btc_open_price: btc_open,
                                });
                                break 'tier_loop;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "Poll error");
                    }
                }
            }

            if next_tier_idx >= tiers.len() && active_order_id.is_none() {
                info!("All tiers exhausted — no fill");
                telegram.send_error("All tiers exhausted — no fill this window").await.ok();
                break 'tier_loop;
            }

            // Between-poll deadline: honour order_lifetime_ms cap
            let min_secs_to_next_tier = tiers
                .get(next_tier_idx)
                .map(|t| {
                    let s = market::seconds_until_close();
                    s.saturating_sub(t.time_before_close_s)
                })
                .unwrap_or(0);

            let sleep_ms = if min_secs_to_next_tier > 0 {
                250u64 // poll while waiting for next tier
            } else {
                order_lifetime_ms.min(500)
            };

            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
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

    Ok(())
}
