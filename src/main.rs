mod cli;
mod config;
mod db;
mod feeds;
mod market;
mod orderbook;
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
use rust_decimal_macros::dec;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::cli::Cli;
use crate::config::AppConfig;
use crate::db::TradeDb;
use crate::risk::RiskManager;
use crate::signal::{SignalEngine, TradeDecision};
use crate::state::{new_shared_state, SharedState};
use crate::telegram::TelegramNotifier;
use crate::types::{BotMode, Direction, MarketInfo, TradeOutcome, TradeRecord};

struct PendingSettle {
    slug: String,
    order_id: String,
    direction: Direction,
    fill_price: Decimal,
    fill_size: Decimal,
    btc_open_price: Decimal,
    condition_id: String,
    neg_risk: bool,
    initial_price: Decimal,
    tighten_count: u32,
    best_ask_at_entry: Decimal,
}

#[tokio::main]
async fn main() -> Result<()> {
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
        min_delta = %config.pricing.min_delta_pct,
        max_entry = %config.pricing.max_entry_price,
        "Polymarket BTC Maker Bot starting (orderbook-aware)"
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
    let verbose = cli.verbose;

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
            run_paper_loop(state, &config, &signal_engine, &risk_manager, &db, &telegram, verbose).await
        }
        BotMode::Live => {
            info!("Initializing live CLOB connection...");
            let result =
                run_live_mode(state, &config, &signal_engine, &risk_manager, &db, &telegram, verbose)
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
    verbose: bool,
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

        // Fetch orderbook (paper mode uses real orderbook for realistic pricing)
        let token_id = {
            let st = state.read().await;
            let (dir, _) = signal::compute_delta(st.btc_price, st.window_open_price);
            match dir {
                Direction::Up => market_info.up_token_id.clone(),
                Direction::Down => market_info.down_token_id.clone(),
            }
        };

        let ob = match orderbook::fetch_orderbook(&config.infra.polymarket_clob_url, &token_id).await {
            Ok(ob) => ob,
            Err(e) => {
                warn!(error = %e, "[PAPER] Orderbook fetch failed, skipping window");
                let remaining = market::seconds_until_close();
                tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
                continue;
            }
        };

        let decision = {
            let st = state.read().await;
            signal_engine.evaluate_trade(&st, &market_info, &ob, config, max_bet)
        };

        let decision = match decision {
            Some(d) => d,
            None => {
                let st = state.read().await;
                let (_, delta) = signal::compute_delta(st.btc_price, st.window_open_price);
                let msg = format!(
                    "[PAPER] Skip — delta {}%, best_ask ${}, min_delta {}%",
                    delta, ob.best_ask, config.pricing.min_delta_pct
                );
                info!("{}", msg);
                if verbose { telegram.send_error(&msg).await.ok(); }
                let remaining = market::seconds_until_close();
                tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
                continue;
            }
        };

        // Paper: simulate fill at initial price (optimistic)
        let order_id = format!(
            "paper-ob-{}-{}",
            decision.direction,
            chrono::Utc::now().timestamp_millis()
        );

        let secs = market::seconds_until_close();
        let trade = build_trade_record_from_decision(
            &order_id, &decision, secs, config, true, 0,
        );
        if let Err(e) = db.insert_trade(&trade) {
            error!(error = %e, "Failed to log paper fill");
        }
        telegram.send_fill_ob(&trade, &decision).await.ok();

        info!(
            direction = %decision.direction,
            price = %decision.initial_price,
            best_ask = %ob.best_ask,
            "[PAPER] FILLED (simulated)"
        );

        last_pending_settle = Some(PendingSettle {
            slug: window.slug.clone(),
            order_id,
            direction: decision.direction,
            fill_price: decision.initial_price,
            fill_size: decision.contracts,
            btc_open_price,
            condition_id: market_info.condition_id.clone(),
            neg_risk: market_info.neg_risk,
            initial_price: decision.initial_price,
            tighten_count: 0,
            best_ask_at_entry: decision.best_ask_at_entry,
        });

        let remaining = market::seconds_until_close();
        tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
    }
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
    verbose: bool,
) -> Result<()> {
    use polymarket_client_sdk::auth::{LocalSigner, Signer as _};
    use polymarket_client_sdk::clob::types::SignatureType;
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

    info!("CLOB client authenticated, entering live trading loop (orderbook-aware)");

    let watch_start = config.watch_start_s();
    let mode = config.mode;
    let adjust_ms = config.pricing.adjust_interval_ms;
    let tighten_step = Decimal::try_from(config.pricing.tighten_step).unwrap_or(dec!(0.01));
    let entry_cutoff_s = config.pricing.entry_cutoff_s;
    let clob_url = config.infra.polymarket_clob_url.clone();

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

        let max_bet = risk_manager.position_size(&*state.read().await);

        // Determine which token to check orderbook for
        let token_id_str = {
            let st = state.read().await;
            let (dir, _) = signal::compute_delta(st.btc_price, st.window_open_price);
            match dir {
                Direction::Up => market_info.up_token_id.clone(),
                Direction::Down => market_info.down_token_id.clone(),
            }
        };

        // Fetch orderbook
        let ob = match orderbook::fetch_orderbook(&clob_url, &token_id_str).await {
            Ok(ob) => ob,
            Err(e) => {
                let msg = format!("Orderbook fetch failed: {e}");
                warn!("{}", msg);
                telegram.send_error(&msg).await.ok();
                let remaining = market::seconds_until_close();
                tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
                continue;
            }
        };

        // Evaluate trade decision
        let decision = {
            let st = state.read().await;
            signal_engine.evaluate_trade(&st, &market_info, &ob, config, max_bet)
        };

        let decision = match decision {
            Some(d) => {
                let msg = format!(
                    "Trade starting: {} delta={}% best_ask=${} initial=${} ceiling=${}",
                    d.direction, d.delta_pct, d.best_ask_at_entry, d.initial_price, d.max_price
                );
                info!("{}", msg);
                telegram.send_error(&msg).await.ok();
                d
            }
            None => {
                let st = state.read().await;
                let (_, delta) = signal::compute_delta(st.btc_price, st.window_open_price);
                let delta_f64: f64 = delta.try_into().unwrap_or(0.0);
                let ceiling = config.max_price_for_delta(delta_f64);
                let reason = if delta < Decimal::try_from(config.pricing.min_delta_pct).unwrap_or_default() {
                    format!("Low delta: {}% (need {}%)", delta, config.pricing.min_delta_pct)
                } else {
                    format!(
                        "Book too expensive (best ask ${}, ceiling ${:.2} for {:.2}% delta)",
                        ob.best_ask, ceiling, delta
                    )
                };
                info!("Skip — {}", reason);
                if verbose {
                    telegram.send_skip(&window.slug, &reason).await.ok();
                }

                // Log skip to DB
                let skip_trade = TradeRecord {
                    id: None,
                    timestamp: Utc::now(),
                    window_ts: window.window_ts,
                    market_slug: window.slug.clone(),
                    direction: "NONE".to_string(),
                    token_id: token_id_str.clone(),
                    order_id: format!("skip-{}", window.window_ts),
                    tier_name: "ob_aware".to_string(),
                    entry_price: Decimal::ZERO,
                    size: Decimal::ZERO,
                    cost_usd: Decimal::ZERO,
                    outcome: "SKIPPED".to_string(),
                    pnl: Decimal::ZERO,
                    btc_open_price: st.window_open_price,
                    btc_close_price: Decimal::ZERO,
                    delta_pct: delta,
                    seconds_at_entry: Decimal::from(secs_left),
                    mode: config.mode.to_string(),
                    filled: false,
                    initial_price: Decimal::ZERO,
                    tighten_count: 0,
                    best_ask_at_entry: ob.best_ask,
                    skip_reason: Some(reason),
                };
                let _ = db.insert_trade(&skip_trade);

                let remaining = market::seconds_until_close();
                tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
                continue;
            }
        };

        // ── Orderbook-aware tighten loop ──

        let token_id_u256 = match U256::from_str(&decision.token_id) {
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

        let btc_open_price = state.read().await.window_open_price;
        let mut current_price = decision.initial_price;
        let max_price = decision.max_price;
        let mut active_order_id: Option<String> = None;
        let mut tighten_count: u32 = 0;
        let mut filled = false;

        // ── Inline order helpers (avoids SDK generic type issues) ──

        macro_rules! post_order {
            ($price:expr, $size:expr) => {{
                use polymarket_client_sdk::clob::types::Side;
                let order = client
                    .limit_order()
                    .token_id(token_id_u256)
                    .size($size)
                    .price($price)
                    .side(Side::Buy)
                    .build()
                    .await;
                match order {
                    Ok(o) => match client.sign(&signer, o).await {
                        Ok(s) => match client.post_order(s).await {
                            Ok(r) => Ok(r.order_id.clone()),
                            Err(e) => Err(anyhow::anyhow!("post: {e}")),
                        },
                        Err(e) => Err(anyhow::anyhow!("sign: {e}")),
                    },
                    Err(e) => Err(anyhow::anyhow!("build: {e}")),
                }
            }};
        }

        macro_rules! poll_fill {
            ($oid:expr) => {{
                let mut result: Option<(Decimal, Decimal)> = None;
                let oid_str: &str = $oid;
                if let Ok(Ok(page)) = tokio::time::timeout(
                    Duration::from_secs(3),
                    client.orders(&Default::default(), None),
                ).await {
                    for o in &page.data {
                        if o.id == oid_str && o.size_matched > Decimal::ZERO {
                            result = Some((o.size_matched, o.price));
                            break;
                        }
                    }
                }
                result
            }};
        }

        // Place initial order
        let contracts = (max_bet / current_price)
            .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);

        match post_order!(current_price, contracts) {
            Ok(oid) => {
                info!(order_id = %oid, price = %current_price, "Initial order posted");
                active_order_id = Some(oid);
            }
            Err(e) => {
                warn!(error = ?e, price = %current_price, "Failed to place initial order");
            }
        }

        // Adjustment loop
        loop {
            let secs = market::seconds_until_close();
            if secs < entry_cutoff_s {
                info!(secs_left = secs, "Hit entry cutoff, stopping");
                break;
            }

            let poll_deadline = tokio::time::Instant::now() + Duration::from_millis(adjust_ms);
            while tokio::time::Instant::now() < poll_deadline {
                tokio::time::sleep(Duration::from_millis(300)).await;

                if let Some(ref oid) = active_order_id {
                    if let Some((matched, price)) = poll_fill!(oid) {
                        info!(order_id = %oid, matched = %matched, price = %price, "FILLED!");
                        telegram.send_error(&format!(
                            "FILLED! price=${} contracts={} tightens={}",
                            price, matched, tighten_count
                        )).await.ok();

                        let mut trade = build_trade_record_from_decision(
                            oid, &decision, secs, config, true, tighten_count,
                        );
                        trade.entry_price = price;
                        trade.size = matched;
                        trade.cost_usd = matched * price;
                        telegram.send_fill_ob(&trade, &decision).await.ok();
                        if let Err(e) = db.insert_trade(&trade) {
                            error!(error = %e, "Failed to log live fill");
                        }

                        last_pending_settle = Some(PendingSettle {
                            slug: window.slug.clone(),
                            order_id: oid.clone(),
                            direction: decision.direction,
                            fill_price: price,
                            fill_size: matched,
                            btc_open_price,
                            condition_id: market_info.condition_id.clone(),
                            neg_risk: market_info.neg_risk,
                            initial_price: decision.initial_price,
                            tighten_count,
                            best_ask_at_entry: decision.best_ask_at_entry,
                        });
                        filled = true;
                        break;
                    }
                }
            }

            if filled {
                break;
            }

            // Tighten: re-read orderbook, compute new price
            let fresh_ask = orderbook::refresh_best_ask(&clob_url, &decision.token_id).await;
            let new_price = {
                let stepped = current_price + tighten_step;
                let ask_bound = fresh_ask
                    .map(|a| a - dec!(0.01))
                    .unwrap_or(stepped);
                stepped.min(ask_bound).round_dp(2)
            };

            if new_price > max_price {
                info!(
                    new_price = %new_price,
                    ceiling = %max_price,
                    "Hit price ceiling, stopping"
                );
                telegram.send_error(&format!(
                    "Hit ceiling ${} (ask ${}, delta {}%)",
                    max_price,
                    fresh_ask.unwrap_or(Decimal::ZERO),
                    decision.delta_pct
                )).await.ok();
                break;
            }

            if new_price <= current_price {
                continue;
            }

            // Cancel old, place new
            if active_order_id.is_some() {
                if let Err(e) = client.cancel_all_orders().await {
                    warn!(error = %e, "Cancel failed during tighten");
                }
                active_order_id = None;
            }

            let new_contracts = (max_bet / new_price)
                .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);

            match post_order!(new_price, new_contracts) {
                Ok(oid) => {
                    tighten_count += 1;
                    info!(
                        order_id = %oid,
                        price = %new_price,
                        tighten = tighten_count,
                        best_ask = ?fresh_ask,
                        ceiling = %max_price,
                        "Tightened order"
                    );
                    active_order_id = Some(oid);
                    current_price = new_price;
                }
                Err(e) => {
                    warn!(error = ?e, price = %new_price, "Tighten order failed");
                }
            }
        }

        // Post-loop: if not filled, wait for window close and do final check
        if !filled {
            if let Some(ref oid) = active_order_id {
                let close_ts = window.close_ts();
                let now = chrono::Utc::now().timestamp() as u64;
                if now < close_ts {
                    let wait = close_ts.saturating_sub(now) + 1;
                    info!(wait_secs = wait, "Waiting for window close with last order");
                    tokio::time::sleep(Duration::from_secs(wait)).await;
                }

                if let Some((matched, price)) = poll_fill!(oid) {
                    info!(order_id = %oid, matched = %matched, "Filled at window close!");
                    telegram.send_error(&format!(
                        "FILLED at close! price=${} contracts={}", price, matched
                    )).await.ok();

                    let mut trade = build_trade_record_from_decision(
                        oid, &decision, 0, config, true, tighten_count,
                    );
                    trade.entry_price = price;
                    trade.size = matched;
                    trade.cost_usd = matched * price;
                    telegram.send_fill_ob(&trade, &decision).await.ok();
                    if let Err(e) = db.insert_trade(&trade) {
                        error!(error = %e, "Failed to log close fill");
                    }

                    last_pending_settle = Some(PendingSettle {
                        slug: window.slug.clone(),
                        order_id: oid.clone(),
                        direction: decision.direction,
                        fill_price: price,
                        fill_size: matched,
                        btc_open_price,
                        condition_id: market_info.condition_id.clone(),
                        neg_risk: market_info.neg_risk,
                        initial_price: decision.initial_price,
                        tighten_count,
                        best_ask_at_entry: decision.best_ask_at_entry,
                    });
                    filled = true;
                }

                if !filled {
                    info!(order_id = %oid, last_price = %current_price, "Order expired unfilled");
                    telegram.send_error(&format!(
                        "Expired unfilled (last ${}, {} tightens, best_ask ${})",
                        current_price, tighten_count, decision.best_ask_at_entry
                    )).await.ok();

                    if let Err(e) = client.cancel_all_orders().await {
                        warn!(error = %e, "Cancel failed at window close");
                    }

                    let mut trade = build_trade_record_from_decision(
                        oid, &decision, 0, config, false, tighten_count,
                    );
                    trade.entry_price = current_price;
                    trade.skip_reason = Some("expired".to_string());
                    if let Err(e) = db.insert_trade(&trade) {
                        error!(error = %e, "Failed to log expired order");
                    }
                }
            } else {
                info!("No orders placed this window");
            }
        }

        let remaining = market::seconds_until_close();
        tokio::time::sleep(Duration::from_secs(remaining + 2)).await;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
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

fn build_trade_record_from_decision(
    order_id: &str,
    decision: &TradeDecision,
    secs_at_entry: u64,
    config: &AppConfig,
    filled: bool,
    tighten_count: u32,
) -> TradeRecord {
    let price = decision.initial_price;
    let size = decision.contracts;
    TradeRecord {
        id: None,
        timestamp: Utc::now(),
        window_ts: 0,
        market_slug: String::new(),
        direction: decision.direction.to_string(),
        token_id: decision.token_id.clone(),
        order_id: order_id.to_string(),
        tier_name: "ob_aware".to_string(),
        entry_price: price,
        size,
        cost_usd: size * price,
        outcome: TradeOutcome::Pending.to_string(),
        pnl: Decimal::ZERO,
        btc_open_price: Decimal::ZERO,
        btc_close_price: Decimal::ZERO,
        delta_pct: decision.delta_pct,
        seconds_at_entry: Decimal::from(secs_at_entry),
        mode: config.mode.to_string(),
        filled,
        initial_price: decision.initial_price,
        tighten_count,
        best_ask_at_entry: decision.best_ask_at_entry,
        skip_reason: None,
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
    tokio::time::sleep(Duration::from_secs(25)).await;

    let btc_close = state.read().await.btc_price;
    let our_bet_is_up = ps.direction == Direction::Up;

    let (won, outcome_source) = match mode {
        BotMode::Paper => {
            let up_won = btc_close > ps.btc_open_price;
            let won = redeem::did_we_win(our_bet_is_up, up_won);
            (won, "btc-price")
        }
        BotMode::Live => {
            let mut resolved: Option<bool> = None;
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
            let won = resolved.unwrap_or_else(|| {
                warn!(slug = %ps.slug, "Using BTC price fallback for settlement");
                redeem::did_we_win(our_bet_is_up, btc_close > ps.btc_open_price)
            });
            (won, if resolved.is_some() { "gamma-api" } else { "btc-fallback" })
        }
    };

    let pnl = if won {
        ps.fill_size * (Decimal::ONE - ps.fill_price)
    } else {
        -(ps.fill_size * ps.fill_price)
    };

    let outcome = if won { TradeOutcome::Win } else { TradeOutcome::Loss };

    db.update_outcome(&ps.order_id, &outcome.to_string(), pnl, btc_close)?;

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
        direction = %ps.direction,
        btc_open = %ps.btc_open_price,
        btc_close = %btc_close,
        outcome = %outcome,
        pnl = %pnl,
        bankroll = %bankroll,
        tightens = ps.tighten_count,
        source = outcome_source,
        "Trade settled"
    );

    telegram.send_settlement(
        &ps.slug,
        ps.direction,
        ps.fill_price,
        ps.fill_size,
        ps.btc_open_price,
        btc_close,
        won,
        pnl,
        bankroll,
        ps.initial_price,
        ps.tighten_count,
        ps.best_ask_at_entry,
    ).await.ok();

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
