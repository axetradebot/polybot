mod cli;
mod config;
mod db;
mod feeds;
mod market;
mod orderbook;
mod positions;
mod redeem;
mod risk;
mod scanner;
mod signal;
mod state;
mod telegram;
mod types;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

use crate::cli::Cli;
use crate::config::AppConfig;
use crate::db::TradeDb;
use crate::feeds::PriceFeeds;
use crate::positions::{Position, PositionTracker};
use crate::risk::RiskManager;
use crate::scanner::MarketOpportunity;
use crate::state::{new_shared_state, SharedState};
use crate::telegram::TelegramNotifier;
use crate::types::{BotMode, Direction, MarketInfo, PositionStatus, TradeOutcome, TradeRecord};

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
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "{},polymarket_client_sdk=error",
                    config.general.log_level
                )
                .into()
            }),
        )
        .with_target(false)
        .with_thread_ids(true)
        .with_ansi(false)
        .with_writer(std::sync::Mutex::new(log_file))
        .init();

    let enabled = config.enabled_markets();
    let market_names: Vec<String> = enabled.iter().map(|m| m.name.clone()).collect();
    let symbols = config.binance_symbols();

    info!(
        global_mode = %config.mode,
        bankroll = %config.bankroll.total,
        symbols = ?symbols,
        "Multi-market scanner bot starting"
    );
    for mkt in &enabled {
        let effective = if config.is_market_paper(&mkt.name) { "paper" } else { "live" };
        info!(market = %mkt.name, mode = effective, "Market enabled");
    }

    let state = new_shared_state(config.bankroll_decimal());
    let price_feeds = PriceFeeds::new();
    let positions = PositionTracker::new();

    let db = Arc::new(
        TradeDb::open(config.db_path()).context("Failed to open trade database")?,
    );

    let telegram = Arc::new(TelegramNotifier::new(
        config.telegram_bot_token.clone(),
        config.telegram_chat_id.clone(),
        &config.telegram,
        config.mode,
    ));

    telegram.send_startup(config.bankroll_decimal(), &market_names).await.ok();

    let risk_manager = RiskManager::new(&config);
    let verbose = cli.verbose || config.telegram.verbose_skips;

    // Spawn Binance combined price feed
    price_feeds.spawn_binance_feed(symbols.clone(), &config.infra.binance_ws_base);

    // Wait for at least one price to arrive
    info!("Waiting for initial price data from Binance...");
    loop {
        let mut any_fresh = false;
        for sym in &symbols {
            if price_feeds.has_fresh_price(sym).await {
                any_fresh = true;
                break;
            }
        }
        if any_fresh {
            for sym in &symbols {
                if let Some(p) = price_feeds.get_price(sym).await {
                    info!(symbol = %sym, price = %p, "Initial price received");
                }
            }
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Run the main scanner loop
    let result = run_scanner_loop(
        &config,
        &price_feeds,
        &positions,
        state.clone(),
        &db,
        &telegram,
        &risk_manager,
        verbose,
    )
    .await;

    if let Err(ref e) = result {
        let msg = format!("Scanner loop crashed: {e}");
        error!("{}", msg);
        telegram.send_error(&msg).await.ok();
    }

    result
}

// ─────────────────────────────────────────────────────────────────────────────
// Main scanner loop
// ─────────────────────────────────────────────────────────────────────────────

async fn run_scanner_loop(
    config: &AppConfig,
    price_feeds: &PriceFeeds,
    positions: &PositionTracker,
    state: SharedState,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
    risk_manager: &RiskManager,
    _verbose: bool,
) -> Result<()> {
    use polymarket_client_sdk::auth::{LocalSigner, Signer as _};
    use polymarket_client_sdk::clob::types::SignatureType;
    use polymarket_client_sdk::clob::{Client, Config};
    use polymarket_client_sdk::types::U256;
    use polymarket_client_sdk::POLYGON;

    // ── CLOB client setup (only if any market is live) ──

    let (client, signer) = if config.needs_clob_client() {
        let s = match LocalSigner::from_str(&config.private_key) {
            Ok(s) => s.with_chain_id(Some(POLYGON)),
            Err(e) => {
                let msg = format!("Invalid private key: {e}");
                error!("{}", msg);
                telegram.send_error(&msg).await.ok();
                anyhow::bail!(msg);
            }
        };

        info!("Authenticating with CLOB...");
        let mut builder = Client::new(&config.infra.polymarket_clob_url, Config::default())?
            .authentication_builder(&s);

        match config.signature_type()? {
            crate::types::SignatureType::GnosisSafe => {
                builder = builder.signature_type(SignatureType::GnosisSafe);
            }
            crate::types::SignatureType::Proxy => {
                builder = builder.signature_type(SignatureType::Proxy);
            }
            crate::types::SignatureType::Eoa => {}
        }

        let c = match builder.authenticate().await {
            Ok(c) => c,
            Err(e) => {
                let msg = format!("CLOB authentication FAILED: {e}");
                error!("{}", msg);
                telegram.send_error(&msg).await.ok();
                anyhow::bail!(msg);
            }
        };

        info!("CLOB client authenticated");
        (Some(c), Some(s))
    } else {
        info!("Pure paper mode — no CLOB client needed");
        (None, None)
    };

    // ── Macros for CLOB operations ──

    macro_rules! post_order {
        ($price:expr, $size:expr, $token_id_u256:expr) => {{
            use polymarket_client_sdk::clob::types::Side;
            let client = client.as_ref().unwrap();
            let signer = signer.as_ref().unwrap();
            let order = client
                .limit_order()
                .token_id($token_id_u256)
                .size($size)
                .price($price)
                .side(Side::Buy)
                .build()
                .await;
            match order {
                Ok(o) => match client.sign(signer, o).await {
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

    macro_rules! poll_fills {
        () => {{
            let client = client.as_ref().unwrap();
            let mut fills: Vec<(String, Decimal, Decimal)> = Vec::new();
            if let Ok(Ok(page)) = tokio::time::timeout(
                Duration::from_secs(3),
                client.orders(&Default::default(), None),
            )
            .await
            {
                for o in &page.data {
                    if o.size_matched > Decimal::ZERO {
                        fills.push((o.id.clone(), o.price, o.size_matched));
                    }
                }
            }
            fills
        }};
    }

    // ── State tracking ──

    let mut last_window_ts: HashMap<String, u64> = HashMap::new();
    let mut token_cache: HashMap<String, MarketInfo> = HashMap::new();
    let scan_interval = Duration::from_millis(config.scanner.scan_interval_ms);
    let adjust_ms = config.pricing.adjust_interval_ms;
    let tighten_step = Decimal::try_from(config.pricing.tighten_step).unwrap_or(dec!(0.01));
    let entry_cutoff_s = config.pricing.entry_cutoff_s;
    let clob_url = config.infra.polymarket_clob_url.clone();

    info!(
        scan_interval_ms = config.scanner.scan_interval_ms,
        max_concurrent = config.bankroll.max_concurrent_positions,
        max_per_market = config.bankroll.max_per_market,
        "Entering scanner loop"
    );

    loop {
        let cycle_start = tokio::time::Instant::now();

        // ── Daily reset ──
        state.write().await.reset_daily_if_needed();

        // ── Phase 1: Window management ──
        for mkt in config.enabled_markets() {
            let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);
            let prev_ts = last_window_ts.get(&mkt.name).copied();

            if prev_ts != Some(window_ts) {
                // New window detected
                if let Some(price) = price_feeds.get_price(&mkt.binance_symbol).await {
                    price_feeds
                        .set_window_open(&mkt.slug_prefix, window_ts, price)
                        .await;
                    info!(
                        market = %mkt.name,
                        window_ts,
                        open_price = %price,
                        secs_remaining = secs_rem,
                        "New window started"
                    );
                }

                // Resolve tokens for this window (best-effort)
                let slug = market::build_slug(&mkt.slug_prefix, window_ts);
                let slug_clone = slug.clone();
                // We do a quick single attempt; scanner will skip if not yet resolved
                match market::resolve_market(&slug_clone).await {
                    Ok(info) => {
                        token_cache.insert(slug, info);
                    }
                    Err(e) => {
                        warn!(market = %mkt.name, slug = %slug_clone, error = %e, "Token resolve failed (will retry)");
                    }
                }

                last_window_ts.insert(mkt.name.clone(), window_ts);
            } else {
                // Retry token resolution if missing
                let slug = market::build_slug(&mkt.slug_prefix, window_ts);
                if !token_cache.contains_key(&slug) && secs_rem > entry_cutoff_s {
                    match market::resolve_market(&slug).await {
                        Ok(info) => {
                            info!(market = %mkt.name, slug = %slug, "Token resolved on retry");
                            token_cache.insert(slug, info);
                        }
                        Err(_) => {}
                    }
                }
            }
        }

        // Prune old window data periodically
        let now_ts = market::epoch_secs();
        price_feeds.prune_old_window_opens(now_ts, 3600).await;
        positions.prune_old(7200).await;

        // Clean expired token cache entries
        token_cache.retain(|slug, _| {
            slug.rsplit_once('-')
                .and_then(|(_, ts)| ts.parse::<u64>().ok())
                .map(|ts| now_ts.saturating_sub(ts) < 3600)
                .unwrap_or(false)
        });

        // ── Phase 2: Run edge scanner ──
        let opportunities =
            scanner::scan_all_markets(config, price_feeds, &token_cache, positions).await;

        // ── Phase 3: Fire orders at best opportunities ──
        let mut orders_fired = 0;

        // Check global risk once
        let global_ok = {
            let st = state.read().await;
            risk_manager.check_global(&st).is_ok()
        };

        if global_ok {
            for (rank, opp) in opportunities.iter().enumerate() {
                if orders_fired >= config.scanner.max_orders_per_cycle {
                    break;
                }

                // Per-market position limit
                if let Err(veto) = risk_manager
                    .check_position_limits(positions, &opp.market_name)
                    .await
                {
                    if matches!(veto, risk::RiskVeto::MaxConcurrentReached) {
                        break;
                    }
                    continue;
                }

                let is_paper = config.is_market_paper(&opp.market_name);
                let mode = if is_paper { BotMode::Paper } else { config.mode };
                let now_inst = tokio::time::Instant::now();

                if is_paper {
                    // Paper: immediate simulated fill at suggested_entry
                    let order_id = format!(
                        "paper-{}-{}-{}",
                        opp.market_name.replace(' ', ""),
                        opp.direction,
                        Utc::now().timestamp_millis()
                    );

                    let pos = Position {
                        id: 0,
                        market_name: opp.market_name.clone(),
                        asset: opp.asset.clone(),
                        window_seconds: opp.window_seconds,
                        window_ts: opp.window_ts,
                        slug: opp.slug.clone(),
                        direction: opp.direction,
                        token_id: opp.token_id.clone(),
                        condition_id: opp.condition_id.clone(),
                        neg_risk: opp.neg_risk,
                        edge_score: opp.edge_score,
                        delta_pct: Decimal::try_from(opp.delta_pct).unwrap_or_default(),
                        initial_price: opp.suggested_entry,
                        current_price: opp.suggested_entry,
                        max_price: opp.max_entry,
                        best_ask_at_entry: opp.best_ask,
                        contracts: opp.contracts,
                        bet_size_usd: opp.bet_size_usd,
                        order_id: Some(order_id.clone()),
                        tighten_count: 0,
                        last_adjust_at: now_inst,
                        placed_at: now_inst,
                        open_price: opp.open_price,
                        status: PositionStatus::Filled,
                        fill_price: Some(opp.suggested_entry),
                        fill_size: Some(opp.contracts),
                        outcome: None,
                        pnl: None,
                        mode,
                        settlement_started: false,
                    };

                    let pos_id = positions.add(pos).await;

                    let trade = build_trade_record(opp, &order_id, mode, true, 0);
                    if let Err(e) = db.insert_trade(&trade) {
                        error!(error = %e, "Failed to log paper fill");
                    }

                    let open_count = positions.open_count().await;
                    telegram
                        .send_fill_multi(
                            &trade,
                            opp,
                            rank + 1,
                            opportunities.len(),
                            open_count,
                            config.bankroll.max_concurrent_positions,
                        )
                        .await
                        .ok();

                    info!(
                        market = %opp.market_name,
                        direction = %opp.direction,
                        price = %opp.suggested_entry,
                        edge = opp.edge_score,
                        pos_id,
                        "[PAPER] Filled (simulated)"
                    );
                } else if client.is_some() {
                    // Live: place CLOB order
                    let token_id_u256 = match U256::from_str(&opp.token_id) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(error = %e, market = %opp.market_name, "Invalid token ID");
                            continue;
                        }
                    };

                    match post_order!(opp.suggested_entry, opp.contracts, token_id_u256) {
                        Ok(oid) => {
                            let pos = Position {
                                id: 0,
                                market_name: opp.market_name.clone(),
                                asset: opp.asset.clone(),
                                window_seconds: opp.window_seconds,
                                window_ts: opp.window_ts,
                                slug: opp.slug.clone(),
                                direction: opp.direction,
                                token_id: opp.token_id.clone(),
                                condition_id: opp.condition_id.clone(),
                                neg_risk: opp.neg_risk,
                                edge_score: opp.edge_score,
                                delta_pct: Decimal::try_from(opp.delta_pct).unwrap_or_default(),
                                initial_price: opp.suggested_entry,
                                current_price: opp.suggested_entry,
                                max_price: opp.max_entry,
                                best_ask_at_entry: opp.best_ask,
                                contracts: opp.contracts,
                                bet_size_usd: opp.bet_size_usd,
                                order_id: Some(oid.clone()),
                                tighten_count: 0,
                                last_adjust_at: now_inst,
                                placed_at: now_inst,
                                open_price: opp.open_price,
                                status: PositionStatus::Pending,
                                fill_price: None,
                                fill_size: None,
                                outcome: None,
                                pnl: None,
                                mode,
                                settlement_started: false,
                            };

                            let pos_id = positions.add(pos).await;

                            info!(
                                market = %opp.market_name,
                                order_id = %oid,
                                price = %opp.suggested_entry,
                                edge = opp.edge_score,
                                pos_id,
                                "Live order posted"
                            );
                        }
                        Err(e) => {
                            warn!(
                                error = ?e,
                                market = %opp.market_name,
                                price = %opp.suggested_entry,
                                "Failed to post order"
                            );
                        }
                    }
                }

                orders_fired += 1;
            }
        }

        // ── Phase 4: Manage pending live positions ──
        if client.is_some() {
            let pending = positions.pending_positions().await;

            if !pending.is_empty() {
                // Batch check all orders for fills
                let fills = poll_fills!();

                for pos in &pending {
                    if let Some(ref oid) = pos.order_id {
                        // Check for fill
                        if let Some((_, fill_price, fill_size)) =
                            fills.iter().find(|(id, _, _)| id == oid)
                        {
                            positions
                                .mark_filled(pos.id, *fill_price, *fill_size)
                                .await;

                            let mut trade = build_trade_record_from_position(pos);
                            trade.filled = true;
                            trade.fill_price = Some(*fill_price);
                            trade.final_price = *fill_price;
                            trade.contracts = *fill_size;
                            trade.bet_size_usd = *fill_size * *fill_price;
                            if let Err(e) = db.insert_trade(&trade) {
                                error!(error = %e, "Failed to log live fill");
                            }

                            info!(
                                market = %pos.market_name,
                                order_id = %oid,
                                fill_price = %fill_price,
                                fill_size = %fill_size,
                                tightens = pos.tighten_count,
                                "FILLED!"
                            );
                            continue;
                        }

                        // Check for expiry (past window close)
                        let now = market::epoch_secs();
                        if now >= pos.window_ts + pos.window_seconds {
                            // Cancel the order
                    if let Some(ref c) = client {
                        let _ = c.cancel_all_orders().await;
                    }
                            positions.mark_expired(pos.id).await;

                            let mut trade = build_trade_record_from_position(pos);
                            trade.skip_reason = Some("expired".into());
                            let _ = db.insert_trade(&trade);

                            info!(
                                market = %pos.market_name,
                                order_id = %oid,
                                last_price = %pos.current_price,
                                tightens = pos.tighten_count,
                                "Order expired unfilled"
                            );
                            continue;
                        }
                    }
                }

                // Tighten eligible positions
                let to_tighten = positions.needs_tighten(adjust_ms).await;
                for pos in &to_tighten {
                    let (_, secs_rem) = market::current_window(pos.window_seconds);
                    if secs_rem < entry_cutoff_s {
                        continue;
                    }

                    let fresh_ask =
                        orderbook::refresh_best_ask(&clob_url, &pos.token_id).await;
                    let new_price = {
                        let stepped = pos.current_price + tighten_step;
                        let ask_bound = fresh_ask
                            .map(|a| a - dec!(0.01))
                            .unwrap_or(stepped);
                        stepped.min(ask_bound).round_dp(2)
                    };

                    if new_price > pos.max_price || new_price <= pos.current_price {
                        continue;
                    }

                    // Cancel old order, place new
                    if let Some(ref c) = client {
                        let _ = c.cancel_all_orders().await;
                    }

                    let token_id_u256 = match U256::from_str(&pos.token_id) {
                        Ok(id) => id,
                        Err(_) => continue,
                    };

                    let new_contracts = (risk_manager.bet_size() / new_price)
                        .round_dp_with_strategy(0, rust_decimal::RoundingStrategy::ToZero);

                    match post_order!(new_price, new_contracts, token_id_u256) {
                        Ok(oid) => {
                            positions
                                .update_tighten(pos.id, oid.clone(), new_price)
                                .await;
                            info!(
                                market = %pos.market_name,
                                order_id = %oid,
                                price = %new_price,
                                tighten = pos.tighten_count + 1,
                                "Tightened order"
                            );
                        }
                        Err(e) => {
                            warn!(error = ?e, market = %pos.market_name, "Tighten order failed");
                        }
                    }
                }
            }
        }

        // ── Phase 5: Settlement ──
        let to_settle = positions.filled_past_close().await;
        for pos in &to_settle {
            positions.mark_settlement_started(pos.id).await;

            let pos_clone = pos.clone();
            let db2 = db.clone();
            let tg2 = telegram.clone();
            let st2 = state.clone();
            let pt2 = positions.clone();
            let rpc = config.infra.polygon_rpc_url.clone();
            let pk = config.private_key.clone();
            let do_redeem = config.infra.auto_redeem;

            tokio::spawn(async move {
                settle_position(pos_clone, db2, tg2, st2, pt2, &rpc, &pk, do_redeem)
                    .await
                    .ok();
            });
        }

        // ── Sleep until next scan ──
        let elapsed = cycle_start.elapsed();
        if elapsed < scan_interval {
            tokio::time::sleep(scan_interval - elapsed).await;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Settlement
// ─────────────────────────────────────────────────────────────────────────────

async fn settle_position(
    pos: Position,
    db: Arc<TradeDb>,
    telegram: Arc<TelegramNotifier>,
    state: SharedState,
    positions: PositionTracker,
    polygon_rpc_url: &str,
    private_key: &str,
    auto_redeem: bool,
) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(25)).await;

    let fill_price = pos.fill_price.unwrap_or(pos.current_price);
    let fill_size = pos.fill_size.unwrap_or(pos.contracts);
    let our_bet_is_up = pos.direction == Direction::Up;

    let (won, _outcome_source) = match pos.mode {
        BotMode::Paper => {
            // Use asset price movement for settlement
            let close_price = {
                // For paper, we use a short delay then consider
                // direction based on position data.
                // In practice, the actual close price would be fetched from feeds.
                // For simplicity, use BTC-equivalent logic.
                fill_price // Placeholder: actual settlement uses Gamma API
            };
            let _ = close_price;
            // Use Gamma API settlement for paper mode too
            let mut resolved: Option<bool> = None;
            for attempt in 0..10 {
                match redeem::check_settlement(&pos.slug).await {
                    Ok(Some(up_won)) => {
                        resolved = Some(redeem::did_we_win(our_bet_is_up, up_won));
                        break;
                    }
                    Ok(None) => {
                        info!(attempt, slug = %pos.slug, market = %pos.market_name, "Settlement pending...");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, attempt, "Settlement check error");
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }
                }
            }
            let won = resolved.unwrap_or_else(|| {
                warn!(slug = %pos.slug, "Settlement unresolved, defaulting to loss");
                false
            });
            (won, "gamma-api")
        }
        BotMode::Live => {
            let mut resolved: Option<bool> = None;
            for attempt in 0..20 {
                match redeem::check_settlement(&pos.slug).await {
                    Ok(Some(up_won)) => {
                        resolved = Some(redeem::did_we_win(our_bet_is_up, up_won));
                        break;
                    }
                    Ok(None) => {
                        info!(attempt, slug = %pos.slug, market = %pos.market_name, "Settlement pending...");
                        tokio::time::sleep(Duration::from_secs(30)).await;
                    }
                    Err(e) => {
                        warn!(error = %e, attempt, "Settlement check error");
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }
                }
            }
            let won = resolved.unwrap_or_else(|| {
                warn!(slug = %pos.slug, "Using fallback for settlement");
                false
            });
            (
                won,
                if resolved.is_some() {
                    "gamma-api"
                } else {
                    "fallback"
                },
            )
        }
    };

    let pnl = if won {
        fill_size * (Decimal::ONE - fill_price)
    } else {
        -(fill_size * fill_price)
    };

    let outcome = if won {
        TradeOutcome::Win
    } else {
        TradeOutcome::Loss
    };

    // Update position tracker
    positions.mark_settled(pos.id, outcome, pnl).await;

    // Update DB
    if let Some(ref oid) = pos.order_id {
        let close_price = Decimal::ZERO; // Could fetch actual close if needed
        db.update_outcome(oid, &outcome.to_string(), pnl, close_price)?;
    }

    // Update global state
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
        market = %pos.market_name,
        slug = %pos.slug,
        direction = %pos.direction,
        outcome = %outcome,
        pnl = %pnl,
        bankroll = %bankroll,
        tightens = pos.tighten_count,
        edge = pos.edge_score,
        "Trade settled"
    );

    telegram
        .send_settlement(
            &pos.market_name,
            &pos.slug,
            pos.direction,
            fill_price,
            fill_size,
            pos.open_price,
            Decimal::ZERO,
            won,
            pnl,
            bankroll,
            pos.initial_price,
            pos.tighten_count,
            pos.best_ask_at_entry,
            &pos.mode.to_string(),
        )
        .await
        .ok();

    // Auto-redeem winning live positions
    if won
        && auto_redeem
        && pos.mode == BotMode::Live
        && !pos.condition_id.is_empty()
    {
        info!(
            market = %pos.market_name,
            slug = %pos.slug,
            "Attempting auto-redeem..."
        );
        match redeem::auto_redeem(polygon_rpc_url, private_key, &pos.condition_id, pos.neg_risk)
            .await
        {
            Ok(tx_hash) => {
                info!(tx = %tx_hash, "Auto-redeemed successfully");
                telegram
                    .send_redeem_success(&pos.market_name, &tx_hash)
                    .await
                    .ok();
            }
            Err(e) => {
                warn!(error = %e, "Auto-redeem failed");
                telegram
                    .send_error(&format!(
                        "Auto-redeem failed for {}: {e:#}",
                        pos.market_name
                    ))
                    .await
                    .ok();
            }
        }
    }

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn build_trade_record(
    opp: &MarketOpportunity,
    order_id: &str,
    mode: BotMode,
    filled: bool,
    tighten_count: u32,
) -> TradeRecord {
    TradeRecord {
        id: None,
        timestamp: Utc::now(),
        market_name: opp.market_name.clone(),
        asset: opp.asset.clone(),
        window_seconds: opp.window_seconds,
        window_ts: opp.window_ts,
        slug: opp.slug.clone(),
        mode: mode.to_string(),
        direction: opp.direction.to_string(),
        token_id: opp.token_id.clone(),
        order_id: order_id.to_string(),
        initial_price: opp.suggested_entry,
        final_price: opp.suggested_entry,
        tighten_count,
        best_ask_at_entry: opp.best_ask,
        filled,
        fill_price: if filled { Some(opp.suggested_entry) } else { None },
        outcome: TradeOutcome::Pending.to_string(),
        pnl: Decimal::ZERO,
        delta_pct: Decimal::try_from(opp.delta_pct).unwrap_or_default(),
        edge_score: opp.edge_score,
        seconds_remaining: opp.seconds_remaining as f64,
        contracts: opp.contracts,
        bet_size_usd: opp.bet_size_usd,
        open_price: opp.open_price,
        close_price: Decimal::ZERO,
        skip_reason: None,
    }
}

fn build_trade_record_from_position(pos: &Position) -> TradeRecord {
    TradeRecord {
        id: None,
        timestamp: Utc::now(),
        market_name: pos.market_name.clone(),
        asset: pos.asset.clone(),
        window_seconds: pos.window_seconds,
        window_ts: pos.window_ts,
        slug: pos.slug.clone(),
        mode: pos.mode.to_string(),
        direction: pos.direction.to_string(),
        token_id: pos.token_id.clone(),
        order_id: pos.order_id.clone().unwrap_or_default(),
        initial_price: pos.initial_price,
        final_price: pos.current_price,
        tighten_count: pos.tighten_count,
        best_ask_at_entry: pos.best_ask_at_entry,
        filled: false,
        fill_price: None,
        outcome: TradeOutcome::Pending.to_string(),
        pnl: Decimal::ZERO,
        delta_pct: pos.delta_pct,
        edge_score: pos.edge_score,
        seconds_remaining: 0.0,
        contracts: pos.contracts,
        bet_size_usd: pos.bet_size_usd,
        open_price: pos.open_price,
        close_price: Decimal::ZERO,
        skip_reason: None,
    }
}
