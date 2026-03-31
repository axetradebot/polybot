mod cli;
mod config;
mod db;
mod feeds;
mod hourly_discovery;
mod market;
mod orderbook;
mod positions;
mod redeem;
mod risk;
mod scanner;
mod shadow_timing;
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
use tracing::{debug, error, info, warn};

use crate::cli::Cli;
use crate::config::AppConfig;
use crate::db::TradeDb;
use crate::feeds::PriceFeeds;
use crate::positions::{Position, PositionTracker};
use crate::risk::RiskManager;
use crate::scanner::MarketOpportunity;
use crate::state::{new_shared_state, SharedState};
use crate::telegram::TelegramNotifier;
use crate::types::{BotMode, Direction, PositionStatus, TradeOutcome, TradeRecord};

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

    // ── Diagnostic mode ──
    if cli.diagnose {
        return run_diagnostics(&config).await;
    }

    // ── Reconciliation mode ──
    if cli.reconcile {
        return run_reconciliation(&config).await;
    }

    let enabled = config.enabled_markets();
    let chainlink_symbols = config.chainlink_symbols();
    let binance_symbols = config.binance_symbols();

    let boot_utc = Utc::now();
    let boot_epoch = market::epoch_secs();
    info!(
        global_mode = %config.mode,
        bankroll = %config.bankroll.total,
        chainlink_symbols = ?chainlink_symbols,
        binance_fallback = ?binance_symbols,
        system_utc = %boot_utc.format("%Y-%m-%d %H:%M:%S%.3f UTC"),
        system_epoch = boot_epoch,
        "Multi-market scanner bot starting"
    );
    for mkt in &enabled {
        let effective = if config.is_market_paper(&mkt.name) { "paper" } else { "live" };
        let price_sym = if mkt.resolution_source == "binance" { &mkt.binance_symbol } else { &mkt.chainlink_symbol };
        info!(market = %mkt.name, mode = effective, source = %mkt.resolution_source, symbol = %price_sym, "Market enabled");
    }

    let state = new_shared_state(config.bankroll_decimal());
    let price_feeds = PriceFeeds::new();
    let positions = PositionTracker::new();

    let db = Arc::new(
        TradeDb::open(config.db_path()).context("Failed to open trade database")?,
    );

    // Backfill shadow data with authoritative Polymarket resolutions
    match db.backfill_shadow_from_poly() {
        Ok(n) if n > 0 => info!(corrected = n, "Backfilled shadow data with Polymarket resolutions"),
        Ok(_) => info!("Shadow data already in sync with Polymarket resolutions"),
        Err(e) => warn!(error = %e, "Failed to backfill shadow data from Polymarket resolutions"),
    }

    let telegram = Arc::new(TelegramNotifier::new(
        config.telegram_bot_token.clone(),
        config.telegram_chat_id.clone(),
        &config.telegram,
        config.mode,
    ));

    let risk_manager = RiskManager::new(&config);
    let verbose = cli.verbose || config.telegram.verbose_skips;

    // Register Chainlink → Binance fallback mappings
    for (cl_sym, bn_sym) in config.fallback_symbol_map() {
        price_feeds.register_fallback(&cl_sym, &bn_sym).await;
    }

    // Spawn Chainlink (PRIMARY) price feed from Polymarket
    price_feeds.spawn_chainlink_feed(chainlink_symbols.clone(), &config.infra.chainlink_ws_url);

    // Spawn Binance (FALLBACK) price feed — only activates if Chainlink goes stale
    price_feeds.spawn_binance_feed(binance_symbols.clone(), &config.infra.binance_ws_base);

    // Wait for price data before starting — per-market resolution source
    info!("Waiting for initial price data...");
    let price_wait_start = tokio::time::Instant::now();
    loop {
        let mut all_fresh = true;
        for mkt in &enabled {
            if !price_feeds.has_fresh_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
                all_fresh = false;
                break;
            }
        }
        if all_fresh {
            for mkt in &enabled {
                if let Some(p) = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
                    info!(market = %mkt.name, price = %p, source = %mkt.resolution_source, "Initial price received");
                }
            }
            break;
        }
        if price_wait_start.elapsed() > Duration::from_secs(30) {
            warn!("Timed out waiting for prices, starting with available data");
            for mkt in &enabled {
                if let Some(p) = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
                    info!(market = %mkt.name, price = %p, source = %mkt.resolution_source, "Price available");
                } else {
                    warn!(market = %mkt.name, source = %mkt.resolution_source, "No price data yet");
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
// Reconciliation mode — re-check past trades against Polymarket resolutions
// ─────────────────────────────────────────────────────────────────────────────

async fn run_reconciliation(config: &AppConfig) -> Result<()> {
    let db = TradeDb::open(std::path::Path::new(&config.infra.db_path))?;
    let trades = db.get_filled_trades_for_reconciliation()?;

    println!("\n{}", "=".repeat(70));
    println!("  TRADE RECONCILIATION — checking {} filled trades", trades.len());
    println!("{}\n", "=".repeat(70));

    let mut checked = 0u32;
    let mut corrections = 0u32;
    let mut unresolved = 0u32;

    for (order_id, slug, token_id, current_outcome, fill_price_str, contracts_str) in &trades {
        if token_id.is_empty() || slug.is_empty() {
            println!("  SKIP  order={} — missing slug or token_id", order_id);
            continue;
        }

        match redeem::check_settlement(slug, token_id).await {
            Ok(Some(won)) => {
                checked += 1;
                let correct_outcome = if won { "WIN" } else { "LOSS" };

                if current_outcome != correct_outcome {
                    corrections += 1;
                    let fill_price: Decimal = fill_price_str.parse().unwrap_or(Decimal::ZERO);
                    let contracts: Decimal = contracts_str.parse().unwrap_or(Decimal::ZERO);
                    let pnl = if won {
                        contracts * (Decimal::ONE - fill_price)
                    } else {
                        -(contracts * fill_price)
                    };

                    println!(
                        "  FIX   order={} slug={} — was {} → now {} (pnl: {})",
                        order_id, slug, current_outcome, correct_outcome, pnl
                    );

                    if let Err(e) = db.update_outcome(order_id, correct_outcome, pnl, Decimal::ZERO) {
                        println!("  ERROR updating order {}: {}", order_id, e);
                    }
                } else {
                    println!(
                        "  OK    order={} slug={} — {} confirmed",
                        order_id, slug, current_outcome
                    );
                }
            }
            Ok(None) => {
                unresolved += 1;
                println!("  PEND  order={} slug={} — market not yet resolved", order_id, slug);
            }
            Err(e) => {
                println!("  ERR   order={} slug={} — {}", order_id, slug, e);
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    println!("\n{}", "-".repeat(70));
    println!(
        "  Checked: {} | Corrected: {} | Unresolved: {} | Total: {}",
        checked, corrections, unresolved, trades.len()
    );
    println!("{}\n", "=".repeat(70));

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Diagnostic mode
// ─────────────────────────────────────────────────────────────────────────────

async fn run_diagnostics(config: &AppConfig) -> Result<()> {
    let clob_url = &config.infra.polymarket_clob_url;

    println!("\n{}", "=".repeat(70));
    println!("  POLYBOT ORDERBOOK DIAGNOSTIC");
    println!("{}\n", "=".repeat(70));

    for mkt in config.enabled_markets() {
        println!("\n--- Market: {} ({}s windows, slug_prefix={}) ---",
            mkt.name, mkt.window_seconds, mkt.slug_prefix);

        let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);
        println!("  Current window: ts={window_ts}, {secs_rem}s remaining");

        // Resolve tokens
        if mkt.is_hourly() {
            println!("  Market type: HOURLY");
            let discovery = hourly_discovery::HourlyDiscovery::new(clob_url);
            match discovery.get_current_market(&mkt.slug_prefix).await {
                Ok(Some(hm)) => {
                    println!("  Hourly slug: {}", hm.event_slug);
                    println!("  End time:    {}", hm.end_time);
                    println!("  Accepting:   {}", hm.accepting_orders);
                    println!("  UP  token:   {}", hm.up_token_id);
                    println!("  DOWN token:  {}", hm.down_token_id);
                    test_orderbook(clob_url, &hm.up_token_id, &hm.down_token_id, &hm.event_slug).await;
                }
                Ok(None) => println!("  ERROR: No active hourly market found!"),
                Err(e) => println!("  ERROR: Hourly discovery failed: {e}"),
            }
        } else {
            let slug = market::build_slug(&mkt.slug_prefix, window_ts);
            println!("  Market type: {}min", mkt.window_seconds / 60);
            println!("  Slug:        {slug}");

            match market::resolve_market(&slug, clob_url).await {
                Ok(info) => {
                    println!("  Accepting:   {}", info.accepting_orders);
                    println!("  UP  token:   {}", info.up_token_id);
                    println!("  DOWN token:  {}", info.down_token_id);
                    test_orderbook(clob_url, &info.up_token_id, &info.down_token_id, &slug).await;
                }
                Err(e) => {
                    println!("  ERROR: Token resolution failed: {e}");
                    println!("  This could mean the market for this window hasn't been created yet.");
                    println!("  Try again in a few seconds after the window starts.");
                }
            }
        }
    }

    println!("\n{}", "=".repeat(70));
    println!("  DIAGNOSTIC COMPLETE");
    println!("{}\n", "=".repeat(70));
    println!("If one token has asks and the other doesn't, that's normal.");
    println!("If BOTH tokens have 0 asks, the market may be settled or not yet live.");
    println!("If the token the bot selected has 0 asks but the OTHER has asks,");
    println!("the UP/DOWN mapping was wrong — the fix above should resolve this.\n");

    Ok(())
}

async fn test_orderbook(clob_url: &str, up_token: &str, down_token: &str, slug: &str) {
    println!("\n  Testing UP token orderbook...");
    let up_url = format!("{}/book?token_id={}", clob_url, up_token);
    println!("    URL: {up_url}");
    match reqwest::Client::new()
        .get(&up_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) => match resp.text().await {
            Ok(body) => {
                let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                let asks = parsed["asks"].as_array().map(|a| a.len()).unwrap_or(0);
                let bids = parsed["bids"].as_array().map(|b| b.len()).unwrap_or(0);
                let top_ask = parsed["asks"][0]["price"].as_str().unwrap_or("none");
                let top_bid = parsed["bids"][0]["price"].as_str().unwrap_or("none");
                println!("    UP token: {asks} asks, {bids} bids | top_ask={top_ask} top_bid={top_bid}");
                if asks == 0 {
                    println!("    ⚠ UP token has NO ASKS");
                }
            }
            Err(e) => println!("    ERROR reading response: {e}"),
        },
        Err(e) => println!("    ERROR fetching: {e}"),
    }

    println!("  Testing DOWN token orderbook...");
    let down_url = format!("{}/book?token_id={}", clob_url, down_token);
    println!("    URL: {down_url}");
    match reqwest::Client::new()
        .get(&down_url)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await
    {
        Ok(resp) => match resp.text().await {
            Ok(body) => {
                let parsed: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                let asks = parsed["asks"].as_array().map(|a| a.len()).unwrap_or(0);
                let bids = parsed["bids"].as_array().map(|b| b.len()).unwrap_or(0);
                let top_ask = parsed["asks"][0]["price"].as_str().unwrap_or("none");
                let top_bid = parsed["bids"][0]["price"].as_str().unwrap_or("none");
                println!("    DOWN token: {asks} asks, {bids} bids | top_ask={top_ask} top_bid={top_bid}");
                if asks == 0 {
                    println!("    ⚠ DOWN token has NO ASKS");
                }
            }
            Err(e) => println!("    ERROR reading response: {e}"),
        },
        Err(e) => println!("    ERROR fetching: {e}"),
    }

    println!("\n  Slug for manual verification: {slug}");
    println!("  curl test commands:");
    println!("    curl -s \"{clob_url}/book?token_id={up_token}\" | python3 -m json.tool");
    println!("    curl -s \"{clob_url}/book?token_id={down_token}\" | python3 -m json.tool");
}

// ─────────────────────────────────────────────────────────────────────────────
// Resolution audit checker — polls Polymarket for actual outcomes
// ─────────────────────────────────────────────────────────────────────────────

async fn run_resolution_audit_checker(db: Arc<TradeDb>) {
    use polymarket_client_sdk::gamma::Client as GammaClient;
    use polymarket_client_sdk::gamma::types::request::MarketBySlugRequest;

    let gamma = GammaClient::default();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;

        let pending = match db.pending_resolution_audits() {
            Ok(rows) => rows,
            Err(e) => {
                warn!(error = %e, "Failed to fetch pending resolution audits");
                continue;
            }
        };

        if pending.is_empty() {
            continue;
        }

        info!(count = pending.len(), "Checking pending resolution audits");

        for (market_name, window_ts, slug) in &pending {
            let request = MarketBySlugRequest::builder().slug(slug.as_str()).build();

            match gamma.market_by_slug(&request).await {
                Ok(market) => {
                    let is_closed = market.closed.unwrap_or(false);
                    if !is_closed {
                        continue;
                    }

                    let outcome_prices = market.outcome_prices.unwrap_or_default();
                    let outcomes = market.outcomes.unwrap_or_default();
                    if outcome_prices.len() < 2 {
                        warn!(slug = %slug, "Market closed but missing outcome_prices");
                        continue;
                    }

                    let poly_resolution = if outcomes.len() >= 2 {
                        let winner_idx = outcome_prices.iter().position(|p| *p == rust_decimal::Decimal::ONE);
                        match winner_idx {
                            Some(idx) => {
                                let label = outcomes[idx].to_uppercase();
                                if label == "UP" || label == "YES" { "UP" }
                                else if label == "DOWN" || label == "NO" { "DOWN" }
                                else {
                                    warn!(slug = %slug, outcome = %outcomes[idx], "Unknown outcome label");
                                    continue;
                                }
                            }
                            None => {
                                warn!(slug = %slug, prices = ?outcome_prices, "Market closed but no winner (no 1.0 price)");
                                continue;
                            }
                        }
                    } else {
                        if outcome_prices[0] == rust_decimal::Decimal::ONE { "UP" }
                        else if outcome_prices[1] == rust_decimal::Decimal::ONE { "DOWN" }
                        else {
                            warn!(slug = %slug, prices = ?outcome_prices, "Market closed but no clear winner");
                            continue;
                        }
                    };

                    if let Err(e) = db.update_resolution_audit(market_name, *window_ts, poly_resolution) {
                        warn!(error = %e, slug = %slug, "Failed to update resolution audit");
                    } else {
                        info!(
                            market = %market_name,
                            slug = %slug,
                            poly_resolution,
                            "Resolution audit updated"
                        );

                        // Re-settle shadow tables with authoritative Polymarket resolution
                        match db.resettle_shadow_with_poly(market_name, *window_ts, poly_resolution) {
                            Ok(n) if n > 0 => info!(
                                market = %market_name,
                                rows = n,
                                poly_resolution,
                                "Shadow trade re-settled with Polymarket resolution"
                            ),
                            Err(e) => warn!(error = %e, market = %market_name, "Failed to re-settle shadow trade"),
                            _ => {}
                        }
                        match db.resettle_shadow_timing_with_poly(market_name, *window_ts, poly_resolution) {
                            Ok(n) if n > 0 => info!(
                                market = %market_name,
                                rows = n,
                                poly_resolution,
                                "Shadow timing re-settled with Polymarket resolution"
                            ),
                            Err(e) => warn!(error = %e, market = %market_name, "Failed to re-settle shadow timing"),
                            _ => {}
                        }

                        // Audit: compare our open price vs Polymarket's priceToBeat
                        if let Some(ptb) = market::fetch_price_to_beat(slug).await {
                            let our_open = db.get_resolution_audit_open(market_name, *window_ts);
                            if let Ok(Some(our_open_str)) = our_open {
                                if let Ok(our_open_val) = our_open_str.parse::<f64>() {
                                    let ptb_f64: f64 = ptb.try_into().unwrap_or(0.0);
                                    let diff = our_open_val - ptb_f64;
                                    let diff_pct = if ptb_f64 > 0.0 { (diff / ptb_f64) * 100.0 } else { 0.0 };
                                    info!(
                                        market = %market_name,
                                        our_open = our_open_val,
                                        poly_ptb = ptb_f64,
                                        diff = diff,
                                        diff_pct = format!("{:.4}%", diff_pct),
                                        "Price-to-beat audit"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, slug = %slug, "Failed to query Gamma API for audit");
                }
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }
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

        // Sync bankroll with real Polymarket portfolio balance
        {
            use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
            use polymarket_client_sdk::clob::types::AssetType;
            let req = BalanceAllowanceRequest::builder()
                .asset_type(AssetType::Collateral)
                .build();
            match c.balance_allowance(req).await {
                Ok(resp) => {
                    let usdc_decimals = Decimal::from(1_000_000u64);
                    let real_balance = resp.balance / usdc_decimals;
                    info!(clob_balance = %real_balance, config_bankroll = %config.bankroll.total, "Syncing bankroll from Polymarket");
                    let mut st = state.write().await;
                    st.bankroll = real_balance;
                }
                Err(e) => {
                    warn!(error = %e, "Could not fetch CLOB balance, using config bankroll");
                }
            }
        }

        (Some(c), Some(s))
    } else {
        info!("Pure paper mode — no CLOB client needed");
        (None, None)
    };

    // Send startup message with real balance
    {
        let st = state.read().await;
        let market_modes: Vec<(String, String)> = config.enabled_markets().iter().map(|m| {
            let mode = if config.is_market_paper(&m.name) { "paper" } else { "live" };
            (m.name.clone(), mode.to_string())
        }).collect();
        telegram.send_startup(st.bankroll, &market_modes).await.ok();
    }

    info!("Warm-up active: all markets will skip trading until their first full window boundary (prevents wrong-direction bets from stale open prices)");

    // ── Macros for CLOB operations ──

    macro_rules! post_order {
        ($price:expr, $size:expr, $token_id_u256:expr) => {{
            use polymarket_client_sdk::clob::types::Side;
            let client = client.as_ref().unwrap();
            let signer = signer.as_ref().unwrap();
            let t0 = std::time::Instant::now();
            let order = client
                .limit_order()
                .token_id($token_id_u256)
                .size($size)
                .price($price)
                .side(Side::Buy)
                .build()
                .await;
            let build_ms = t0.elapsed().as_millis();
            match order {
                Ok(o) => {
                    let t1 = std::time::Instant::now();
                    match client.sign(signer, o).await {
                        Ok(s) => {
                            let sign_ms = t1.elapsed().as_millis();
                            let t2 = std::time::Instant::now();
                            match client.post_order(s).await {
                                Ok(r) => {
                                    let post_ms = t2.elapsed().as_millis();
                                    info!(
                                        build_ms = build_ms,
                                        sign_ms = sign_ms,
                                        post_ms = post_ms,
                                        total_ms = t0.elapsed().as_millis(),
                                        order_id = %r.order_id,
                                        "CLOB order pipeline timing"
                                    );
                                    Ok(r.order_id.clone())
                                }
                                Err(e) => {
                                    warn!(build_ms = build_ms, sign_ms = sign_ms, post_ms = t2.elapsed().as_millis(), "CLOB post failed");
                                    Err(anyhow::anyhow!("post: {e}"))
                                }
                            }
                        }
                        Err(e) => {
                            warn!(build_ms = build_ms, sign_ms = t1.elapsed().as_millis(), "CLOB sign failed");
                            Err(anyhow::anyhow!("sign: {e}"))
                        }
                    }
                }
                Err(e) => {
                    warn!(build_ms = build_ms, "CLOB order build failed");
                    Err(anyhow::anyhow!("build: {e}"))
                }
            }
        }};
    }

    macro_rules! poll_fills {
        () => {{
            use polymarket_client_sdk::clob::types::OrderStatusType;

            let client = client.as_ref().unwrap();
            let mut fills: Vec<(String, Decimal, Decimal)> = Vec::new();
            let mut seen_ids = std::collections::HashSet::new();

            // Method 1: Query each pending order by ID — require status==Matched.
            let pending = positions.pending_positions().await;
            for pos in &pending {
                if let Some(ref oid) = pos.order_id {
                    let req = polymarket_client_sdk::clob::types::request::OrdersRequest::builder()
                        .order_id(oid.as_str())
                        .build();
                    if let Ok(Ok(page)) = tokio::time::timeout(
                        Duration::from_secs(3),
                        client.orders(&req, None),
                    )
                    .await
                    {
                        for o in &page.data {
                            let is_matched = o.status == OrderStatusType::Matched;
                            let has_size = o.size_matched > Decimal::ZERO;
                            debug!(
                                order_id = %o.id,
                                status = ?o.status,
                                size_matched = %o.size_matched,
                                original_size = %o.original_size,
                                price = %o.price,
                                "poll_fills: order query result"
                            );
                            if o.id == *oid && has_size {
                                if !is_matched {
                                    info!(
                                        order_id = %o.id,
                                        status = ?o.status,
                                        size_matched = %o.size_matched,
                                        original_size = %o.original_size,
                                        price = %o.price,
                                        "poll_fills: partial fill (size_matched < original, status != Matched)"
                                    );
                                } else {
                                    info!(
                                        order_id = %o.id,
                                        status = ?o.status,
                                        size_matched = %o.size_matched,
                                        price = %o.price,
                                        "poll_fills: confirmed fill via orders endpoint"
                                    );
                                }
                                fills.push((o.id.clone(), o.price, o.size_matched));
                                seen_ids.insert(o.id.clone());
                            }
                        }
                    }
                }
            }

            // Method 2: Query recent trades — check both taker_order_id and maker_orders.
            if let Ok(Ok(page)) = tokio::time::timeout(
                Duration::from_secs(3),
                client.trades(
                    &polymarket_client_sdk::clob::types::request::TradesRequest::default(),
                    None,
                ),
            )
            .await
            {
                for t in &page.data {
                    // Check if we were the taker
                    if !seen_ids.contains(&t.taker_order_id) && t.size > Decimal::ZERO {
                        fills.push((t.taker_order_id.clone(), t.price, t.size));
                        seen_ids.insert(t.taker_order_id.clone());
                    }
                    // Check if we were the maker (our order_id appears in maker_orders)
                    for mo in &t.maker_orders {
                        if !seen_ids.contains(&mo.order_id) && mo.matched_amount > Decimal::ZERO {
                            fills.push((mo.order_id.clone(), mo.price, mo.matched_amount));
                            seen_ids.insert(mo.order_id.clone());
                        }
                    }
                }
            }

            fills
        }};
    }

    // Fetch real USDC balance from the CLOB API.
    macro_rules! fetch_clob_balance {
        () => {{
            if let Some(ref c) = client {
                use polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest;
                let req = BalanceAllowanceRequest::builder()
                    .asset_type(polymarket_client_sdk::clob::types::AssetType::Collateral)
                    .build();
                match tokio::time::timeout(Duration::from_secs(3), c.balance_allowance(req)).await {
                    Ok(Ok(resp)) => {
                        let usdc_decimals = Decimal::from(1_000_000u64);
                        Some(resp.balance / usdc_decimals)
                    }
                    _ => None,
                }
            } else {
                None
            }
        }};
    }

    // ── State tracking ──

    let mut last_window_ts: HashMap<String, u64> = HashMap::new();
    let token_cache: shadow_timing::SharedTokenCache =
        Arc::new(tokio::sync::RwLock::new(HashMap::new()));
    let mut window_traded: HashMap<String, bool> = HashMap::new();
    // Markets that have completed at least one full window transition since
    // startup. On the first window we join mid-way, so our captured "open"
    // price is unreliable (it's the current price at boot, not the true
    // window-start price Polymarket uses as "Price to Beat"). Trading with a
    // wrong open causes inverted direction bets → guaranteed losses.
    let mut warmed_up: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut last_skip_reasons: HashMap<String, String> = HashMap::new();
    let mut skip_notified: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut shadow_recorded: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut watching_markets: HashMap<String, (u64, u32)> = HashMap::new();
    let mut window_max_delta: HashMap<String, (f64, String)> = HashMap::new();
    let mut early_reject_until: HashMap<String, std::time::Instant> = HashMap::new();
    let mut scanner_result_counts: HashMap<String, u64> = HashMap::new();
    let mut last_cl_msg_count: u64 = 0;
    let mut last_bn_msg_count: u64 = 0;
    let mut last_analytics_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let mut risk_alert_sent: std::collections::HashSet<String> = std::collections::HashSet::new();
    let scan_interval = Duration::from_millis(config.scanner.scan_interval_ms);
    let adjust_ms = config.pricing.adjust_interval_ms;
    let tighten_step = Decimal::try_from(config.pricing.tighten_step).unwrap_or(dec!(0.01));
    let entry_cutoff_s = config.pricing.entry_cutoff_s;
    let clob_url = config.infra.polymarket_clob_url.clone();

    // Hourly market discovery
    let hourly_discovery = hourly_discovery::HourlyDiscovery::new(&clob_url);
    let hourly_slug_map: shadow_timing::SharedSlugMap =
        Arc::new(tokio::sync::RwLock::new(HashMap::new()));

    // Orderbook cache shared between scanner and shadow timing (2s TTL)
    let ob_cache = orderbook::OrderbookCache::new(2000);

    // ── Spawn shadow timing tracker (observation-only, separate async task) ──
    {
        let st_config = config.clone();
        let st_feeds = price_feeds.clone();
        let st_db = db.clone();
        let st_tokens = token_cache.clone();
        let st_slugs = hourly_slug_map.clone();
        let st_cache = ob_cache.clone();
        tokio::spawn(async move {
            shadow_timing::run_shadow_timing(
                st_config, st_feeds, st_db, st_tokens, st_slugs, st_cache,
            )
            .await;
        });
    }

    // Spawn resolution audit checker — polls Polymarket for actual outcomes
    {
        let audit_db = db.clone();
        tokio::spawn(async move {
            run_resolution_audit_checker(audit_db).await;
        });
    }

    info!(
        scan_interval_ms = config.scanner.scan_interval_ms,
        max_concurrent = config.bankroll.max_concurrent_positions,
        max_per_market = config.bankroll.max_per_market,
        "Entering scanner loop"
    );

    // ── Recover persisted positions from previous session ──
    match db.load_active_positions() {
        Ok(persisted) if !persisted.is_empty() => {
            let persisted: Vec<crate::db::PersistedPosition> = persisted;
            info!(count = persisted.len(), "Recovering positions from previous session");
            let now_inst = tokio::time::Instant::now();
            for pp in &persisted {
                let direction = if pp.direction.eq_ignore_ascii_case("UP") {
                    Direction::Up
                } else {
                    Direction::Down
                };
                let mode = if pp.mode.eq_ignore_ascii_case("live") {
                    BotMode::Live
                } else {
                    BotMode::Paper
                };
                let status = match pp.status.as_str() {
                    "FILLED" => PositionStatus::Filled,
                    _ => PositionStatus::Pending,
                };
                let pos = Position {
                    id: 0,
                    db_id: Some(pp.id),
                    market_name: pp.market_name.clone(),
                    asset: pp.asset.clone(),
                    window_seconds: pp.window_seconds,
                    window_ts: pp.window_ts,
                    slug: pp.slug.clone(),
                    direction,
                    token_id: pp.token_id.clone(),
                    condition_id: pp.condition_id.clone(),
                    neg_risk: pp.neg_risk,
                    edge_score: pp.edge_score,
                    delta_pct: pp.delta_pct.parse().unwrap_or_default(),
                    initial_price: pp.initial_price.parse().unwrap_or_default(),
                    current_price: pp.current_price.parse().unwrap_or_default(),
                    max_price: pp.max_price.parse().unwrap_or_default(),
                    best_ask_at_entry: pp.best_ask_at_entry.parse().unwrap_or_default(),
                    contracts: pp.contracts.parse().unwrap_or_default(),
                    bet_size_usd: pp.bet_size_usd.parse().unwrap_or_default(),
                    order_id: pp.order_id.clone(),
                    tighten_count: pp.tighten_count,
                    last_adjust_at: now_inst,
                    placed_at: now_inst,
                    open_price: pp.open_price.parse().unwrap_or_default(),
                    status,
                    fill_price: pp.fill_price.as_ref().and_then(|s| s.parse::<Decimal>().ok()),
                    fill_size: pp.fill_size.as_ref().and_then(|s| s.parse::<Decimal>().ok()),
                    outcome: None,
                    pnl: None,
                    mode,
                    settlement_started: false,
                    submit_secs_before_close: None,
                    confirm_secs_before_close: None,
                    pipeline_ms: None,
                    is_early_limit: pp.initial_price.parse::<Decimal>().unwrap_or_default() == dec!(0.30),
                };
                let now_epoch = market::epoch_secs();
                let window_end = pos.window_ts + pos.window_seconds;
                if now_epoch >= window_end && status == PositionStatus::Filled {
                    info!(
                        market = %pos.market_name,
                        slug = %pos.slug,
                        direction = %pos.direction,
                        mode = %pos.mode,
                        "Recovered FILLED position — spawning settlement"
                    );
                    let pos_id = positions.add(pos.clone()).await;
                    positions.mark_settlement_started(pos_id).await;

                    let pos_for_settle = {
                        let mut p = pos;
                        p.id = pos_id;
                        p
                    };
                    let pp_id = pp.id;
                    let db2 = db.clone();
                    let tg2 = telegram.clone();
                    let st2 = state.clone();
                    let pt2 = positions.clone();
                    let rpc = config.infra.polygon_rpc_url.clone();
                    let pk = config.private_key.clone();
                    let do_redeem = config.infra.auto_redeem;

                    tokio::spawn(async move {
                        let result = settle_position(
                            pos_for_settle, db2.clone(), tg2, st2, pt2, &rpc, &pk, do_redeem,
                        )
                        .await;
                        if result.is_ok() {
                            let _ = db2.remove_position(pp_id);
                        }
                    });
                } else if now_epoch >= window_end && status == PositionStatus::Pending {
                    info!(
                        market = %pp.market_name,
                        slug = %pp.slug,
                        "Recovered PENDING position past window close — checking for fill"
                    );
                    if client.is_some() {
                        let fills = poll_fills!();
                        if let Some(ref oid) = pp.order_id {
                            if let Some((_, fill_price, fill_size)) =
                                fills.iter().find(|(id, _, _)| id == oid)
                            {
                                info!(
                                    market = %pp.market_name,
                                    fill_price = %fill_price,
                                    fill_size = %fill_size,
                                    "Recovered position was actually FILLED — spawning settlement"
                                );
                                let mut filled_pos = pos.clone();
                                filled_pos.status = PositionStatus::Filled;
                                filled_pos.fill_price = Some(*fill_price);
                                filled_pos.fill_size = Some(*fill_size);
                                let pos_id = positions.add(filled_pos.clone()).await;
                                positions.mark_settlement_started(pos_id).await;
                                filled_pos.id = pos_id;

                                let db2 = db.clone();
                                let tg2 = telegram.clone();
                                let st2 = state.clone();
                                let pt2 = positions.clone();
                                let rpc = config.infra.polygon_rpc_url.clone();
                                let pk = config.private_key.clone();
                                let do_redeem = config.infra.auto_redeem;
                                let pp_id = pp.id;

                                tokio::spawn(async move {
                                    let result = settle_position(
                                        filled_pos, db2.clone(), tg2, st2, pt2, &rpc, &pk, do_redeem,
                                    )
                                    .await;
                                    if result.is_ok() {
                                        let _ = db2.remove_position(pp_id);
                                    }
                                });
                            } else {
                                info!(
                                    market = %pp.market_name,
                                    "Recovered position truly expired — cleaning up"
                                );
                                let _ = db.remove_position(pp.id);
                            }
                        } else {
                            let _ = db.remove_position(pp.id);
                        }
                    } else if mode == BotMode::Paper {
                        info!(
                            market = %pp.market_name,
                            slug = %pp.slug,
                            "Recovered PAPER position — spawning settlement"
                        );
                        let mut filled_pos = pos.clone();
                        filled_pos.status = PositionStatus::Filled;
                        filled_pos.fill_price = Some(filled_pos.current_price);
                        filled_pos.fill_size = Some(filled_pos.contracts);
                        let pos_id = positions.add(filled_pos.clone()).await;
                        positions.mark_settlement_started(pos_id).await;
                        filled_pos.id = pos_id;

                        let db2 = db.clone();
                        let tg2 = telegram.clone();
                        let st2 = state.clone();
                        let pt2 = positions.clone();
                        let rpc = config.infra.polygon_rpc_url.clone();
                        let pk = config.private_key.clone();
                        let do_redeem = config.infra.auto_redeem;
                        let pp_id = pp.id;

                        tokio::spawn(async move {
                            let result = settle_position(
                                filled_pos, db2.clone(), tg2, st2, pt2, &rpc, &pk, do_redeem,
                            )
                            .await;
                            if result.is_ok() {
                                let _ = db2.remove_position(pp_id);
                            }
                        });
                    } else {
                        let _ = db.remove_position(pp.id);
                    }
                } else {
                    let pos_id = positions.add(pos).await;
                    info!(
                        market = %pp.market_name,
                        slug = %pp.slug,
                        status = %pp.status,
                        pos_id,
                        "Recovered position into active tracker"
                    );
                }
            }
        }
        Ok(_) => {}
        Err(e) => warn!(error = %e, "Failed to load persisted positions"),
    }

    let mut heartbeat_counter: u64 = 0;
    let hb_markets: Vec<(String, String, String)> = config.enabled_markets().iter().map(|m| {
        let sym = if m.resolution_source == "binance" { m.binance_symbol.clone() } else { m.chainlink_symbol.clone() };
        (m.name.clone(), m.resolution_source.clone(), sym)
    }).collect();

    loop {
        let cycle_start = tokio::time::Instant::now();
        heartbeat_counter += 1;
        let mut missed_windows: Vec<(String, String)> = Vec::new();

        // Snapshot shared token caches into locals for this cycle
        let mut tc_local = token_cache.read().await.clone();
        let mut hs_local = hourly_slug_map.read().await.clone();

        // Log heartbeat every 60 seconds
        if heartbeat_counter % 60 == 0 {
            let open_pos = positions.open_count().await;
            let ws_msgs = crate::feeds::binance::messages_received();
            let st = state.read().await;
            let clob_bal = fetch_clob_balance!();
            let mut price_ages = Vec::new();
            for (name, _src, sym) in &hb_markets {
                if let Some(age) = price_feeds.price_age_ms(sym).await {
                    price_ages.push(format!("{}={}ms", name, age));
                } else {
                    price_ages.push(format!("{}=NONE", name));
                }
            }
            info!(
                positions = open_pos,
                bankroll = %st.bankroll,
                clob_balance = ?clob_bal,
                wins = st.daily_wins,
                losses = st.daily_losses,
                ws_messages = ws_msgs,
                price_ages = %price_ages.join(", "),
                "Heartbeat"
            );
        }

        // Scanner diagnostic heartbeat every 5 minutes
        if heartbeat_counter % 300 == 0 && heartbeat_counter > 0 {
            let mut market_deltas = Vec::new();
            for mkt in config.enabled_markets() {
                let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);
                let binance_sym = mkt.binance_symbol.to_lowercase();
                let current = price_feeds.get_price(&binance_sym).await;
                let open = price_feeds.get_binance_window_open(&mkt.slug_prefix, window_ts).await;
                let cl_open = price_feeds.get_window_open(&mkt.slug_prefix, window_ts).await;
                let delta = match (current, open) {
                    (Some(c), Some(o)) if o > Decimal::ZERO => {
                        let d: f64 = (((c - o).abs() / o) * Decimal::from(100)).try_into().unwrap_or(0.0);
                        format!("{:.4}%", d)
                    }
                    (Some(_), None) => "NO_OPEN".into(),
                    (None, _) => "NO_PRICE".into(),
                    _ => "ZERO_OPEN".into(),
                };
                let slug = market::build_slug(&mkt.slug_prefix, window_ts);
                let has_token = tc_local.contains_key(&slug);
                market_deltas.push(format!(
                    "{}[T-{}s delta={} open={} cl_open={} tok={}]",
                    mkt.name, secs_rem, delta,
                    open.map(|p| p.to_string()).unwrap_or("NONE".into()),
                    cl_open.map(|p| p.to_string()).unwrap_or("NONE".into()),
                    has_token,
                ));
            }

            let mut result_summary: Vec<String> = scanner_result_counts
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();
            result_summary.sort();

            let bn_age = price_feeds.price_age_ms("btcusdt").await;
            let cl_age = price_feeds.price_age_ms("btc/usd").await;

            let cl_now = crate::feeds::chainlink::messages_received();
            let bn_now = crate::feeds::binance::messages_received();
            let cl_delta = cl_now.saturating_sub(last_cl_msg_count);
            let bn_delta = bn_now.saturating_sub(last_bn_msg_count);
            last_cl_msg_count = cl_now;
            last_bn_msg_count = bn_now;

            let st = state.read().await;

            info!(
                scanner_results = %result_summary.join(", "),
                binance_age_ms = ?bn_age,
                chainlink_age_ms = ?cl_age,
                cl_msgs_5min = cl_delta,
                bn_msgs_5min = bn_delta,
                bankroll = %st.bankroll,
                daily_pnl = %st.daily_pnl,
                consec_losses = st.consecutive_losses,
                markets = %market_deltas.join(" | "),
                "Scanner alive — 5min diagnostic"
            );

            if cl_delta == 0 {
                warn!("Chainlink feed produced 0 messages in 5 minutes — may be dead");
            }
            if bn_delta == 0 {
                warn!("Binance feed produced 0 messages in 5 minutes — may be dead");
            }

            scanner_result_counts.clear();
        }

        // Telegram heartbeat every 15 minutes
        if heartbeat_counter % 900 == 0 {
            let open_pos = positions.open_count().await;
            let st = state.read().await;
            let hb_clob_bal = fetch_clob_balance!();
            let mut snapshots = Vec::new();
            for mkt in config.enabled_markets() {
                let current_price = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await.unwrap_or_default();
                let (window_ts, _) = market::current_window(mkt.window_seconds);
                let open_price = price_feeds.get_window_open(&mkt.slug_prefix, window_ts).await.unwrap_or(current_price);
                let delta_pct = if open_price > Decimal::ZERO {
                    let diff: f64 = ((current_price - open_price).abs() / open_price * Decimal::from(100)).try_into().unwrap_or(0.0);
                    if current_price >= open_price { diff } else { -diff }
                } else {
                    0.0
                };
                snapshots.push(crate::telegram::MarketSnapshot {
                    name: mkt.name.clone(),
                    is_live: !config.is_market_paper(&mkt.name),
                    current_price: current_price.try_into().unwrap_or(0.0),
                    delta_pct,
                    min_delta: mkt.min_delta_pct,
                });
            }
            telegram.send_heartbeat(
                &snapshots,
                open_pos,
                st.bankroll,
                st.daily_wins,
                st.daily_losses,
                st.daily_pnl,
                heartbeat_counter / 60,
                hb_clob_bal,
            ).await.ok();
        }

        // ── Daily reset + analytics ──
        {
            let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
            if today != last_analytics_date {
                // Day rolled over — send yesterday's analytics
                match db.daily_analytics() {
                    Ok(analytics) => {
                        let st = state.read().await;
                        telegram.send_daily_analytics(&analytics, st.bankroll).await.ok();
                    }
                    Err(e) => warn!(error = %e, "Failed to compute daily analytics"),
                }
                last_analytics_date = today;
            }
        }
        state.write().await.reset_daily_if_needed();

        // ── Phase 1: Window management ──
        for mkt in config.enabled_markets() {
            let (window_ts, secs_rem) = market::current_window(mkt.window_seconds);
            let prev_ts = last_window_ts.get(&mkt.name).copied();

            if prev_ts != Some(window_ts) {
                // Window transitioned — check if previous window missed trades
                if let Some(old_ts) = prev_ts {
                    if warmed_up.insert(mkt.name.clone()) {
                        info!(market = %mkt.name, "Market warmed up — first full window boundary observed, trading enabled");
                    }
                    let key = format!("{}-{}", mkt.name, old_ts);
                    let traded = window_traded.get(&key).copied().unwrap_or(false);
                    let old_open = price_feeds.get_window_open(&mkt.slug_prefix, old_ts).await.unwrap_or_default();
                    let current = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await.unwrap_or(old_open);

                    let watch_key = format!("{}-{}", mkt.name, old_ts);
                    let was_watching = watching_markets.remove(&watch_key);

                    if !traded {
                        let base_reason = last_skip_reasons
                            .get(&mkt.name)
                            .cloned()
                            .unwrap_or_else(|| "No opportunity found".into());
                        let reason = if let Some((started_secs, poll_count)) = was_watching {
                            format!("Watched ~{}s (polled {} times) — {}", started_secs, poll_count, base_reason)
                        } else {
                            base_reason
                        };
                        missed_windows.push((mkt.name.clone(), reason.clone()));

                        let (md, md_dir) = window_max_delta.remove(&key).unwrap_or((0.0, String::new()));
                        let _ = db.upsert_window_summary(
                            &mkt.name, old_ts, &old_open.to_string(), &current.to_string(),
                            md, Some(&md_dir), false, None, None, None, Some(&reason),
                        );
                    }

                    // Settle shadow trade: determine direction from Chainlink open→close
                    let actual_dir = if current > old_open {
                        "UP"
                    } else if current < old_open {
                        "DOWN"
                    } else {
                        "FLAT"
                    };

                    // Transition logging: compare both sources at window close
                    {
                        let chainlink_price = price_feeds.get_price_with_fallback(&mkt.chainlink_symbol).await;
                        let binance_price = price_feeds.get_price(&mkt.binance_symbol).await;
                        let using_fallback = price_feeds.is_using_fallback(&mkt.chainlink_symbol).await;
                        let other_dir = |p: Decimal| {
                            if p > old_open { "UP" } else if p < old_open { "DOWN" } else { "FLAT" }
                        };
                        let chainlink_dir = chainlink_price.map(|p| other_dir(p));
                        let binance_dir = binance_price.map(|p| other_dir(p));
                        let diverged = match (chainlink_dir, binance_dir) {
                            (Some(c), Some(b)) => c != b,
                            _ => false,
                        };
                        info!(
                            market = %mkt.name,
                            resolution_source = %mkt.resolution_source,
                            close = %current,
                            chainlink_close = ?chainlink_price.map(|p| p.to_string()),
                            binance_close = ?binance_price.map(|p| p.to_string()),
                            direction = actual_dir,
                            chainlink_dir = ?chainlink_dir,
                            binance_dir = ?binance_dir,
                            open = %old_open,
                            diverged,
                            using_fallback,
                            "Window close price comparison"
                        );
                    }

                    if let Err(e) = db.settle_shadow_trade(
                        &mkt.name, old_ts, &current.to_string(), actual_dir,
                    ) {
                        warn!(error = %e, market = %mkt.name, "Failed to settle shadow trade");
                    }
                    shadow_recorded.remove(&format!("{}-{}", mkt.name, old_ts));

                    // Settle shadow timing snapshots for this window
                    let timing_actual = if actual_dir == "FLAT" { "DOWN" } else { actual_dir };
                    let window_id = format!(
                        "{}-{}m-{}",
                        mkt.slug_prefix.split('-').next().unwrap_or(&mkt.slug_prefix),
                        mkt.window_seconds / 60,
                        old_ts
                    );
                    if let Err(e) = db.settle_shadow_timing(&window_id, timing_actual) {
                        warn!(error = %e, market = %mkt.name, "Failed to settle shadow timing");
                    }

                    // Record resolution audit — our prediction vs what Polymarket actually resolves
                    {
                        let old_slug = market::build_slug(&mkt.slug_prefix, old_ts);
                        let source = if mkt.resolution_source == "binance" {
                            "binance"
                        } else if price_feeds.is_using_fallback(&mkt.chainlink_symbol).await {
                            "chainlink_fallback_to_binance"
                        } else {
                            "chainlink"
                        };
                        if let Err(e) = db.insert_resolution_audit(
                            &mkt.name,
                            old_ts,
                            &old_slug,
                            &old_open.to_string(),
                            &current.to_string(),
                            actual_dir,
                            source,
                        ) {
                            warn!(error = %e, market = %mkt.name, "Failed to insert resolution audit");
                        }
                    }

                    window_traded.remove(&key);
                    early_reject_until.remove(&key);
                    window_max_delta.remove(&key);
                }
                // Clear skip reason and skip notifications for fresh window
                last_skip_reasons.remove(&mkt.name);
                skip_notified.retain(|k| !k.starts_with(&mkt.name));

                // Capture the Chainlink (USD) open for direction determination.
                // We use get_market_price (Chainlink with Binance fallback) rather
                // than the tick-buffer because Chainlink updates every 60-90s, so
                // get_price_at_offset's 5s tolerance almost always misses and falls
                // back to Binance USDT — contaminating the USD open with a USDT
                // price and causing systematic direction bias.
                let secs_since_start = mkt.window_seconds.saturating_sub(secs_rem);
                if let Some(price) = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
                    price_feeds
                        .set_window_open(&mkt.slug_prefix, window_ts, price)
                        .await;
                    info!(
                        market = %mkt.name,
                        window_ts,
                        open_price = %price,
                        secs_remaining = secs_rem,
                        secs_into_window = secs_since_start,
                        "New window — Chainlink (USD) open captured"
                    );
                }

                // Also capture Binance open for unbiased delta calculation.
                // USDT premium cancels out when both open and current use Binance.
                let binance_sym = mkt.binance_symbol.to_lowercase();
                let binance_open = if secs_since_start > 0 {
                    price_feeds.get_price_at_offset(&binance_sym, secs_since_start).await
                } else {
                    None
                };
                if let Some(bp) = binance_open {
                    price_feeds.set_binance_window_open(&mkt.slug_prefix, window_ts, bp).await;
                    info!(market = %mkt.name, binance_open = %bp, "Binance open captured (tick-buffer)");
                } else if let Some(bp) = price_feeds.get_price(&binance_sym).await {
                    price_feeds.set_binance_window_open(&mkt.slug_prefix, window_ts, bp).await;
                    info!(market = %mkt.name, binance_open = %bp, "Binance open captured (current price)");
                }

                // Resolve tokens for this window (best-effort)
                if mkt.is_hourly() {
                    // Clear previous hourly slug on window transition
                    if let Some(old_slug) = hs_local.remove(&mkt.name) {
                        tc_local.remove(&old_slug);
                    }
                    hourly_discovery.invalidate_cache().await;
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        hourly_discovery.get_current_market(&mkt.slug_prefix),
                    ).await {
                        Ok(Ok(Some(hm))) => {
                            info!(market = %mkt.name, slug = %hm.event_slug, end_time = hm.end_time, "Hourly market resolved");
                            tc_local.insert(hm.event_slug.clone(), hm.to_market_info());
                            hs_local.insert(mkt.name.clone(), hm.event_slug);
                        }
                        Ok(Ok(None)) => {
                            warn!(market = %mkt.name, "No active hourly market found (will retry)");
                        }
                        Ok(Err(e)) => {
                            warn!(market = %mkt.name, error = %e, "Hourly market discovery failed (will retry)");
                        }
                        Err(_) => {
                            warn!(market = %mkt.name, "Hourly market discovery timed out (will retry)");
                        }
                    }
                } else {
                    let slug = market::build_slug(&mkt.slug_prefix, window_ts);
                    let slug_clone = slug.clone();
                    match tokio::time::timeout(
                        Duration::from_secs(8),
                        market::resolve_market(&slug_clone, &clob_url),
                    ).await {
                        Ok(Ok(info)) => {
                            tc_local.insert(slug, info);
                        }
                        Ok(Err(e)) => {
                            warn!(market = %mkt.name, slug = %slug_clone, error = %e, "Token resolve failed (will retry)");
                        }
                        Err(_) => {
                            warn!(market = %mkt.name, slug = %slug_clone, "Token resolve timed out on window start (will retry)");
                        }
                    }
                }

                last_window_ts.insert(mkt.name.clone(), window_ts);
            } else {
                // Retry open price capture if it was missed at window start
                let wo_key_exists = price_feeds
                    .get_window_open(&mkt.slug_prefix, window_ts)
                    .await
                    .is_some();
                if !wo_key_exists {
                    if let Some(price) = price_feeds.get_market_price(&mkt.resolution_source, &mkt.chainlink_symbol, &mkt.binance_symbol).await {
                        price_feeds
                            .set_window_open(&mkt.slug_prefix, window_ts, price)
                            .await;
                        info!(
                            market = %mkt.name,
                            window_ts,
                            open_price = %price,
                            "Chainlink open set (late capture)"
                        );
                    }
                }
                let bwo_key_exists = price_feeds
                    .get_binance_window_open(&mkt.slug_prefix, window_ts)
                    .await
                    .is_some();
                if !bwo_key_exists {
                    let binance_sym = mkt.binance_symbol.to_lowercase();
                    if let Some(bp) = price_feeds.get_price(&binance_sym).await {
                        price_feeds.set_binance_window_open(&mkt.slug_prefix, window_ts, bp).await;
                        info!(market = %mkt.name, binance_open = %bp, "Binance open set (late capture)");
                    }
                }

                // priceToBeat is Chainlink price at exact window start (T-300).
                // No re-snap needed — the T-300 capture above is the correct reference.

                // Retry token resolution if missing
                if mkt.is_hourly() {
                    let has_slug = hs_local.contains_key(&mkt.name);
                    let has_token = has_slug
                        && tc_local.contains_key(hs_local.get(&mkt.name).unwrap());
                    let mkt_cutoff = mkt.effective_entry_cutoff(entry_cutoff_s);
                    if !has_token && secs_rem > mkt_cutoff && heartbeat_counter % 30 == 0 {
                        hourly_discovery.invalidate_cache().await;
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            hourly_discovery.get_current_market(&mkt.slug_prefix),
                        ).await {
                            Ok(Ok(Some(hm))) => {
                                info!(market = %mkt.name, slug = %hm.event_slug, "Hourly market resolved on retry");
                                tc_local.insert(hm.event_slug.clone(), hm.to_market_info());
                                hs_local.insert(mkt.name.clone(), hm.event_slug);
                            }
                            _ => {}
                        }
                    }
                } else {
                    let slug = market::build_slug(&mkt.slug_prefix, window_ts);
                    if !tc_local.contains_key(&slug) && secs_rem > entry_cutoff_s {
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            market::resolve_market(&slug, &clob_url),
                        ).await {
                            Ok(Ok(info)) => {
                                info!(market = %mkt.name, slug = %slug, "Token resolved on retry");
                                tc_local.insert(slug, info);
                            }
                            Ok(Err(_)) => {}
                            Err(_) => {
                                debug!(market = %mkt.name, slug = %slug, "Token retry timed out");
                            }
                        }
                    }
                }
            }
        }

        // Send Telegram notification for missed windows
        if !missed_windows.is_empty() {
            telegram.send_window_miss(&missed_windows).await.ok();
        }

        // Prune old window data periodically
        let now_ts = market::epoch_secs();
        price_feeds.prune_old_window_opens(now_ts, 3600).await;
        positions.prune_old(7200).await;

        // Clean expired token cache — only keep current + next window slugs
        {
            let mut active_slugs: Vec<String> = Vec::new();
            for mkt in config.enabled_markets() {
                if !mkt.is_hourly() {
                    let (wts, _) = market::current_window(mkt.window_seconds);
                    active_slugs.push(market::build_slug(&mkt.slug_prefix, wts));
                    active_slugs.push(market::build_slug(&mkt.slug_prefix, wts + mkt.window_seconds));
                }
            }
            let before = tc_local.len();
            tc_local.retain(|slug, _| {
                hs_local.values().any(|s| s == slug) || active_slugs.contains(slug)
            });
            let evicted = before.saturating_sub(tc_local.len());
            if evicted > 0 {
                debug!(evicted, remaining = tc_local.len(), "Pruned stale token cache entries");
            }
        }

        // ── Pre-resolve NEXT window tokens (avoid slow resolution during entry) ──
        for mkt in config.enabled_markets() {
            let (_, secs_rem) = market::current_window(mkt.window_seconds);
            let pre_resolve_at = (mkt.entry_start_s * 3).max(60);
            if secs_rem <= pre_resolve_at {
                if mkt.is_hourly() {
                    let has_token = hs_local.get(&mkt.name)
                        .map(|s| tc_local.contains_key(s))
                        .unwrap_or(false);
                    if !has_token {
                        hourly_discovery.invalidate_cache().await;
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            hourly_discovery.get_current_market(&mkt.slug_prefix),
                        ).await {
                            Ok(Ok(Some(hm))) => {
                                if !tc_local.contains_key(&hm.event_slug) {
                                    info!(market = %mkt.name, slug = %hm.event_slug, secs_until_close = secs_rem, "Pre-cached hourly market tokens");
                                    tc_local.insert(hm.event_slug.clone(), hm.to_market_info());
                                    hs_local.insert(mkt.name.clone(), hm.event_slug);
                                }
                            }
                            _ => {}
                        }
                    }
                } else {
                    let next_ts = market::next_window_ts(mkt.window_seconds);
                    let next_slug = market::build_slug(&mkt.slug_prefix, next_ts);
                    if !tc_local.contains_key(&next_slug) {
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            market::resolve_market(&next_slug, &clob_url),
                        ).await {
                            Ok(Ok(info)) => {
                                info!(market = %mkt.name, slug = %next_slug, secs_until_next = secs_rem, "Pre-cached NEXT window tokens");
                                tc_local.insert(next_slug, info);
                            }
                            Ok(Err(_)) | Err(_) => {}
                        }
                    }
                }
            }
        }

        // ── Phase 2: Run edge scanner ──
        let scan_result =
            scanner::scan_all_markets(config, price_feeds, &tc_local, positions, &hs_local).await;
        let opportunities = scan_result.opportunities;

        // Update last skip reasons for window-miss reporting
        for (name, reason) in &scan_result.skip_reasons {
            last_skip_reasons.insert(name.clone(), reason.clone());
        }

        // Recoverable orderbook conditions → enter "watching" mode instead of
        // sending a premature SKIP alert. The scanner already re-polls every
        // scan_interval_ms; this just changes how results are reported.
        for eval in &scan_result.evaluations {
            let is_recoverable = matches!(
                eval.result.as_str(),
                "SKIP_ORDERBOOK" | "SKIP_SETTLED" | "SKIP_SANITY" | "SKIP_PRICE" | "SKIP_EDGE"
            );
            if is_recoverable {
                if let Some(ref dir) = eval.direction {
                    let watch_key = format!("{}-{}", eval.market_name, eval.window_ts);
                    if let Some(entry) = watching_markets.get_mut(&watch_key) {
                        entry.1 += 1;
                    } else {
                        watching_markets.insert(watch_key, (eval.secs_remaining, 1));
                        telegram.send_watching(
                            &eval.market_name,
                            dir,
                            eval.delta_pct.unwrap_or(0.0),
                            eval.secs_remaining,
                            eval.detail.as_deref().unwrap_or(&eval.result),
                        ).await.ok();
                    }
                }
            }
        }

        // Log scanner evaluations to DB
        for eval in &scan_result.evaluations {
            if let Err(e) = db.insert_scanner_log(eval) {
                warn!(error = %e, market = %eval.market_name, "Failed to log scanner eval");
            }
            *scanner_result_counts.entry(eval.result.clone()).or_insert(0) += 1;
            // Track max delta per window for window_summary
            if let Some(delta) = eval.delta_pct {
                let key = format!("{}-{}", eval.market_name, eval.window_ts);
                let entry = window_max_delta.entry(key).or_insert((0.0, String::new()));
                if delta > entry.0 {
                    entry.0 = delta;
                    entry.1 = eval.direction.clone().unwrap_or_default();
                }
            }
        }

        // ── Record shadow trades for every evaluated window ──
        for eval in &scan_result.evaluations {
            if let (Some(ref dir), Some(delta)) = (&eval.direction, eval.delta_pct) {
                let shadow_key = format!("{}-{}", eval.market_name, eval.window_ts);
                if !shadow_recorded.contains(&shadow_key) {
                    let mkt_cfg = config.markets.iter().find(|m| m.name == eval.market_name);
                    let ws = mkt_cfg.map(|m| m.window_seconds).unwrap_or(300);
                    let open_str = eval.open_price.map(|d| d.to_string()).unwrap_or_default();
                    let best_ask_str = eval.best_ask.map(|d| d.to_string());
                    let skip = if eval.result != "OPPORTUNITY" {
                        eval.detail.as_deref().or(Some(eval.result.as_str()))
                    } else {
                        None
                    };
                    if let Err(e) = db.insert_shadow_trade(
                        &eval.market_name, eval.window_ts, ws,
                        &open_str, delta, dir,
                        best_ask_str.as_deref(),
                        eval.secs_remaining,
                        false,
                        skip,
                        eval.velocity_5s,
                        None,
                        eval.range_30s,
                        eval.signal_score,
                        eval.ob_imbalance,
                        eval.volume_ratio,
                    ) {
                        warn!(error = %e, market = %eval.market_name, "Failed to record shadow trade");
                    }
                    shadow_recorded.insert(shadow_key);
                }
            }
        }

        // ── Phase 3: Fire orders at best opportunities ──
        let mut orders_fired = 0;

        // Check global risk once
        let global_ok = {
            let st = state.read().await;
            match risk_manager.check_global(&st) {
                Ok(()) => {
                    risk_alert_sent.clear();
                    true
                }
                Err(ref veto) => {
                    let key = match veto {
                        risk::RiskVeto::DailyLossLimitReached => "daily_loss",
                        risk::RiskVeto::ConsecutiveLossPause => "consec_loss",
                        risk::RiskVeto::Paused { .. } => "paused",
                        risk::RiskVeto::InsufficientBankroll => "bankroll",
                        _ => "other",
                    };
                    if risk_alert_sent.insert(key.to_string()) {
                        let msg = match veto {
                            risk::RiskVeto::DailyLossLimitReached => format!(
                                "🛑 Daily Loss Limit Reached\n\n\
                                 Daily P&L: {}\n\
                                 Limit: ${:.2}\n\n\
                                 Trading halted until next day reset.",
                                st.daily_pnl,
                                config.bankroll.daily_loss_limit_usd
                            ),
                            risk::RiskVeto::ConsecutiveLossPause => format!(
                                "⚠️ Consecutive Loss Circuit Breaker\n\n\
                                 Consecutive losses: {}\n\
                                 Threshold: {}\n\
                                 Pause duration: {} minutes\n\n\
                                 Trading paused.",
                                st.consecutive_losses,
                                config.bankroll.consecutive_loss_pause,
                                config.bankroll.pause_duration_minutes
                            ),
                            risk::RiskVeto::Paused { seconds_remaining } => format!(
                                "⏸️ Trading Paused (Circuit Breaker)\n\n\
                                 Time remaining: {}m {}s",
                                seconds_remaining / 60,
                                seconds_remaining % 60
                            ),
                            risk::RiskVeto::InsufficientBankroll => format!(
                                "💸 Insufficient Bankroll\n\n\
                                 Current bankroll: {}\n\
                                 Minimum required: bet size\n\n\
                                 Trading halted — deposit funds.",
                                st.bankroll
                            ),
                            _ => format!("🚨 Risk Veto: {veto}"),
                        };
                        telegram.send_error(&msg).await.ok();
                    }
                    false
                }
            }
        };

        if global_ok {
            for (rank, opp) in opportunities.iter().enumerate() {
                if orders_fired >= config.scanner.max_orders_per_cycle {
                    break;
                }

                // Block trades until this market has completed at least one
                // full window boundary. The first window after startup uses a
                // "current price at boot" as the open, which differs from
                // Polymarket's "Price to Beat" and causes wrong-direction bets.
                if !warmed_up.contains(&opp.market_name) {
                    info!(
                        market = %opp.market_name,
                        direction = %opp.direction,
                        delta = opp.delta_pct,
                        "Skip order: market not warmed up (first window after restart — open price unreliable)"
                    );
                    continue;
                }

                // If this market was being watched, the orderbook just improved
                let watch_key = format!("{}-{}", opp.market_name, opp.window_ts);
                if let Some((started_secs, poll_count)) = watching_markets.remove(&watch_key) {
                    info!(
                        market = %opp.market_name,
                        direction = %opp.direction,
                        delta = opp.delta_pct,
                        best_ask = %opp.best_ask,
                        watched_for = %format!("~{}s", started_secs.saturating_sub(opp.seconds_remaining)),
                        polls = poll_count,
                        "Watching -> orderbook appeared! Placing order (polled {} times)", poll_count
                    );
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

                    let db_id = db.save_position(
                        &opp.market_name, &opp.asset, opp.window_seconds, opp.window_ts,
                        &opp.slug, &opp.direction.to_string(), &opp.token_id,
                        &opp.condition_id, opp.neg_risk, opp.edge_score,
                        &Decimal::try_from(opp.delta_pct).unwrap_or_default().to_string(),
                        &opp.suggested_entry.to_string(), &opp.suggested_entry.to_string(),
                        &opp.max_entry.to_string(), &opp.best_ask.to_string(),
                        &opp.contracts.to_string(), &opp.bet_size_usd.to_string(),
                        Some(&order_id), 0, &opp.open_price.to_string(),
                        "FILLED", Some(&opp.suggested_entry.to_string()),
                        Some(&opp.contracts.to_string()), "paper",
                    ).ok();

                    let pos = Position {
                        id: 0,
                        db_id,
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
                        submit_secs_before_close: None,
                        confirm_secs_before_close: None,
                        pipeline_ms: None,
                        is_early_limit: opp.is_early_limit,
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
                            None,
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
                    let traded_key = format!("{}-{}", opp.market_name, opp.window_ts);

                    if window_traded.get(&traded_key).copied().unwrap_or(false) {
                        debug!(market = %opp.market_name, window = opp.window_ts, "Already traded this window — skipping");
                        continue;
                    }

                    // Check 425 "Too Early" backoff — skip if we were recently rejected
                    if let Some(retry_at) = early_reject_until.get(&traded_key) {
                        if std::time::Instant::now() < *retry_at {
                            debug!(
                                market = %opp.market_name,
                                "Skipping order — 425 backoff active"
                            );
                            continue;
                        }
                        early_reject_until.remove(&traded_key);
                    }

                    let token_id_u256 = match U256::from_str(&opp.token_id) {
                        Ok(id) => id,
                        Err(e) => {
                            warn!(error = %e, market = %opp.market_name, "Invalid token ID");
                            continue;
                        }
                    };

                    let order_submit_utc = Utc::now();
                    let order_submit_epoch = market::epoch_secs();
                    let window_end_epoch = opp.window_ts + opp.window_seconds;
                    let secs_before_close = window_end_epoch.saturating_sub(order_submit_epoch);
                    info!(
                        market = %opp.market_name,
                        utc = %order_submit_utc.format("%H:%M:%S%.3f"),
                        epoch = order_submit_epoch,
                        window_end = window_end_epoch,
                        secs_before_close = secs_before_close,
                        price = %opp.suggested_entry,
                        contracts = %opp.contracts,
                        direction = %opp.direction,
                        "SUBMITTING order to CLOB"
                    );

                    let pipeline_timer = std::time::Instant::now();
                    match post_order!(opp.suggested_entry, opp.contracts, token_id_u256) {
                        Ok(oid) => {
                            let pipeline_elapsed_ms = pipeline_timer.elapsed().as_millis();
                            let posted_epoch = market::epoch_secs();
                            let secs_after_post = window_end_epoch.saturating_sub(posted_epoch);
                            let db_id = db.save_position(
                                &opp.market_name, &opp.asset, opp.window_seconds, opp.window_ts,
                                &opp.slug, &opp.direction.to_string(), &opp.token_id,
                                &opp.condition_id, opp.neg_risk, opp.edge_score,
                                &Decimal::try_from(opp.delta_pct).unwrap_or_default().to_string(),
                                &opp.suggested_entry.to_string(), &opp.suggested_entry.to_string(),
                                &opp.max_entry.to_string(), &opp.best_ask.to_string(),
                                &opp.contracts.to_string(), &opp.bet_size_usd.to_string(),
                                Some(&oid), 0, &opp.open_price.to_string(),
                                "PENDING", None, None, "live",
                            ).ok();

                            let pos = Position {
                                id: 0,
                                db_id,
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
                                submit_secs_before_close: Some(secs_before_close),
                                confirm_secs_before_close: Some(secs_after_post),
                                pipeline_ms: Some(pipeline_elapsed_ms),
                                is_early_limit: opp.is_early_limit,
                            };

                            let pos_id = positions.add(pos).await;

                            let order_type = if opp.is_early_limit { "EARLY_LIMIT" } else if opp.is_taker { "TAKER" } else { "MAKER" };
                            info!(
                                market = %opp.market_name,
                                order_id = %oid,
                                price = %opp.suggested_entry,
                                edge = opp.edge_score,
                                order_type,
                                pos_id,
                                utc_posted = %Utc::now().format("%H:%M:%S%.3f"),
                                secs_before_close = secs_after_post,
                                "ORDER CONFIRMED by CLOB"
                            );

                            telegram.send_order_submitted(
                                &opp.market_name, &opp.slug, &opp.direction.to_string(),
                                opp.suggested_entry, opp.contracts,
                                secs_after_post, pipeline_elapsed_ms, &oid,
                            ).await.ok();

                            orders_fired += 1;
                            window_traded.insert(traded_key.clone(), true);
                            let _ = db.mark_shadow_traded(&opp.market_name, opp.window_ts);

                            let (md, md_dir) = window_max_delta.get(&traded_key).cloned().unwrap_or((opp.delta_pct, opp.direction.to_string()));
                            let _ = db.upsert_window_summary(
                                &opp.market_name, opp.window_ts,
                                &opp.open_price.to_string(), &opp.open_price.to_string(),
                                md, Some(&md_dir), true, None,
                                Some(&opp.suggested_entry.to_string()), None, None,
                            );
                        }
                        Err(e) => {
                            let err_str = format!("{e}");
                            let fail_secs = window_end_epoch.saturating_sub(market::epoch_secs());

                            if err_str.contains("425") || err_str.to_lowercase().contains("too early") {
                                let already_backing_off = early_reject_until.contains_key(&traded_key);
                                // If this was an early entry (T>30), back off until the
                                // normal entry window instead of spamming every 5 seconds.
                                let backoff_secs = if fail_secs > 35 {
                                    fail_secs.saturating_sub(30)
                                } else {
                                    5
                                };
                                early_reject_until.insert(
                                    traded_key.clone(),
                                    std::time::Instant::now() + Duration::from_secs(backoff_secs),
                                );
                                warn!(
                                    market = %opp.market_name,
                                    secs_before_close = fail_secs,
                                    backoff_s = backoff_secs,
                                    "425 Too Early — will retry after backoff"
                                );
                                if !already_backing_off {
                                    telegram.send_order_failed(
                                        &opp.market_name, &opp.slug, &opp.direction.to_string(),
                                        opp.suggested_entry, fail_secs,
                                        &format!("425 Too Early — will retry at T-30 (backoff {backoff_secs}s)"),
                                    ).await.ok();
                                }
                            } else {
                                warn!(
                                    error = ?e,
                                    market = %opp.market_name,
                                    price = %opp.suggested_entry,
                                    utc = %Utc::now().format("%H:%M:%S%.3f"),
                                    secs_before_close = fail_secs,
                                    "Failed to post order"
                                );
                                window_traded.insert(traded_key.clone(), true);
                                telegram.send_order_failed(
                                    &opp.market_name, &opp.slug, &opp.direction.to_string(),
                                    opp.suggested_entry, fail_secs, &err_str,
                                ).await.ok();
                            }
                        }
                    }
                }
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
                        // Check for fill in the batch poll
                        if let Some((_, fill_price, fill_size)) =
                            fills.iter().find(|(id, _, _)| id == oid)
                        {
                            handle_live_fill(
                                pos, *fill_price, *fill_size, positions, &db,
                                &telegram, config,
                            ).await;
                            continue;
                        }

                        // Check for expiry (past window close)
                        let now = market::epoch_secs();
                        if now >= pos.window_ts + pos.window_seconds {
                            // RACE CONDITION FIX: wait a moment and poll fills ONE MORE TIME
                            // The order may have been matched right at the boundary.
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            let recheck_fills = poll_fills!();
                            if let Some((_, fill_price, fill_size)) =
                                recheck_fills.iter().find(|(id, _, _)| id == oid)
                            {
                                info!(
                                    market = %pos.market_name,
                                    order_id = %oid,
                                    fill_price = %fill_price,
                                    "Fill detected on recheck (race condition avoided)"
                                );
                                handle_live_fill(
                                    pos, *fill_price, *fill_size, positions, &db,
                                    &telegram, config,
                                ).await;
                                continue;
                            }

                            // Truly expired — cancel and clean up
                            if let Some(ref c) = client {
                                let _ = c.cancel_all_orders().await;
                            }
                            positions.mark_expired(pos.id).await;
                            if let Some(db_id) = pos.db_id {
                                let _ = db.remove_position(db_id);
                            }

                            let mut trade = build_trade_record_from_position(pos);
                            trade.skip_reason = Some("expired".into());
                            let _ = db.insert_trade(&trade);

                            info!(
                                market = %pos.market_name,
                                order_id = %oid,
                                last_price = %pos.current_price,
                                tightens = pos.tighten_count,
                                "Order expired unfilled (confirmed after recheck)"
                            );
                            continue;
                        }
                    }
                }

                // Tighten eligible positions (use smallest interval to get all candidates)
                let min_adjust = config
                    .enabled_markets()
                    .iter()
                    .filter_map(|m| m.adjust_interval_ms)
                    .min()
                    .unwrap_or(adjust_ms)
                    .min(adjust_ms);
                let to_tighten = positions.needs_tighten(min_adjust).await;
                for pos in &to_tighten {
                    // Per-market tighten interval check
                    let mkt_cfg = config.markets.iter().find(|m| m.name == pos.market_name);
                    let pos_adjust_ms = mkt_cfg
                        .map(|m| m.effective_adjust_interval_ms(adjust_ms))
                        .unwrap_or(adjust_ms);
                    let pos_interval = Duration::from_millis(pos_adjust_ms);
                    if tokio::time::Instant::now().duration_since(pos.last_adjust_at) < pos_interval {
                        continue;
                    }

                    let pos_cutoff = mkt_cfg
                        .map(|m| m.effective_entry_cutoff(entry_cutoff_s))
                        .unwrap_or(entry_cutoff_s);
                    let (_, secs_rem) = market::current_window(pos.window_seconds);
                    if secs_rem < pos_cutoff {
                        continue;
                    }

                    let pos_tighten_step = mkt_cfg
                        .map(|m| Decimal::try_from(m.effective_tighten_step(config.pricing.tighten_step)).unwrap_or(tighten_step))
                        .unwrap_or(tighten_step);

                    let fresh_ask =
                        orderbook::refresh_best_ask(&clob_url, &pos.token_id).await;
                    let new_price = {
                        let stepped = pos.current_price + pos_tighten_step;
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
                        match c.cancel_all_orders().await {
                            Ok(resp) => {
                                if !resp.not_canceled.is_empty() {
                                    warn!(
                                        not_canceled = ?resp.not_canceled,
                                        market = %pos.market_name,
                                        "Some orders could not be canceled (may already be matched)"
                                    );
                                }
                                debug!(
                                    canceled = resp.canceled.len(),
                                    not_canceled = resp.not_canceled.len(),
                                    market = %pos.market_name,
                                    "Cancel result during tighten"
                                );
                            }
                            Err(e) => {
                                warn!(error = ?e, market = %pos.market_name, "cancel_all_orders failed during tighten");
                            }
                        }
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
        let has_settlements = !to_settle.is_empty();
        for pos in &to_settle {
            positions.mark_settlement_started(pos.id).await;

            let pos_clone = pos.clone();
            let pos_db_id = pos.db_id;
            let db2 = db.clone();
            let tg2 = telegram.clone();
            let st2 = state.clone();
            let pt2 = positions.clone();
            let rpc = config.infra.polygon_rpc_url.clone();
            let pk = config.private_key.clone();
            let do_redeem = config.infra.auto_redeem;

            tokio::spawn(async move {
                let result = settle_position(pos_clone, db2.clone(), tg2, st2, pt2, &rpc, &pk, do_redeem)
                    .await;
                if result.is_ok() {
                    if let Some(db_id) = pos_db_id {
                        let _ = db2.remove_position(db_id);
                    }
                }
            });
        }

        // After settlements, check for bankroll drift against the CLOB cash
        // balance. Only LOG — don't overwrite, because cash balance excludes
        // unredeemed winnings and open positions.
        if has_settlements {
            if let Some(clob_cash) = fetch_clob_balance!() {
                let st = state.read().await;
                let drift = st.bankroll - clob_cash;
                if drift.abs() > dec!(1.00) {
                    warn!(
                        internal_bankroll = %st.bankroll,
                        clob_cash = %clob_cash,
                        drift = %drift,
                        "Bankroll vs CLOB cash drift (may include unredeemed wins)"
                    );
                }
            }
        }

        // ── Sync shared token caches back for shadow timing task ──
        *token_cache.write().await = tc_local;
        *hourly_slug_map.write().await = hs_local;

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
    _polygon_rpc_url: &str,
    _private_key: &str,
    _auto_redeem: bool,
) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(25)).await;

    let fill_price = pos.fill_price.unwrap_or(pos.current_price);
    let fill_size = pos.fill_size.unwrap_or(pos.contracts);

    let max_attempts = match pos.mode {
        BotMode::Paper => 10u32,
        BotMode::Live => 20,
    };

    let mut resolved: Option<bool> = None;
    for attempt in 0..max_attempts {
        match redeem::check_settlement(&pos.slug, &pos.token_id).await {
            Ok(Some(won)) => {
                resolved = Some(won);
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
        warn!(slug = %pos.slug, token_id = %pos.token_id, "Settlement unresolved after {max_attempts} attempts, defaulting to loss");
        false
    });
    let _outcome_source = if resolved.is_some() { "gamma-api" } else { "fallback" };

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
            won,
            pnl,
            bankroll,
            pos.initial_price,
            pos.tighten_count,
            pos.best_ask_at_entry,
            pos.edge_score,
            pos.delta_pct,
            &pos.mode.to_string(),
        )
        .await
        .ok();

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Live fill handler (extracted to avoid duplication between normal + recheck)
// ─────────────────────────────────────────────────────────────────────────────

async fn handle_live_fill(
    pos: &Position,
    fill_price: Decimal,
    fill_size: Decimal,
    positions: &PositionTracker,
    db: &Arc<TradeDb>,
    telegram: &Arc<TelegramNotifier>,
    config: &AppConfig,
) {
    positions.mark_filled(pos.id, fill_price, fill_size).await;

    if let Some(db_id) = pos.db_id {
        let _ = db.update_position_status(
            db_id,
            "FILLED",
            Some(&fill_price.to_string()),
            Some(&fill_size.to_string()),
        );
    }

    let mut trade = build_trade_record_from_position(pos);
    trade.filled = true;
    trade.fill_price = Some(fill_price);
    trade.final_price = fill_price;
    trade.contracts = fill_size;
    trade.bet_size_usd = fill_size * fill_price;
    if let Err(e) = db.insert_trade(&trade) {
        error!(error = %e, "Failed to log live fill");
    }

    let open_count = positions.open_count().await;
    telegram
        .send_fill_from_position(
            &pos.market_name,
            &pos.slug,
            &pos.direction.to_string(),
            &pos.mode.to_string(),
            fill_price,
            fill_size,
            pos.edge_score,
            pos.best_ask_at_entry,
            pos.delta_pct,
            pos.tighten_count,
            open_count,
            config.bankroll.max_concurrent_positions,
            pos.submit_secs_before_close.map(|s| telegram::OrderTiming {
                secs_before_submit: s,
                secs_before_confirm: pos.confirm_secs_before_close.unwrap_or(0),
                pipeline_ms: pos.pipeline_ms.unwrap_or(0),
            }).as_ref(),
        )
        .await
        .ok();

    info!(
        market = %pos.market_name,
        order_id = pos.order_id.as_deref().unwrap_or("?"),
        fill_price = %fill_price,
        fill_size = %fill_size,
        tightens = pos.tighten_count,
        "FILLED!"
    );
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
        market_type: opp.market_type.clone(),
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
        best_bid: opp.best_bid,
        spread: opp.spread,
        depth_at_ask: opp.depth_usd,
        fill_latency_ms: 0,
    }
}

fn infer_market_type(window_seconds: u64) -> &'static str {
    match window_seconds {
        300 => "5min",
        900 => "15min",
        3600 => "hourly",
        _ => "5min",
    }
}

fn build_trade_record_from_position(pos: &Position) -> TradeRecord {
    let fill_latency_ms = pos.placed_at.elapsed().as_millis() as i64;
    TradeRecord {
        id: None,
        timestamp: Utc::now(),
        market_name: pos.market_name.clone(),
        asset: pos.asset.clone(),
        market_type: infer_market_type(pos.window_seconds).to_string(),
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
        best_bid: Decimal::ZERO,
        spread: Decimal::ZERO,
        depth_at_ask: Decimal::ZERO,
        fill_latency_ms,
    }
}
