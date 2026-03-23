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

    // ── Diagnostic mode ──
    if cli.diagnose {
        return run_diagnostics(&config).await;
    }

    let enabled = config.enabled_markets();
    let symbols = config.binance_symbols();

    let boot_utc = Utc::now();
    let boot_epoch = market::epoch_secs();
    info!(
        global_mode = %config.mode,
        bankroll = %config.bankroll.total,
        symbols = ?symbols,
        system_utc = %boot_utc.format("%Y-%m-%d %H:%M:%S%.3f UTC"),
        system_epoch = boot_epoch,
        "Multi-market scanner bot starting (clock source: OS SystemTime)"
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

    let risk_manager = RiskManager::new(&config);
    let verbose = cli.verbose || config.telegram.verbose_skips;

    // Spawn Binance combined price feed
    price_feeds.spawn_binance_feed(symbols.clone(), &config.infra.binance_ws_base);

    // Wait for ALL symbols to have fresh prices before starting
    info!("Waiting for initial price data from Binance...");
    let price_wait_start = tokio::time::Instant::now();
    loop {
        let mut all_fresh = true;
        for sym in &symbols {
            if !price_feeds.has_fresh_price(sym).await {
                all_fresh = false;
                break;
            }
        }
        if all_fresh {
            for sym in &symbols {
                if let Some(p) = price_feeds.get_price(sym).await {
                    info!(symbol = %sym, price = %p, "Initial price received");
                }
            }
            break;
        }
        if price_wait_start.elapsed() > Duration::from_secs(30) {
            warn!("Timed out waiting for all prices, starting with available data");
            for sym in &symbols {
                if let Some(p) = price_feeds.get_price(sym).await {
                    info!(symbol = %sym, price = %p, "Price available");
                } else {
                    warn!(symbol = %sym, "No price data yet");
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
                            if o.id == *oid && is_matched && has_size {
                                info!(
                                    order_id = %o.id,
                                    status = ?o.status,
                                    size_matched = %o.size_matched,
                                    price = %o.price,
                                    "poll_fills: confirmed fill via orders endpoint"
                                );
                                fills.push((o.id.clone(), o.price, o.size_matched));
                                seen_ids.insert(o.id.clone());
                            } else if o.id == *oid && has_size && !is_matched {
                                warn!(
                                    order_id = %o.id,
                                    status = ?o.status,
                                    size_matched = %o.size_matched,
                                    "poll_fills: size_matched>0 but status is NOT Matched — ignoring (false fill)"
                                );
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
    let mut token_cache: HashMap<String, MarketInfo> = HashMap::new();
    let mut window_traded: HashMap<String, bool> = HashMap::new();
    let mut last_skip_reasons: HashMap<String, String> = HashMap::new();
    let mut skip_notified: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut shadow_recorded: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut watching_markets: HashMap<String, u64> = HashMap::new(); // "market-window_ts" -> secs_remaining when watching started
    let mut window_max_delta: HashMap<String, (f64, String)> = HashMap::new(); // key -> (max_delta, direction)
    let mut last_analytics_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let scan_interval = Duration::from_millis(config.scanner.scan_interval_ms);
    let adjust_ms = config.pricing.adjust_interval_ms;
    let tighten_step = Decimal::try_from(config.pricing.tighten_step).unwrap_or(dec!(0.01));
    let entry_cutoff_s = config.pricing.entry_cutoff_s;
    let clob_url = config.infra.polymarket_clob_url.clone();

    // Hourly market discovery
    let hourly_discovery = hourly_discovery::HourlyDiscovery::new(&clob_url);
    let mut hourly_slug_map: HashMap<String, String> = HashMap::new();

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
    let hb_symbols = config.binance_symbols();

    loop {
        let cycle_start = tokio::time::Instant::now();
        heartbeat_counter += 1;
        let mut missed_windows: Vec<(String, String)> = Vec::new();

        // Log heartbeat every 60 seconds
        if heartbeat_counter % 60 == 0 {
            let open_pos = positions.open_count().await;
            let ws_msgs = crate::feeds::binance::messages_received();
            let st = state.read().await;
            let clob_bal = fetch_clob_balance!();
            let mut price_ages = Vec::new();
            for sym in &hb_symbols {
                if let Some(age) = price_feeds.price_age_ms(sym).await {
                    price_ages.push(format!("{}={}ms", sym, age));
                } else {
                    price_ages.push(format!("{}=NONE", sym));
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

        // Telegram heartbeat every 15 minutes
        if heartbeat_counter % 900 == 0 {
            let open_pos = positions.open_count().await;
            let st = state.read().await;
            let hb_clob_bal = fetch_clob_balance!();
            let mut snapshots = Vec::new();
            for mkt in config.enabled_markets() {
                let current_price = price_feeds.get_price(&mkt.binance_symbol).await.unwrap_or_default();
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
                    let key = format!("{}-{}", mkt.name, old_ts);
                    let traded = window_traded.get(&key).copied().unwrap_or(false);
                    let old_open = price_feeds.get_window_open(&mkt.slug_prefix, old_ts).await.unwrap_or_default();
                    let current = price_feeds.get_price(&mkt.binance_symbol).await.unwrap_or(old_open);

                    let watch_key = format!("{}-{}", mkt.name, old_ts);
                    let was_watching = watching_markets.remove(&watch_key);

                    if !traded {
                        let base_reason = last_skip_reasons
                            .get(&mkt.name)
                            .cloned()
                            .unwrap_or_else(|| "No opportunity found".into());
                        let reason = if let Some(started_secs) = was_watching {
                            format!("Watched ~{}s — {}", started_secs, base_reason)
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

                    // Settle shadow trade: determine actual BTC direction from open→close
                    let actual_dir = if current > old_open {
                        "UP"
                    } else if current < old_open {
                        "DOWN"
                    } else {
                        "FLAT"
                    };
                    if let Err(e) = db.settle_shadow_trade(
                        &mkt.name, old_ts, &current.to_string(), actual_dir,
                    ) {
                        warn!(error = %e, market = %mkt.name, "Failed to settle shadow trade");
                    }
                    shadow_recorded.remove(&format!("{}-{}", mkt.name, old_ts));

                    window_traded.remove(&key);
                }
                // Clear skip reason and skip notifications for fresh window
                last_skip_reasons.remove(&mkt.name);
                skip_notified.retain(|k| !k.starts_with(&mkt.name));

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
                if mkt.is_hourly() {
                    // Clear previous hourly slug on window transition
                    if let Some(old_slug) = hourly_slug_map.remove(&mkt.name) {
                        token_cache.remove(&old_slug);
                    }
                    hourly_discovery.invalidate_cache().await;
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        hourly_discovery.get_current_market(&mkt.slug_prefix),
                    ).await {
                        Ok(Ok(Some(hm))) => {
                            info!(market = %mkt.name, slug = %hm.event_slug, end_time = hm.end_time, "Hourly market resolved");
                            token_cache.insert(hm.event_slug.clone(), hm.to_market_info());
                            hourly_slug_map.insert(mkt.name.clone(), hm.event_slug);
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
                            token_cache.insert(slug, info);
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
                    if let Some(price) = price_feeds.get_price(&mkt.binance_symbol).await {
                        price_feeds
                            .set_window_open(&mkt.slug_prefix, window_ts, price)
                            .await;
                        info!(
                            market = %mkt.name,
                            window_ts,
                            open_price = %price,
                            "Open price set (late capture)"
                        );
                    }
                }

                // Retry token resolution if missing
                if mkt.is_hourly() {
                    let has_slug = hourly_slug_map.contains_key(&mkt.name);
                    let has_token = has_slug
                        && token_cache.contains_key(hourly_slug_map.get(&mkt.name).unwrap());
                    let mkt_cutoff = mkt.effective_entry_cutoff(entry_cutoff_s);
                    if !has_token && secs_rem > mkt_cutoff && heartbeat_counter % 30 == 0 {
                        hourly_discovery.invalidate_cache().await;
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            hourly_discovery.get_current_market(&mkt.slug_prefix),
                        ).await {
                            Ok(Ok(Some(hm))) => {
                                info!(market = %mkt.name, slug = %hm.event_slug, "Hourly market resolved on retry");
                                token_cache.insert(hm.event_slug.clone(), hm.to_market_info());
                                hourly_slug_map.insert(mkt.name.clone(), hm.event_slug);
                            }
                            _ => {}
                        }
                    }
                } else {
                    let slug = market::build_slug(&mkt.slug_prefix, window_ts);
                    if !token_cache.contains_key(&slug) && secs_rem > entry_cutoff_s {
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            market::resolve_market(&slug, &clob_url),
                        ).await {
                            Ok(Ok(info)) => {
                                info!(market = %mkt.name, slug = %slug, "Token resolved on retry");
                                token_cache.insert(slug, info);
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
            let before = token_cache.len();
            token_cache.retain(|slug, _| {
                hourly_slug_map.values().any(|s| s == slug) || active_slugs.contains(slug)
            });
            let evicted = before.saturating_sub(token_cache.len());
            if evicted > 0 {
                debug!(evicted, remaining = token_cache.len(), "Pruned stale token cache entries");
            }
        }

        // ── Pre-resolve NEXT window tokens (avoid slow resolution during entry) ──
        for mkt in config.enabled_markets() {
            let (_, secs_rem) = market::current_window(mkt.window_seconds);
            let pre_resolve_at = (mkt.entry_start_s * 3).max(60);
            if secs_rem <= pre_resolve_at {
                if mkt.is_hourly() {
                    let has_token = hourly_slug_map.get(&mkt.name)
                        .map(|s| token_cache.contains_key(s))
                        .unwrap_or(false);
                    if !has_token {
                        hourly_discovery.invalidate_cache().await;
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            hourly_discovery.get_current_market(&mkt.slug_prefix),
                        ).await {
                            Ok(Ok(Some(hm))) => {
                                if !token_cache.contains_key(&hm.event_slug) {
                                    info!(market = %mkt.name, slug = %hm.event_slug, secs_until_close = secs_rem, "Pre-cached hourly market tokens");
                                    token_cache.insert(hm.event_slug.clone(), hm.to_market_info());
                                    hourly_slug_map.insert(mkt.name.clone(), hm.event_slug);
                                }
                            }
                            _ => {}
                        }
                    }
                } else {
                    let next_ts = market::next_window_ts(mkt.window_seconds);
                    let next_slug = market::build_slug(&mkt.slug_prefix, next_ts);
                    if !token_cache.contains_key(&next_slug) {
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            market::resolve_market(&next_slug, &clob_url),
                        ).await {
                            Ok(Ok(info)) => {
                                info!(market = %mkt.name, slug = %next_slug, secs_until_next = secs_rem, "Pre-cached NEXT window tokens");
                                token_cache.insert(next_slug, info);
                            }
                            Ok(Err(_)) | Err(_) => {}
                        }
                    }
                }
            }
        }

        // ── Phase 2: Run edge scanner ──
        let scan_result =
            scanner::scan_all_markets(config, price_feeds, &token_cache, positions, &hourly_slug_map).await;
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
                    if !watching_markets.contains_key(&watch_key) {
                        watching_markets.insert(watch_key, eval.secs_remaining);
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
            risk_manager.check_global(&st).is_ok()
        };

        if global_ok {
            for (rank, opp) in opportunities.iter().enumerate() {
                if orders_fired >= config.scanner.max_orders_per_cycle {
                    break;
                }

                // If this market was being watched, the orderbook just improved
                let watch_key = format!("{}-{}", opp.market_name, opp.window_ts);
                if let Some(started_secs) = watching_markets.remove(&watch_key) {
                    info!(
                        market = %opp.market_name,
                        direction = %opp.direction,
                        delta = opp.delta_pct,
                        best_ask = %opp.best_ask,
                        watched_for = %format!("~{}s", started_secs.saturating_sub(opp.seconds_remaining)),
                        "Watching -> orderbook appeared! Placing order"
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
                            };

                            let pos_id = positions.add(pos).await;

                            let order_type = if opp.is_taker { "TAKER" } else { "MAKER" };
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
                        }
                        Err(e) => {
                            let fail_secs = window_end_epoch.saturating_sub(market::epoch_secs());
                            warn!(
                                error = ?e,
                                market = %opp.market_name,
                                price = %opp.suggested_entry,
                                utc = %Utc::now().format("%H:%M:%S%.3f"),
                                secs_before_close = fail_secs,
                                "Failed to post order"
                            );
                            telegram.send_order_failed(
                                &opp.market_name, &opp.slug, &opp.direction.to_string(),
                                opp.suggested_entry, fail_secs, &format!("{e}"),
                            ).await.ok();
                        }
                    }
                }

                orders_fired += 1;
                let traded_key = format!("{}-{}", opp.market_name, opp.window_ts);
                window_traded.insert(traded_key.clone(), true);
                let _ = db.mark_shadow_traded(&opp.market_name, opp.window_ts);

                // Write window_summary for the traded window
                let (md, md_dir) = window_max_delta.get(&traded_key).cloned().unwrap_or((opp.delta_pct, opp.direction.to_string()));
                let _ = db.upsert_window_summary(
                    &opp.market_name, opp.window_ts,
                    &opp.open_price.to_string(), &opp.open_price.to_string(),
                    md, Some(&md_dir), true, None,
                    Some(&opp.suggested_entry.to_string()), None, None,
                );
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
