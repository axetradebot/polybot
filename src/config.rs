use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::collections::HashSet;
use std::path::Path;

use crate::types::{BotMode, SignatureType};

// ─── Top-level application config ────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct AppConfig {
    pub mode: BotMode,
    pub private_key: String,
    pub funder_address: Option<String>,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub api_passphrase: Option<String>,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub general: GeneralConfig,
    pub bankroll: BankrollConfig,
    pub pricing: PricingConfig,
    pub scanner: ScannerConfig,
    pub markets: Vec<MarketConfig>,
    pub telegram: TelegramConfig,
    pub infra: InfraConfig,
    /// Markets that should run in paper mode even when the bot is in live mode.
    pub paper_markets: HashSet<String>,
}

// ─── Section configs ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
pub struct GeneralConfig {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

fn default_mode() -> String { "paper".into() }
fn default_log_level() -> String { "info".into() }

#[derive(Debug, Clone, Deserialize)]
pub struct BankrollConfig {
    pub total: f64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_positions: usize,
    #[serde(default = "default_max_per_market")]
    pub max_per_market: usize,
    #[serde(default = "default_bet_size")]
    pub bet_size_usd: f64,
    #[serde(default = "default_daily_loss")]
    pub daily_loss_limit_usd: f64,
    #[serde(default = "default_consec_pause")]
    pub consecutive_loss_pause: u32,
    #[serde(default = "default_pause_min")]
    pub pause_duration_minutes: u64,
    #[serde(default)]
    pub dynamic_sizing: bool,
    #[serde(default = "default_min_bet_mult")]
    pub min_bet_multiplier: f64,
    #[serde(default = "default_max_bet_mult")]
    pub max_bet_multiplier: f64,
    #[serde(default = "default_baseline_signal")]
    pub baseline_signal: f64,
}

fn default_max_concurrent() -> usize { 6 }
fn default_max_per_market() -> usize { 2 }
fn default_bet_size() -> f64 { 5.0 }
fn default_daily_loss() -> f64 { 50.0 }
fn default_consec_pause() -> u32 { 5 }
fn default_pause_min() -> u64 { 15 }
fn default_min_bet_mult() -> f64 { 0.5 }
fn default_max_bet_mult() -> f64 { 2.0 }
fn default_baseline_signal() -> f64 { 0.10 }

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct PricingConfig {
    #[serde(default = "default_strategy")]
    pub strategy: String,
    #[serde(default = "default_undercut")]
    pub undercut_offset: f64,
    #[serde(default = "default_tighten")]
    pub tighten_step: f64,
    #[serde(default = "default_adjust_ms")]
    pub adjust_interval_ms: u64,
    #[serde(default = "default_min_entry")]
    pub min_entry_price: f64,
    #[serde(default = "default_cutoff")]
    pub entry_cutoff_s: u64,
    /// Delta threshold above which the bot takes the ask directly (taker order)
    /// instead of undercutting. Set to 0.0 to always use maker orders.
    #[serde(default)]
    pub taker_delta_threshold: f64,
    /// Expected win rate (0.0–1.0). When > 0, enforces a hard fill price ceiling
    /// of `breakeven_win_rate - profit_margin_pct` regardless of delta tiers.
    #[serde(default)]
    pub breakeven_win_rate: f64,
    #[serde(default = "default_profit_margin")]
    pub profit_margin_pct: f64,
}

impl PricingConfig {
    /// Hard ceiling on fill price to ensure profitability at the target win rate.
    /// Returns 1.0 (no cap) when breakeven_win_rate is 0 (disabled).
    pub fn max_profitable_price(&self) -> f64 {
        if self.breakeven_win_rate > 0.0 {
            (self.breakeven_win_rate - self.profit_margin_pct).max(0.30)
        } else {
            1.0
        }
    }
}

fn default_strategy() -> String { "orderbook_aware".into() }
fn default_undercut() -> f64 { 0.03 }
fn default_tighten() -> f64 { 0.01 }
fn default_adjust_ms() -> u64 { 2000 }
fn default_min_entry() -> f64 { 0.55 }
fn default_cutoff() -> u64 { 4 }
fn default_profit_margin() -> f64 { 0.05 }

#[derive(Debug, Clone, Deserialize)]
pub struct ScannerConfig {
    #[serde(default = "default_scan_ms")]
    pub scan_interval_ms: u64,
    #[serde(default = "default_min_edge")]
    pub min_edge_score: f64,
    #[serde(default = "default_max_orders")]
    pub max_orders_per_cycle: usize,
    #[serde(default = "default_weight_delta")]
    pub signal_weight_delta: f64,
    #[serde(default)]
    pub signal_weight_velocity: f64,
    #[serde(default)]
    pub signal_weight_volatility: f64,
    #[serde(default)]
    pub signal_weight_accel: f64,
    #[serde(default)]
    pub min_volatility_pct: f64,
    #[serde(default)]
    pub require_acceleration: bool,
    #[serde(default)]
    pub min_signal_score: f64,
    /// Minimum orderbook imbalance for early entries (0.0 = disabled).
    /// Imbalance = winner_depth / (winner_depth + loser_depth).
    /// Below this threshold, early entries are skipped.
    #[serde(default = "default_min_ob_imbalance")]
    pub min_ob_imbalance: f64,
}

fn default_min_ob_imbalance() -> f64 { 0.5 }

fn default_scan_ms() -> u64 { 1000 }
fn default_min_edge() -> f64 { 0.05 }
fn default_max_orders() -> usize { 3 }
fn default_weight_delta() -> f64 { 1.0 }

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct MarketConfig {
    pub name: String,
    pub asset: String,
    #[serde(default = "default_market_type")]
    pub market_type: String,
    pub window_seconds: u64,
    pub slug_prefix: String,
    pub binance_symbol: String,
    #[serde(default)]
    pub chainlink_symbol: String,
    /// Bybit inverse perpetual symbol (e.g., "BTCUSD") for USD price feed.
    /// If set, used for direction determination instead of Chainlink.
    #[serde(default)]
    pub bybit_symbol: String,
    #[serde(default = "default_resolution_source")]
    pub resolution_source: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Per-market mode override: "paper" | "live".
    /// If omitted, inherits from [general].mode.
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default = "default_entry_start")]
    pub entry_start_s: u64,
    #[serde(default)]
    pub entry_cutoff_s: Option<u64>,
    #[serde(default = "default_min_delta")]
    pub min_delta_pct: f64,
    #[serde(default = "default_max_entry_mkt")]
    pub max_entry_price: f64,
    pub delta_tiers: Vec<[f64; 2]>,
    #[serde(default)]
    pub undercut_offset: Option<f64>,
    #[serde(default)]
    pub tighten_step: Option<f64>,
    #[serde(default)]
    pub adjust_interval_ms: Option<u64>,
    /// Optional early entry window: start scanning this many seconds before close
    /// with a higher delta requirement. 0 = disabled.
    #[serde(default)]
    pub early_entry_start_s: u64,
    #[serde(default = "default_early_delta")]
    pub early_entry_min_delta_pct: f64,
    /// Delta tiers for early entries (T-120). Uses orderbook-aware pricing
    /// with conservative ceilings. Empty = early entries disabled.
    #[serde(default)]
    pub early_delta_tiers: Vec<[f64; 2]>,
}

fn default_true() -> bool { true }
fn default_market_type() -> String { "5min".into() }
fn default_resolution_source() -> String { "chainlink".into() }
fn default_entry_start() -> u64 { 20 }
fn default_min_delta() -> f64 { 0.07 }
fn default_max_entry_mkt() -> f64 { 0.85 }
fn default_early_delta() -> f64 { 0.20 }

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct TelegramConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub on_trade: bool,
    #[serde(default)]
    pub on_error: bool,
    #[serde(default)]
    pub daily_summary: bool,
    #[serde(default)]
    pub verbose_skips: bool,
    #[serde(default = "default_true")]
    pub on_fill: bool,
    #[serde(default = "default_true")]
    pub on_settlement: bool,
    #[serde(default = "default_true")]
    pub on_order_placed: bool,
    #[serde(default = "default_true")]
    pub on_startup: bool,
    #[serde(default)]
    pub on_watching: bool,
    #[serde(default)]
    pub on_window_miss: bool,
    #[serde(default)]
    pub on_heartbeat: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct InfraConfig {
    #[serde(default = "default_binance_ws")]
    pub binance_ws_base: String,
    #[serde(default = "default_chainlink_ws")]
    pub chainlink_ws_url: String,
    #[serde(default = "default_bybit_ws")]
    pub bybit_ws_url: String,
    pub polymarket_clob_url: String,
    #[serde(default = "default_rpc")]
    pub polygon_rpc_url: String,
    #[serde(default = "default_chain_id")]
    pub polygon_chain_id: u64,
    #[serde(default = "default_sig_type")]
    pub signature_type: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
    #[serde(default)]
    pub auto_redeem: bool,
}

fn default_binance_ws() -> String { "wss://stream.binance.com:9443/ws".into() }
fn default_chainlink_ws() -> String { "wss://ws-live-data.polymarket.com".into() }
fn default_bybit_ws() -> String { "wss://stream.bybit.com/v5/public/inverse".into() }
fn default_rpc() -> String { "https://polygon-rpc.com".into() }
fn default_chain_id() -> u64 { 137 }
fn default_sig_type() -> String { "GnosisSafe".into() }
fn default_db_path() -> String { "./trades.db".into() }

// ─── TOML deserialization target ─────────────────────────────────────────────

#[derive(Deserialize)]
struct TomlConfig {
    general: GeneralConfig,
    bankroll: BankrollConfig,
    pricing: PricingConfig,
    scanner: ScannerConfig,
    markets: Vec<MarketConfig>,
    telegram: TelegramConfig,
    infrastructure: InfraConfig,
}

// ─── AppConfig implementation ────────────────────────────────────────────────

impl AppConfig {
    pub fn load(config_path: &str) -> Result<Self> {
        dotenvy::dotenv().ok();
        dotenvy::from_filename(".env.local").ok();

        let toml_str = std::fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {config_path}"))?;
        let toml_cfg: TomlConfig =
            toml::from_str(&toml_str).context("Failed to parse config.toml")?;

        validate_markets(&toml_cfg.markets)?;

        let mode: BotMode = std::env::var("BOT_MODE")
            .unwrap_or_else(|_| toml_cfg.general.mode.clone())
            .parse()
            .context("Invalid BOT_MODE")?;

        let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .context("POLYMARKET_PRIVATE_KEY must be set")?;

        Ok(AppConfig {
            mode,
            private_key,
            funder_address: std::env::var("POLYMARKET_FUNDER_ADDRESS").ok(),
            api_key: non_empty_env("POLY_API_KEY"),
            api_secret: non_empty_env("POLY_API_SECRET"),
            api_passphrase: non_empty_env("POLY_API_PASSPHRASE"),
            telegram_bot_token: non_empty_env("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id: non_empty_env("TELEGRAM_CHAT_ID"),
            general: toml_cfg.general,
            bankroll: toml_cfg.bankroll,
            pricing: toml_cfg.pricing,
            scanner: toml_cfg.scanner,
            markets: toml_cfg.markets,
            telegram: toml_cfg.telegram,
            infra: toml_cfg.infrastructure,
            paper_markets: HashSet::new(),
        })
    }

    pub fn apply_cli_overrides(&mut self, cli: &crate::cli::Cli) {
        if let Some(m) = &cli.mode {
            if let Ok(mode) = m.parse() {
                self.mode = mode;
            }
        }
        if let Some(v) = cli.bet_size {
            self.bankroll.bet_size_usd = v;
        }
        if let Some(v) = cli.max_positions {
            self.bankroll.max_concurrent_positions = v;
        }

        // --markets: only enable listed markets
        if let Some(ref list) = cli.markets {
            let allowed: HashSet<&str> = list.split(',').map(str::trim).collect();
            for m in &mut self.markets {
                m.enabled = allowed.contains(m.name.as_str());
            }
        }

        // --disable-market: disable specific markets
        if let Some(ref list) = cli.disable_market {
            let disabled: HashSet<&str> = list.split(',').map(str::trim).collect();
            for m in &mut self.markets {
                if disabled.contains(m.name.as_str()) {
                    m.enabled = false;
                }
            }
        }

        // --paper-markets: run specific markets in paper mode even in live
        if let Some(ref list) = cli.paper_markets {
            self.paper_markets = list.split(',').map(|s| s.trim().to_string()).collect();
        }
    }

    pub fn enabled_markets(&self) -> Vec<&MarketConfig> {
        self.markets.iter().filter(|m| m.enabled).collect()
    }

    /// Unique Binance symbols across all enabled markets (used for fallback feed).
    pub fn binance_symbols(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.enabled_markets()
            .iter()
            .filter_map(|m| {
                if seen.insert(m.binance_symbol.clone()) {
                    Some(m.binance_symbol.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Unique Chainlink symbols across all enabled markets (primary price source).
    pub fn chainlink_symbols(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        self.enabled_markets()
            .iter()
            .filter(|m| !m.chainlink_symbol.is_empty())
            .filter_map(|m| {
                if seen.insert(m.chainlink_symbol.clone()) {
                    Some(m.chainlink_symbol.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Build a mapping from Chainlink symbol → Binance symbol for fallback.
    pub fn fallback_symbol_map(&self) -> Vec<(String, String)> {
        let mut seen = HashSet::new();
        self.enabled_markets()
            .iter()
            .filter(|m| !m.chainlink_symbol.is_empty())
            .filter_map(|m| {
                let key = m.chainlink_symbol.to_lowercase();
                if seen.insert(key.clone()) {
                    Some((key, m.binance_symbol.to_lowercase()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Resolve the effective mode for a market.
    /// Priority: CLI --paper-markets > per-market mode > global mode.
    pub fn is_market_paper(&self, market_name: &str) -> bool {
        // CLI override wins
        if self.paper_markets.contains(market_name) {
            return true;
        }
        // Per-market config override
        if let Some(mkt) = self.markets.iter().find(|m| m.name == market_name) {
            if let Some(ref m) = mkt.mode {
                return m.eq_ignore_ascii_case("paper");
            }
        }
        // Fall through to global mode
        self.mode == BotMode::Paper
    }

    pub fn needs_clob_client(&self) -> bool {
        self.enabled_markets().iter().any(|m| !self.is_market_paper(&m.name))
    }

    pub fn signature_type(&self) -> Result<SignatureType> {
        self.infra.signature_type.parse()
    }

    pub fn bet_size_decimal(&self) -> Decimal {
        Decimal::try_from(self.bankroll.bet_size_usd).unwrap_or(Decimal::from(5))
    }

    pub fn bankroll_decimal(&self) -> Decimal {
        Decimal::try_from(self.bankroll.total).unwrap_or(Decimal::from(500))
    }

    pub fn daily_loss_limit_decimal(&self) -> Decimal {
        Decimal::try_from(self.bankroll.daily_loss_limit_usd).unwrap_or(Decimal::from(50))
    }

    pub fn db_path(&self) -> &Path {
        Path::new(&self.infra.db_path)
    }

    pub fn telegram_enabled(&self) -> bool {
        self.telegram.enabled
            && self.telegram_bot_token.is_some()
            && self.telegram_chat_id.is_some()
    }
}

impl MarketConfig {
    /// Max entry price allowed for a given delta, using per-market delta tiers.
    pub fn max_price_for_delta(&self, delta_pct: f64) -> f64 {
        let mut ceiling = self.max_entry_price;
        for tier in &self.delta_tiers {
            if delta_pct >= tier[0] {
                ceiling = tier[1];
            }
        }
        ceiling.min(self.max_entry_price)
    }

    /// Max entry price for early (T-120) entries. Returns None if early tiers
    /// are empty (early entries disabled) or delta doesn't meet any tier.
    pub fn max_price_for_early_delta(&self, delta_pct: f64) -> Option<f64> {
        if self.early_delta_tiers.is_empty() {
            return None;
        }
        let mut ceiling: Option<f64> = None;
        for tier in &self.early_delta_tiers {
            if delta_pct >= tier[0] {
                ceiling = Some(tier[1]);
            }
        }
        ceiling
    }

    pub fn early_entries_enabled(&self) -> bool {
        self.early_entry_start_s > 0 && !self.early_delta_tiers.is_empty()
    }

    /// Seconds before close at which we start watching (entry_start_s + buffer).
    pub fn watch_start_s(&self) -> u64 {
        self.entry_start_s + 2
    }

    pub fn is_hourly(&self) -> bool {
        self.market_type == "hourly"
    }

    pub fn effective_entry_cutoff(&self, global: u64) -> u64 {
        self.entry_cutoff_s.unwrap_or(global)
    }

    pub fn effective_undercut_offset(&self, global: f64) -> f64 {
        self.undercut_offset.unwrap_or(global)
    }

    pub fn effective_tighten_step(&self, global: f64) -> f64 {
        self.tighten_step.unwrap_or(global)
    }

    pub fn effective_adjust_interval_ms(&self, global: u64) -> u64 {
        self.adjust_interval_ms.unwrap_or(global)
    }
}

// ─── Validation ──────────────────────────────────────────────────────────────

fn validate_markets(markets: &[MarketConfig]) -> Result<()> {
    if markets.is_empty() {
        bail!("At least one [[markets]] entry is required");
    }
    for (i, m) in markets.iter().enumerate() {
        if m.name.is_empty() {
            bail!("Market {i}: name cannot be empty");
        }
        if m.window_seconds != 300 && m.window_seconds != 900 && m.window_seconds != 3600 {
            bail!("Market '{}': window_seconds must be 300, 900, or 3600", m.name);
        }
        if m.delta_tiers.is_empty() {
            bail!("Market '{}': delta_tiers must have at least one entry", m.name);
        }
        for (j, tier) in m.delta_tiers.iter().enumerate() {
            if tier[0] < 0.01 || tier[1] < 0.30 || tier[1] > 0.99 {
                bail!("Market '{}' delta_tier {j}: invalid values {:?}", m.name, tier);
            }
        }
        for (j, tier) in m.early_delta_tiers.iter().enumerate() {
            if tier[0] < 0.01 || tier[1] < 0.20 || tier[1] > 0.99 {
                bail!("Market '{}' early_delta_tier {j}: invalid values {:?}", m.name, tier);
            }
        }
        if m.entry_start_s < 5 {
            bail!("Market '{}': entry_start_s must be >= 5", m.name);
        }
    }
    Ok(())
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
