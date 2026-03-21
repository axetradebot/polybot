use anyhow::{bail, Context, Result};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::path::Path;

use crate::types::{BotMode, SignatureType};

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
    pub trading: TradingConfig,
    pub signal: SignalConfig,
    pub infra: InfraConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradingConfig {
    pub starting_bankroll: f64,
    pub max_bet_usd: f64,
    pub daily_loss_limit_usd: f64,
    pub consecutive_loss_pause: u32,
    pub pause_duration_minutes: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SignalConfig {
    pub min_delta_pct: f64,
    pub max_entry_price: f64,
    pub order_lifetime_ms: u64,
    pub delta_pricing: DeltaPricingConfig,
    pub sweep: SweepConfig,
    #[serde(default)]
    pub entry_tiers: EntryTiersConfig,
    #[serde(default)]
    pub single_entry: SingleEntryConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeltaPricingConfig {
    pub tiers: Vec<[f64; 3]>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SweepConfig {
    /// Seconds before window close to begin sweep.
    pub start_s: u64,
    /// Minimum absolute BTC delta % to trigger a sweep.
    pub min_delta_pct: f64,
    /// First bid offset from fair value (negative = cheaper, more profit).
    pub start_offset: f64,
    /// Last bid offset from fair value (positive = above fair value, ensures fill).
    pub end_offset: f64,
    /// Number of price steps in the sweep.
    pub steps: u32,
}

/// A single entry tier: fires at T-`time_before_close_s` with `price_offset` from fair value.
#[derive(Debug, Clone, Deserialize)]
pub struct EntryTier {
    pub name: String,
    pub time_before_close_s: u64,
    pub price_offset: f64,
    pub min_delta_pct: f64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct EntryTiersConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_watch_start")]
    pub watch_start_s: u64,
    #[serde(default)]
    pub tiers: Vec<EntryTier>,
}

fn default_watch_start() -> u64 { 30 }

#[derive(Debug, Clone, Deserialize)]
pub struct SingleEntryConfig {
    #[serde(default = "default_single_entry_time")]
    pub entry_time_before_close_s: u64,
}

fn default_single_entry_time() -> u64 { 10 }

impl Default for SingleEntryConfig {
    fn default() -> Self {
        Self { entry_time_before_close_s: 10 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct InfraConfig {
    pub binance_ws_url: String,
    pub polymarket_clob_url: String,
    pub polymarket_ws_url: String,
    pub polygon_rpc_url: String,
    pub polygon_chain_id: u64,
    pub signature_type: String,
    pub db_path: String,
    #[serde(default)]
    pub auto_redeem: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct LoggingConfig {
    pub level: String,
    pub telegram_enabled: bool,
    pub telegram_on_trade: bool,
    pub telegram_on_error: bool,
    pub telegram_daily_summary: bool,
}

#[derive(Deserialize)]
struct TomlConfig {
    trading: TradingConfig,
    signal: SignalConfig,
    infrastructure: InfraConfig,
    logging: LoggingConfig,
}

impl AppConfig {
    pub fn load(config_path: &str) -> Result<Self> {
        dotenvy::dotenv().ok();
        dotenvy::from_filename(".env.local").ok();

        let toml_str = std::fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {config_path}"))?;
        let toml_cfg: TomlConfig =
            toml::from_str(&toml_str).context("Failed to parse config.toml")?;

        // Validate sweep config
        validate_sweep(&toml_cfg.signal.sweep)
            .context("Invalid [signal.sweep] configuration")?;

        // Validate entry tiers (if present)
        if toml_cfg.signal.entry_tiers.enabled {
            validate_entry_tiers(&toml_cfg.signal.entry_tiers)
                .context("Invalid [signal.entry_tiers] configuration")?;
        }

        let mode: BotMode = std::env::var("BOT_MODE")
            .unwrap_or_else(|_| "paper".to_string())
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
            trading: toml_cfg.trading,
            signal: toml_cfg.signal,
            infra: toml_cfg.infrastructure,
            logging: toml_cfg.logging,
        })
    }

    /// Apply CLI overrides on top of loaded config.
    pub fn apply_cli_overrides(&mut self, cli: &crate::cli::Cli) {
        if let Some(m) = &cli.mode {
            if let Ok(mode) = m.parse() {
                self.mode = mode;
            }
        }
    }

    pub fn signature_type(&self) -> Result<SignatureType> {
        self.infra.signature_type.parse()
    }

    pub fn max_bet_decimal(&self) -> Decimal {
        Decimal::try_from(self.trading.max_bet_usd).unwrap_or(Decimal::from(5))
    }

    pub fn starting_bankroll_decimal(&self) -> Decimal {
        Decimal::try_from(self.trading.starting_bankroll).unwrap_or(Decimal::from(500))
    }

    pub fn daily_loss_limit_decimal(&self) -> Decimal {
        Decimal::try_from(self.trading.daily_loss_limit_usd).unwrap_or(Decimal::from(25))
    }

    pub fn db_path(&self) -> &Path {
        Path::new(&self.infra.db_path)
    }

    pub fn telegram_enabled(&self) -> bool {
        self.logging.telegram_enabled
            && self.telegram_bot_token.is_some()
            && self.telegram_chat_id.is_some()
    }

    /// Watch-start time: when the sweep loop should wake up.
    pub fn watch_start_s(&self) -> u64 {
        self.signal.sweep.start_s + 2
    }
}

fn validate_sweep(cfg: &SweepConfig) -> Result<()> {
    if cfg.start_s < 3 || cfg.start_s > 30 {
        bail!("sweep.start_s must be 3..=30, got {}", cfg.start_s);
    }
    if cfg.steps < 2 || cfg.steps > 20 {
        bail!("sweep.steps must be 2..=20, got {}", cfg.steps);
    }
    if cfg.start_offset >= cfg.end_offset {
        bail!(
            "sweep.start_offset ({}) must be < end_offset ({})",
            cfg.start_offset,
            cfg.end_offset
        );
    }
    if cfg.min_delta_pct < 0.001 || cfg.min_delta_pct > 1.0 {
        bail!(
            "sweep.min_delta_pct must be 0.001..=1.0, got {}",
            cfg.min_delta_pct
        );
    }
    Ok(())
}

fn validate_entry_tiers(cfg: &EntryTiersConfig) -> Result<()> {
    if !cfg.enabled {
        return Ok(());
    }

    if cfg.tiers.is_empty() {
        bail!("At least one entry tier is required when tiered system is enabled");
    }

    for tier in &cfg.tiers {
        if tier.time_before_close_s < 3 || tier.time_before_close_s > 60 {
            bail!(
                "Tier '{}': time_before_close_s must be 3..=60, got {}",
                tier.name,
                tier.time_before_close_s
            );
        }
        if tier.price_offset < -0.30 || tier.price_offset > 0.0 {
            bail!(
                "Tier '{}': price_offset must be -0.30..=0.0, got {}",
                tier.name,
                tier.price_offset
            );
        }
        if tier.min_delta_pct < 0.01 || tier.min_delta_pct > 1.0 {
            bail!(
                "Tier '{}': min_delta_pct must be 0.01..=1.0, got {}",
                tier.name,
                tier.min_delta_pct
            );
        }
    }

    // Must be sorted descending by time (earliest tier first)
    let times: Vec<u64> = cfg.tiers.iter().map(|t| t.time_before_close_s).collect();
    for w in times.windows(2) {
        if w[0] <= w[1] {
            bail!(
                "Entry tiers must be sorted by time_before_close_s descending \
                 (e.g. [20, 12, 6]). Got {:?}",
                times
            );
        }
    }

    // No duplicate times
    let mut seen = std::collections::HashSet::new();
    for t in &times {
        if !seen.insert(t) {
            bail!("Duplicate time_before_close_s {} in entry tiers", t);
        }
    }

    // watch_start_s must give enough buffer
    let max_tier_time = times[0];
    if cfg.watch_start_s < max_tier_time + 2 {
        bail!(
            "watch_start_s ({}) must be >= earliest tier time ({}) + 2",
            cfg.watch_start_s,
            max_tier_time
        );
    }

    Ok(())
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
