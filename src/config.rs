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
    pub pricing: PricingConfig,
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
pub struct PricingConfig {
    pub min_delta_pct: f64,
    pub entry_start_s: u64,
    pub entry_cutoff_s: u64,
    pub undercut_offset: f64,
    pub tighten_step: f64,
    pub adjust_interval_ms: u64,
    pub max_entry_price: f64,
    pub min_entry_price: f64,
    pub delta_scaling: DeltaScalingConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeltaScalingConfig {
    /// Each entry: [min_delta_pct, max_entry_price_for_this_delta]
    pub tiers: Vec<[f64; 2]>,
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
    pricing: PricingConfig,
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

        validate_pricing(&toml_cfg.pricing)
            .context("Invalid [pricing] configuration")?;

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
            pricing: toml_cfg.pricing,
            infra: toml_cfg.infrastructure,
            logging: toml_cfg.logging,
        })
    }

    pub fn apply_cli_overrides(&mut self, cli: &crate::cli::Cli) {
        if let Some(m) = &cli.mode {
            if let Ok(mode) = m.parse() {
                self.mode = mode;
            }
        }
        if let Some(v) = cli.max_entry {
            self.pricing.max_entry_price = v;
        }
        if let Some(v) = cli.undercut {
            self.pricing.undercut_offset = v;
        }
        if let Some(v) = cli.min_delta {
            self.pricing.min_delta_pct = v;
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

    pub fn watch_start_s(&self) -> u64 {
        self.pricing.entry_start_s + 2
    }

    /// Max entry price for a given delta, using delta_scaling tiers.
    pub fn max_price_for_delta(&self, delta_pct: f64) -> f64 {
        let mut ceiling = self.pricing.max_entry_price;
        for tier in &self.pricing.delta_scaling.tiers {
            if delta_pct >= tier[0] {
                ceiling = tier[1];
            }
        }
        ceiling.min(self.pricing.max_entry_price)
    }
}

fn validate_pricing(cfg: &PricingConfig) -> Result<()> {
    if cfg.entry_start_s < 5 || cfg.entry_start_s > 60 {
        bail!("entry_start_s must be 5..=60, got {}", cfg.entry_start_s);
    }
    if cfg.entry_cutoff_s >= cfg.entry_start_s {
        bail!("entry_cutoff_s ({}) must be < entry_start_s ({})", cfg.entry_cutoff_s, cfg.entry_start_s);
    }
    if cfg.min_entry_price >= cfg.max_entry_price {
        bail!("min_entry_price ({}) must be < max_entry_price ({})", cfg.min_entry_price, cfg.max_entry_price);
    }
    if cfg.undercut_offset < 0.0 || cfg.undercut_offset > 0.20 {
        bail!("undercut_offset must be 0..=0.20, got {}", cfg.undercut_offset);
    }
    if cfg.tighten_step < 0.005 || cfg.tighten_step > 0.05 {
        bail!("tighten_step must be 0.005..=0.05, got {}", cfg.tighten_step);
    }
    if cfg.min_delta_pct < 0.01 || cfg.min_delta_pct > 1.0 {
        bail!("min_delta_pct must be 0.01..=1.0, got {}", cfg.min_delta_pct);
    }
    if cfg.delta_scaling.tiers.is_empty() {
        bail!("delta_scaling.tiers must have at least one entry");
    }
    for (i, tier) in cfg.delta_scaling.tiers.iter().enumerate() {
        if tier[0] < 0.01 || tier[1] < 0.50 || tier[1] > 0.99 {
            bail!("delta_scaling tier {}: invalid values {:?}", i, tier);
        }
    }
    Ok(())
}

fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
