use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "polymarket-btc-bot", about = "Polymarket 5-minute BTC maker bot")]
pub struct Cli {
    /// Trading mode: paper | live (overrides BOT_MODE env var)
    #[arg(long)]
    pub mode: Option<String>,

    /// Override early tier time_before_close_s
    #[arg(long)]
    pub tier_early_time: Option<u64>,

    /// Override early tier price_offset (e.g. -0.12)
    #[arg(long)]
    pub tier_early_offset: Option<f64>,

    /// Override mid tier time_before_close_s
    #[arg(long)]
    pub tier_mid_time: Option<u64>,

    /// Override late tier time_before_close_s
    #[arg(long)]
    pub tier_late_time: Option<u64>,

    /// Disable tiered system, use single-entry mode
    #[arg(long, default_value_t = false)]
    pub single_entry: bool,

    /// Single-entry time before close override (seconds)
    #[arg(long)]
    pub entry_time: Option<u64>,

    /// Skip a specific tier by name (e.g. --skip-tier late)
    #[arg(long)]
    pub skip_tier: Option<String>,

    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: String,
}
