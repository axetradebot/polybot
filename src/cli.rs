use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    name = "polymarket-btc-bot",
    about = "Multi-market Polymarket Up/Down maker bot with edge-ranked order routing"
)]
pub struct Cli {
    /// Trading mode: paper | live (overrides BOT_MODE env var)
    #[arg(long)]
    pub mode: Option<String>,

    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: String,

    /// Only run specific markets (comma-separated names, e.g. "BTC 5-min,ETH 5-min")
    #[arg(long)]
    pub markets: Option<String>,

    /// Disable specific markets at runtime (comma-separated)
    #[arg(long)]
    pub disable_market: Option<String>,

    /// Override max concurrent positions
    #[arg(long)]
    pub max_positions: Option<usize>,

    /// Override bet size in USD
    #[arg(long)]
    pub bet_size: Option<f64>,

    /// Run specific markets in paper mode while others run live (comma-separated)
    #[arg(long)]
    pub paper_markets: Option<String>,

    /// Send skip/verbose notifications to Telegram
    #[arg(long)]
    pub verbose: bool,
}
