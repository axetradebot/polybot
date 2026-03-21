use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "polymarket-btc-bot", about = "Polymarket 5-minute BTC maker bot")]
pub struct Cli {
    /// Trading mode: paper | live (overrides BOT_MODE env var)
    #[arg(long)]
    pub mode: Option<String>,

    /// Path to config file
    #[arg(long, default_value = "config.toml")]
    pub config: String,

    /// Override max entry price
    #[arg(long)]
    pub max_entry: Option<f64>,

    /// Override undercut offset
    #[arg(long)]
    pub undercut: Option<f64>,

    /// Override minimum delta percentage
    #[arg(long)]
    pub min_delta: Option<f64>,

    /// Send skip notifications to Telegram
    #[arg(long)]
    pub verbose: bool,
}
