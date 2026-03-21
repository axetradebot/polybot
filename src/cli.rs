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
}
