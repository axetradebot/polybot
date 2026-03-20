use anyhow::Result;
use rust_decimal::Decimal;
use tracing::{debug, error};

use crate::types::{BotMode, Direction, TradeRecord};

pub struct TelegramNotifier {
    client: reqwest::Client,
    bot_token: String,
    chat_id: String,
    enabled: bool,
    mode: BotMode,
}

impl TelegramNotifier {
    pub fn new(
        bot_token: Option<String>,
        chat_id: Option<String>,
        enabled: bool,
        mode: BotMode,
    ) -> Self {
        let is_enabled = enabled && bot_token.as_ref().map_or(false, |t| !t.is_empty());
        Self {
            client: reqwest::Client::new(),
            bot_token: bot_token.unwrap_or_default(),
            chat_id: chat_id.unwrap_or_default(),
            enabled: is_enabled,
            mode,
        }
    }

    /// Tier-aware fill notification.
    pub async fn send_fill(&self, trade: &TradeRecord) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let tier_label = format!("{} (T-{})", trade.tier_name, trade.seconds_at_entry);

        let msg = format!(
            "📊 FILL {mode_tag}— Window {}\n\
             \u{200b}  Tier: {tier_label}\n\
             \u{200b}  Direction: {}\n\
             \u{200b}  Entry: ${}\n\
             \u{200b}  Delta: {}%\n\
             \u{200b}  Contracts: {}\n\
             \u{200b}  Bet: ${}",
            trade.window_ts,
            trade.direction,
            trade.entry_price,
            trade.delta_pct,
            trade.size,
            trade.cost_usd,
        );

        self.send_message(&msg).await
    }

    /// Generic trade placed notification (backward compat).
    pub async fn send_trade(&self, trade: &TradeRecord) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let emoji = if trade.filled { "🔵" } else { "⚪" };
        let msg = format!(
            "{emoji} {mode_tag}Trade — {}\n\
             \u{200b}  Tier: {} (T-{})\n\
             \u{200b}  Direction: {}\n\
             \u{200b}  Entry: ${}\n\
             \u{200b}  Size: {} contracts\n\
             \u{200b}  Cost: ${}\n\
             \u{200b}  Delta: {}%\n\
             \u{200b}  Filled: {}",
            trade.market_slug,
            trade.tier_name,
            trade.seconds_at_entry,
            trade.direction,
            trade.entry_price,
            trade.size,
            trade.cost_usd,
            trade.delta_pct,
            if trade.filled { "Yes" } else { "No" }
        );

        self.send_message(&msg).await
    }

    pub async fn send_outcome(
        &self,
        order_id: &str,
        outcome: &str,
        pnl: Decimal,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let emoji = match outcome {
            "WIN" => "✅",
            "LOSS" => "❌",
            _ => "⏳",
        };

        let msg = format!(
            "{emoji} {mode_tag}Trade Result\n\
             \u{200b}  Order: {order_id}\n\
             \u{200b}  Outcome: {outcome}\n\
             \u{200b}  P&L: ${pnl}"
        );

        self.send_message(&msg).await
    }

    /// Rich settlement notification with full P&L breakdown.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_settlement(
        &self,
        slug: &str,
        tier_name: &str,
        direction: Direction,
        fill_price: Decimal,
        fill_size: Decimal,
        btc_open: Decimal,
        btc_close: Decimal,
        won: bool,
        pnl: Decimal,
        bankroll: Decimal,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let emoji = if won { "✅" } else { "❌" };
        let outcome = if won { "WIN" } else { "LOSS" };

        let dir_arrow = match direction {
            Direction::Up => "↑ UP",
            Direction::Down => "↓ DOWN",
        };

        let btc_moved = if btc_close >= btc_open { "↑ UP" } else { "↓ DOWN" };
        let correct = if won { "✓" } else { "✗" };

        let bet_usd = fill_price * fill_size;
        let pnl_sign = if pnl >= Decimal::ZERO { "+" } else { "" };
        let roi_pct = if bet_usd > Decimal::ZERO {
            (pnl / bet_usd * Decimal::from(100))
                .round_dp(1)
                .to_string()
        } else {
            "0.0".to_string()
        };

        let msg = format!(
            "{emoji} {mode_tag}{outcome} — {slug}\n\
             \u{200b}  Tier: {tier_name}\n\
             \u{200b}  Direction: {dir_arrow} {correct}\n\
             \u{200b}  Fill: ${fill_price} × {fill_size} contracts\n\
             \u{200b}  BTC: ${btc_open:.0} → ${btc_close:.0} ({btc_moved})\n\
             \u{200b}  P&amp;L: {pnl_sign}${pnl:.2} ({pnl_sign}{roi_pct}%)\n\
             \u{200b}  Bankroll: ${bankroll:.2}"
        );

        self.send_message(&msg).await
    }

    /// Daily summary with per-tier breakdown.
    #[allow(dead_code)]
    pub async fn send_daily_summary_with_tiers(
        &self,
        date: &str,
        total_windows: u32,
        attempts: u32,
        tier_stats: &[(String, u32, u32, u32, Decimal)], // (name, fills, wins, losses, pnl)
        total_pnl: Decimal,
        bankroll: Decimal,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let mut tier_lines = String::new();
        let mut total_fills = 0u32;
        let mut total_wins = 0u32;
        let mut total_losses = 0u32;

        for (name, fills, wins, losses, pnl) in tier_stats {
            total_fills += fills;
            total_wins += wins;
            total_losses += losses;
            let fill_pct = if attempts > 0 {
                *fills as f64 / attempts as f64 * 100.0
            } else {
                0.0
            };
            tier_lines.push_str(&format!(
                "\u{200b}  {name}: {fills} fills ({fill_pct:.0}%) — {wins}W/{losses}L — ${pnl:.2}\n"
            ));
        }

        let win_rate = if total_fills > 0 {
            format!("{:.0}%", total_wins as f64 / total_fills as f64 * 100.0)
        } else {
            "N/A".to_string()
        };

        let msg = format!(
            "📈 {mode_tag}Daily Summary — {date}\n\
             \u{200b}  Total windows: {total_windows}\n\
             \u{200b}  Attempts: {attempts}\n\n\
             Tier breakdown:\n{tier_lines}\n\
             \u{200b}  Total: {total_fills} fills — {total_wins}W/{total_losses}L ({win_rate})\n\
             \u{200b}  Net P&L: ${total_pnl:.2}\n\
             \u{200b}  Bankroll: ${bankroll:.2}"
        );

        self.send_message(&msg).await
    }

    #[allow(dead_code)]
    pub async fn send_daily_summary(
        &self,
        total: u32,
        wins: u32,
        losses: u32,
        pnl: Decimal,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let win_rate = if total > 0 {
            format!("{:.1}%", wins as f64 / total as f64 * 100.0)
        } else {
            "N/A".to_string()
        };

        let msg = format!(
            "📊 {mode_tag}Daily Summary\n\
             \u{200b}  Trades: {total}\n\
             \u{200b}  Wins: {wins} | Losses: {losses}\n\
             \u{200b}  Win Rate: {win_rate}\n\
             \u{200b}  Net P&L: ${pnl}"
        );

        self.send_message(&msg).await
    }

    pub async fn send_error(&self, error_msg: &str) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let msg = format!("🚨 {}Error\n{error_msg}", self.mode_tag());
        self.send_message(&msg).await
    }

    pub async fn send_startup(&self, bankroll: Decimal) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let msg =
            format!("🚀 {mode_tag}Bot Started\nBankroll: ${bankroll}\nMode: {}", self.mode);
        self.send_message(&msg).await
    }

    fn mode_tag(&self) -> &'static str {
        match self.mode {
            BotMode::Paper => "[PAPER] ",
            BotMode::Live => "",
        }
    }

    async fn send_message(&self, text: &str) -> Result<()> {
        let url = format!(
            "https://api.telegram.org/bot{}/sendMessage",
            self.bot_token
        );

        let body = serde_json::json!({
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
        });

        match self.client.post(&url).json(&body).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    error!(status = %status, body = %body, "Telegram API error");
                } else {
                    debug!("Telegram message sent");
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to send Telegram message");
            }
        }

        Ok(())
    }
}
