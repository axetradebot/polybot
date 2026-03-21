use anyhow::Result;
use rust_decimal::Decimal;
use tracing::{debug, error};

use crate::signal::TradeDecision;
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

    /// Orderbook-aware fill notification.
    pub async fn send_fill_ob(&self, trade: &TradeRecord, decision: &TradeDecision) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let tighten_info = if trade.tighten_count > 0 {
            format!(" (tightened {}x)", trade.tighten_count)
        } else {
            String::new()
        };

        let msg = format!(
            "📊 FILL {mode_tag}— {}\n\
             \u{200b}  Direction: {}\n\
             \u{200b}  Book best ask: ${}\n\
             \u{200b}  Initial post: ${}\n\
             \u{200b}  Fill price: ${}{}\n\
             \u{200b}  Delta: {}%\n\
             \u{200b}  Contracts: {}\n\
             \u{200b}  Bet: ${}",
            trade.market_slug,
            trade.direction,
            decision.best_ask_at_entry,
            decision.initial_price,
            trade.entry_price,
            tighten_info,
            trade.delta_pct,
            trade.size,
            trade.cost_usd,
        );

        self.send_message(&msg).await
    }

    /// Skip notification (for --verbose mode).
    pub async fn send_skip(&self, slug: &str, reason: &str) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let msg = format!(
            "⏭️ SKIP {mode_tag}— {slug}\n\
             \u{200b}  Reason: {reason}"
        );

        self.send_message(&msg).await
    }

    /// Rich settlement notification with orderbook context.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_settlement(
        &self,
        slug: &str,
        direction: Direction,
        fill_price: Decimal,
        fill_size: Decimal,
        btc_open: Decimal,
        btc_close: Decimal,
        won: bool,
        pnl: Decimal,
        bankroll: Decimal,
        initial_price: Decimal,
        tighten_count: u32,
        best_ask_at_entry: Decimal,
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

        let tighten_info = if tighten_count > 0 {
            format!(" ({}x tightens)", tighten_count)
        } else {
            String::new()
        };

        let msg = format!(
            "{emoji} {mode_tag}{outcome} — {slug}\n\
             \u{200b}  Direction: {dir_arrow} {correct}\n\
             \u{200b}  Book ask: ${best_ask_at_entry} → Post: ${initial_price}{tighten_info}\n\
             \u{200b}  Fill: ${fill_price} × {fill_size} contracts\n\
             \u{200b}  BTC: ${btc_open:.0} → ${btc_close:.0} ({btc_moved})\n\
             \u{200b}  P&amp;L: {pnl_sign}${pnl:.2} ({pnl_sign}{roi_pct}%)\n\
             \u{200b}  Bankroll: ${bankroll:.2}"
        );

        self.send_message(&msg).await
    }

    /// Legacy fill notification (backward compat).
    #[allow(dead_code)]
    pub async fn send_fill(&self, trade: &TradeRecord) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let mode_tag = self.mode_tag();
        let msg = format!(
            "📊 FILL {mode_tag}— Window {}\n\
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
            format!("🚀 {mode_tag}Bot Started (orderbook-aware)\nBankroll: ${bankroll}\nMode: {}", self.mode);
        self.send_message(&msg).await
    }

    #[allow(dead_code)]
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
