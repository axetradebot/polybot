use anyhow::Result;
use rust_decimal::Decimal;
use tracing::{debug, error};

use crate::config::TelegramConfig;
use crate::db::{DailyAnalytics, MarketDayStats};
use crate::scanner::MarketOpportunity;
use crate::types::{BotMode, Direction, TradeRecord};

pub struct OrderTiming {
    pub secs_before_submit: u64,
    pub secs_before_confirm: u64,
    pub pipeline_ms: u128,
}

pub struct MarketSnapshot {
    pub name: String,
    pub is_live: bool,
    pub current_price: f64,
    pub delta_pct: f64,
    pub min_delta: f64,
}

pub struct TelegramNotifier {
    client: reqwest::Client,
    bot_token: String,
    chat_id: String,
    enabled: bool,
    on_trade: bool,
    on_error: bool,
    on_daily_summary: bool,
    verbose_skips: bool,
    mode: BotMode,
}

impl TelegramNotifier {
    pub fn new(
        bot_token: Option<String>,
        chat_id: Option<String>,
        tg_config: &TelegramConfig,
        mode: BotMode,
    ) -> Self {
        let is_enabled = tg_config.enabled
            && bot_token.as_ref().map_or(false, |t| !t.is_empty())
            && chat_id.as_ref().map_or(false, |c| !c.is_empty());
        Self {
            client: reqwest::Client::new(),
            bot_token: bot_token.unwrap_or_default(),
            chat_id: chat_id.unwrap_or_default(),
            enabled: is_enabled,
            on_trade: tg_config.on_trade,
            on_error: tg_config.on_error,
            on_daily_summary: tg_config.daily_summary,
            verbose_skips: tg_config.verbose_skips,
            mode,
        }
    }

    /// Order submission notification — sent when an order is posted to CLOB.
    pub async fn send_order_submitted(
        &self,
        market_name: &str,
        slug: &str,
        direction: &str,
        price: Decimal,
        contracts: Decimal,
        secs_before_close: u64,
        pipeline_ms: u128,
        order_id: &str,
    ) -> Result<()> {
        if !self.enabled || !self.on_trade {
            return Ok(());
        }
        let bet_usd = price * contracts;
        let msg = format!(
            "\u{1F4E4} ORDER PLACED \u{2014} {market_name}\n\
             \u{200b}  Window: {slug}\n\
             \u{200b}  Direction: {direction} | ${price} \u{00D7} {contracts} = ${bet_usd:.2}\n\
             \u{200b}  \u{23F1} Confirmed T-{secs_before_close}s | Pipeline {pipeline_ms}ms\n\
             \u{200b}  Order: {order_id}",
        );
        self.send_message(&msg).await
    }

    /// Order failure notification — sent when CLOB rejects or order pipeline fails.
    pub async fn send_order_failed(
        &self,
        market_name: &str,
        slug: &str,
        direction: &str,
        price: Decimal,
        secs_before_close: u64,
        error: &str,
    ) -> Result<()> {
        if !self.enabled || !self.on_error {
            return Ok(());
        }
        let msg = format!(
            "\u{274C} ORDER FAILED \u{2014} {market_name}\n\
             \u{200b}  Window: {slug}\n\
             \u{200b}  Direction: {direction} | Price: ${price}\n\
             \u{200b}  \u{23F1} T-{secs_before_close}s remaining\n\
             \u{200b}  Error: {error}",
        );
        self.send_message(&msg).await
    }

    /// Scanner skip notification — sent for important skips like "no asks".
    pub async fn send_scanner_skip(
        &self,
        market_name: &str,
        slug: &str,
        direction: &str,
        secs_remaining: u64,
        reason: &str,
    ) -> Result<()> {
        if !self.enabled || !self.on_error {
            return Ok(());
        }
        let msg = format!(
            "\u{26A0}\u{FE0F} SKIP \u{2014} {market_name}\n\
             \u{200b}  Window: {slug}\n\
             \u{200b}  Direction: {direction} | T-{secs_remaining}s\n\
             \u{200b}  Reason: {reason}",
        );
        self.send_message(&msg).await
    }

    /// Watching notification — sent when the bot has a delta signal but is waiting
    /// for a tradeable orderbook. Replaces the premature SKIP alert.
    pub async fn send_watching(
        &self,
        market_name: &str,
        direction: &str,
        delta: f64,
        secs_remaining: u64,
        initial_reason: &str,
    ) -> Result<()> {
        if !self.enabled || !self.on_trade {
            return Ok(());
        }
        let msg = format!(
            "\u{1F440} WATCHING \u{2014} {market_name}\n\
             \u{200b}  {direction} {delta:.4}% | T-{secs_remaining}s\n\
             \u{200b}  {initial_reason}\n\
             \u{200b}  Polling for tradeable asks until cutoff\u{2026}",
        );
        self.send_message(&msg).await
    }

    /// Fill notification — gated by `on_trade`.
    pub async fn send_fill_multi(
        &self,
        trade: &TradeRecord,
        opp: &MarketOpportunity,
        rank: usize,
        total_opps: usize,
        open_positions: usize,
        max_positions: usize,
        timing: Option<&OrderTiming>,
    ) -> Result<()> {
        if !self.enabled || !self.on_trade {
            return Ok(());
        }

        let mode_tag = self.mode_tag_for(&trade.mode);
        let tighten_info = if trade.tighten_count > 0 {
            format!(" (tightened {}x)", trade.tighten_count)
        } else {
            String::new()
        };

        let timing_line = if let Some(t) = timing {
            format!(
                "\n\u{200b}  \u{23F1} Submitted T-{}s | Confirmed T-{}s | Pipeline {}ms",
                t.secs_before_submit, t.secs_before_confirm, t.pipeline_ms
            )
        } else {
            String::new()
        };

        let msg = format!(
            "📊 FILL {mode_tag}— {}\n\
             \u{200b}  Window: {}\n\
             \u{200b}  Direction: {}\n\
             \u{200b}  Edge score: {:.3} (#{} of {} opportunities)\n\
             \u{200b}  Book ask: ${} → Filled: ${}{}\n\
             \u{200b}  Delta: {}%\n\
             \u{200b}  Contracts: {} | Bet: ${}\n\
             \u{200b}  Open positions: {}/{}{}",
            trade.market_name,
            trade.slug,
            trade.direction,
            opp.edge_score,
            rank,
            total_opps,
            opp.best_ask,
            trade.final_price,
            tighten_info,
            trade.delta_pct,
            trade.contracts,
            trade.bet_size_usd,
            open_positions,
            max_positions,
            timing_line,
        );

        self.send_message(&msg).await
    }

    /// Simple fill notification from position data — gated by `on_trade`.
    /// Used for live fills detected via order polling.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_fill_from_position(
        &self,
        market_name: &str,
        slug: &str,
        direction: &str,
        mode_str: &str,
        fill_price: Decimal,
        fill_size: Decimal,
        edge_score: f64,
        best_ask: Decimal,
        delta_pct: Decimal,
        tighten_count: u32,
        open_positions: usize,
        max_positions: usize,
        timing: Option<&OrderTiming>,
    ) -> Result<()> {
        if !self.enabled || !self.on_trade {
            return Ok(());
        }

        let mode_tag = self.mode_tag_for(mode_str);
        let tighten_info = if tighten_count > 0 {
            format!(" (tightened {}x)", tighten_count)
        } else {
            String::new()
        };
        let bet_usd = fill_price * fill_size;

        let timing_line = if let Some(t) = timing {
            format!(
                "\n\u{200b}  \u{23F1} Submitted T-{}s | Confirmed T-{}s | Pipeline {}ms",
                t.secs_before_submit, t.secs_before_confirm, t.pipeline_ms
            )
        } else {
            String::new()
        };

        let msg = format!(
            "📊 FILL {mode_tag}— {market_name}\n\
             \u{200b}  Window: {slug}\n\
             \u{200b}  Direction: {direction}\n\
             \u{200b}  Edge score: {edge_score:.3}\n\
             \u{200b}  Book ask: ${best_ask} → Filled: ${fill_price}{tighten_info}\n\
             \u{200b}  Delta: {delta_pct}%\n\
             \u{200b}  Contracts: {fill_size} | Bet: ${bet_usd:.2}\n\
             \u{200b}  Open positions: {open_positions}/{max_positions}{timing_line}",
        );

        self.send_message(&msg).await
    }

    /// Settlement notification — gated by `on_trade`.
    #[allow(clippy::too_many_arguments)]
    pub async fn send_settlement(
        &self,
        market_name: &str,
        slug: &str,
        direction: Direction,
        fill_price: Decimal,
        fill_size: Decimal,
        won: bool,
        pnl: Decimal,
        bankroll: Decimal,
        initial_price: Decimal,
        tighten_count: u32,
        best_ask_at_entry: Decimal,
        edge_score: f64,
        delta_pct: Decimal,
        mode_str: &str,
    ) -> Result<()> {
        if !self.enabled || !self.on_trade {
            return Ok(());
        }

        let mode_tag = self.mode_tag_for(mode_str);
        let emoji = if won { "✅" } else { "❌" };
        let outcome = if won { "WIN" } else { "LOSS" };

        let dir_arrow = match direction {
            Direction::Up => "↑ UP",
            Direction::Down => "↓ DOWN",
        };

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
            "{emoji} {mode_tag}{outcome} — {market_name}\n\
             \u{200b}  Window: {slug}\n\
             \u{200b}  Direction: {dir_arrow} {correct}\n\
             \u{200b}  Book ask: ${best_ask_at_entry} → Post: ${initial_price}{tighten_info}\n\
             \u{200b}  Fill: ${fill_price} × {fill_size} contracts\n\
             \u{200b}  Delta: {delta_pct:.4}% | Edge: {edge_score:.3}\n\
             \u{200b}  P&L: {pnl_sign}${pnl:.2} ({pnl_sign}{roi_pct}%)\n\
             \u{200b}  Bankroll: ${bankroll:.2}"
        );

        self.send_message(&msg).await
    }

    /// Daily summary — gated by `daily_summary`.
    pub async fn send_daily_summary_multi(
        &self,
        total_windows: u64,
        total_opps: u32,
        total_placed: u32,
        total_fills: u32,
        total_wins: u32,
        total_losses: u32,
        net_pnl: Decimal,
        bankroll: Decimal,
        market_stats: &[MarketDayStats],
    ) -> Result<()> {
        if !self.enabled || !self.on_daily_summary {
            return Ok(());
        }

        let fill_rate = if total_placed > 0 {
            format!("{:.0}%", total_fills as f64 / total_placed as f64 * 100.0)
        } else {
            "N/A".to_string()
        };

        let win_rate = if total_fills > 0 {
            format!(
                "{:.1}%",
                total_wins as f64 / total_fills as f64 * 100.0
            )
        } else {
            "N/A".to_string()
        };

        let pnl_sign = if net_pnl >= Decimal::ZERO { "+" } else { "" };
        let mode_tag = match self.mode {
            BotMode::Paper => "[PAPER] ",
            BotMode::Live => "",
        };

        let mut msg = format!(
            "📈 {mode_tag}Daily Summary\n\n\
             \u{200b}  OVERALL:\n\
             \u{200b}    Windows scanned: {total_windows}\n\
             \u{200b}    Opportunities found: {total_opps}\n\
             \u{200b}    Orders placed: {total_placed}\n\
             \u{200b}    Filled: {total_fills} ({fill_rate} fill rate)\n\
             \u{200b}    Won: {total_wins} / Lost: {total_losses} ({win_rate} win rate)\n\
             \u{200b}    Net P&amp;L: {pnl_sign}${net_pnl:.2}\n\
             \u{200b}    Bankroll: ${bankroll:.2}"
        );

        if !market_stats.is_empty() {
            msg.push_str("\n\n\u{200b}  BY MARKET:");
            for stat in market_stats {
                let stat_pnl_sign = if stat.pnl >= Decimal::ZERO { "+" } else { "" };
                msg.push_str(&format!(
                    "\n\u{200b}    {}: {} fills — {}W/{}L — {}${}",
                    stat.market_name, stat.fills, stat.wins, stat.losses,
                    stat_pnl_sign, stat.pnl.round_dp(2)
                ));
            }
        }

        self.send_message(&msg).await
    }

    /// Skip notification — gated by `verbose_skips`.
    pub async fn send_skip(&self, market_name: &str, slug: &str, reason: &str) -> Result<()> {
        if !self.enabled || !self.verbose_skips {
            return Ok(());
        }
        let mode_tag = self.mode_tag();
        let msg = format!(
            "⏭️ SKIP {mode_tag}— {market_name}\n\
             \u{200b}  Window: {slug}\n\
             \u{200b}  Reason: {}",
            Self::escape_html(reason)
        );
        self.send_message(&msg).await
    }

    /// Error notification — gated by `on_error`.
    pub async fn send_error(&self, error_msg: &str) -> Result<()> {
        if !self.enabled || !self.on_error {
            return Ok(());
        }
        let msg = format!("🚨 {}Error\n{}", self.mode_tag(), Self::escape_html(error_msg));
        self.send_message(&msg).await
    }

    /// Startup notification — always sent when telegram is enabled.
    /// Shows each market with its effective mode (LIVE/PAPER).
    pub async fn send_startup(
        &self,
        bankroll: Decimal,
        market_modes: &[(String, String)],
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let mut market_lines = String::new();
        for (name, mode) in market_modes {
            let tag = if mode.eq_ignore_ascii_case("live") {
                "🟢 LIVE"
            } else {
                "⚪ PAPER"
            };
            market_lines.push_str(&format!("\n\u{200b}  {} — {}", tag, name));
        }

        let msg = format!(
            "🚀 Multi-Market Bot Started\n\
             \u{200b}  Bankroll: ${bankroll}{market_lines}"
        );
        self.send_message(&msg).await
    }

    /// Periodic heartbeat with market status — sent every N minutes.
    /// Shows prices, deltas, positions, and why it's quiet if no trades.
    pub async fn send_heartbeat(
        &self,
        market_snapshots: &[MarketSnapshot],
        open_positions: usize,
        bankroll: Decimal,
        daily_wins: u32,
        daily_losses: u32,
        daily_pnl: Decimal,
        windows_scanned: u64,
        clob_balance: Option<Decimal>,
    ) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }

        let pnl_sign = if daily_pnl >= Decimal::ZERO { "+" } else { "" };
        let bal_str = clob_balance
            .map(|b| format!("${b:.2}"))
            .unwrap_or_else(|| "N/A".to_string());
        let mut msg = format!(
            "📡 Status Update\n\
             \u{200b}  Positions: {} | CLOB Balance: {bal_str}\n\
             \u{200b}  Bot Bankroll: ${bankroll:.2}\n\
             \u{200b}  Today: {}W/{}L | {pnl_sign}${daily_pnl:.2}\n\
             \u{200b}  Windows scanned: {windows_scanned}\n",
            open_positions, daily_wins, daily_losses,
        );

        msg.push_str("\n\u{200b}  MARKETS:");
        for snap in market_snapshots {
            let mode_tag = if snap.is_live { "🟢" } else { "⚪" };
            let delta_str = format!("{:.4}%", snap.delta_pct);
            let dir = if snap.delta_pct >= 0.0 { "↑" } else { "↓" };
            let status = if snap.delta_pct.abs() < snap.min_delta {
                "quiet"
            } else {
                "ready"
            };
            msg.push_str(&format!(
                "\n\u{200b}  {} {} ${:.0} | {}{} (need {:.2}%) [{}]",
                mode_tag, snap.name, snap.current_price,
                dir, delta_str, snap.min_delta, status,
            ));
        }

        self.send_message(&msg).await
    }

    /// Window miss notification — sent when a window closes with no trades placed.
    /// Gated by `on_trade`.
    pub async fn send_window_miss(
        &self,
        missed_markets: &[(String, String)], // (market_name, reason)
    ) -> Result<()> {
        if !self.enabled || !self.on_trade || missed_markets.is_empty() {
            return Ok(());
        }
        let mut msg = "⏭ Window closed — no trades placed".to_string();
        for (name, reason) in missed_markets {
            msg.push_str(&format!("\n\u{200b}  ❌ {name}: {}", Self::escape_html(reason)));
        }
        self.send_message(&msg).await
    }

    /// Daily analytics report with delta/timing breakdowns — gated by `daily_summary`.
    pub async fn send_daily_analytics(
        &self,
        analytics: &DailyAnalytics,
        bankroll: Decimal,
    ) -> Result<()> {
        if !self.enabled || !self.on_daily_summary {
            return Ok(());
        }

        let total_trades: u32 = analytics.market_stats.iter().map(|s| s.fills).sum();
        let total_wins: u32 = analytics.market_stats.iter().map(|s| s.wins).sum();
        let total_pnl: Decimal = analytics.market_stats.iter().map(|s| s.pnl).sum();
        let pnl_sign = if total_pnl >= Decimal::ZERO { "+" } else { "" };
        let win_rate = if total_trades > 0 {
            format!("{:.1}%", total_wins as f64 / total_trades as f64 * 100.0)
        } else {
            "N/A".to_string()
        };

        let mut msg = format!(
            "📊 Daily Analytics Report\n\n\
             \u{200b}  Trades: {} | Win rate: {} | {pnl_sign}${total_pnl:.2}\n\
             \u{200b}  Missed windows: {}\n\
             \u{200b}  Bankroll: ${bankroll:.2}\n",
            total_trades, win_rate, analytics.missed_windows,
        );

        if !analytics.delta_buckets.is_empty() {
            msg.push_str("\n\u{200b}  DELTA BREAKDOWN:");
            for b in &analytics.delta_buckets {
                let wr = if b.trades > 0 {
                    format!("{:.0}%", b.wins as f64 / b.trades as f64 * 100.0)
                } else { "—".into() };
                let ps = if b.pnl >= 0.0 { "+" } else { "" };
                msg.push_str(&format!(
                    "\n\u{200b}    {}: {} trades, {} WR, {ps}${:.2}",
                    b.label, b.trades, wr, b.pnl
                ));
            }
        }

        if !analytics.timing_buckets.is_empty() {
            msg.push_str("\n\n\u{200b}  ENTRY TIMING:");
            for b in &analytics.timing_buckets {
                let wr = if b.trades > 0 {
                    format!("{:.0}%", b.wins as f64 / b.trades as f64 * 100.0)
                } else { "—".into() };
                let ps = if b.pnl >= 0.0 { "+" } else { "" };
                msg.push_str(&format!(
                    "\n\u{200b}    {}: {} trades, {} WR, {ps}${:.2}",
                    b.label, b.trades, wr, b.pnl
                ));
            }
        }

        if !analytics.market_stats.is_empty() {
            msg.push_str("\n\n\u{200b}  BY MARKET:");
            for s in &analytics.market_stats {
                let ps = if s.pnl >= Decimal::ZERO { "+" } else { "" };
                msg.push_str(&format!(
                    "\n\u{200b}    {}: {}W/{}L — {ps}${:.2}",
                    s.market_name, s.wins, s.losses, s.pnl.round_dp(2)
                ));
            }
        }

        self.send_message(&msg).await
    }

    /// Redemption success notification — gated by `on_trade` (not `on_error`).
    pub async fn send_redeem_success(&self, market_name: &str, tx_hash: &str) -> Result<()> {
        if !self.enabled || !self.on_trade {
            return Ok(());
        }
        let msg = format!(
            "💰 Auto-redeemed — {market_name}\n\
             \u{200b}  tx: {tx_hash}"
        );
        self.send_message(&msg).await
    }

    fn escape_html(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
    }

    fn mode_tag(&self) -> &'static str {
        match self.mode {
            BotMode::Paper => "[PAPER] ",
            BotMode::Live => "",
        }
    }

    fn mode_tag_for(&self, mode_str: &str) -> &'static str {
        if mode_str.eq_ignore_ascii_case("paper") {
            "[PAPER] "
        } else {
            ""
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
