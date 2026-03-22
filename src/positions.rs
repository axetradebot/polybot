use rust_decimal::Decimal;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Instant;

use crate::market;
use crate::types::{BotMode, Direction, PositionStatus, TradeOutcome};

/// A single open or recently-closed position tracked by the bot.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Position {
    pub id: u64,
    /// Row ID in the `active_positions` DB table (for persistence across restarts).
    pub db_id: Option<i64>,
    pub market_name: String,
    pub asset: String,
    pub window_seconds: u64,
    pub window_ts: u64,
    pub slug: String,
    pub direction: Direction,
    pub token_id: String,
    pub condition_id: String,
    pub neg_risk: bool,
    pub edge_score: f64,
    pub delta_pct: Decimal,

    pub initial_price: Decimal,
    pub current_price: Decimal,
    pub max_price: Decimal,
    pub best_ask_at_entry: Decimal,
    pub contracts: Decimal,
    pub bet_size_usd: Decimal,

    pub order_id: Option<String>,
    pub tighten_count: u32,
    pub last_adjust_at: Instant,
    pub placed_at: Instant,

    pub open_price: Decimal,
    pub status: PositionStatus,
    pub fill_price: Option<Decimal>,
    pub fill_size: Option<Decimal>,
    pub outcome: Option<TradeOutcome>,
    pub pnl: Option<Decimal>,
    pub mode: BotMode,
    pub settlement_started: bool,
    pub submit_secs_before_close: Option<u64>,
    pub confirm_secs_before_close: Option<u64>,
    pub pipeline_ms: Option<u128>,
}

/// Thread-safe tracker for all active and recently-settled positions.
#[derive(Clone)]
pub struct PositionTracker {
    positions: Arc<RwLock<Vec<Position>>>,
    next_id: Arc<RwLock<u64>>,
}

impl PositionTracker {
    pub fn new() -> Self {
        Self {
            positions: Arc::new(RwLock::new(Vec::new())),
            next_id: Arc::new(RwLock::new(1)),
        }
    }

    async fn next_id(&self) -> u64 {
        let mut id = self.next_id.write().await;
        let current = *id;
        *id += 1;
        current
    }

    /// Add a new position and return its assigned ID.
    pub async fn add(&self, mut pos: Position) -> u64 {
        let id = self.next_id().await;
        pos.id = id;
        self.positions.write().await.push(pos);
        id
    }

    /// Count positions that are currently open (Pending or Filled).
    pub async fn open_count(&self) -> usize {
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| matches!(p.status, PositionStatus::Pending | PositionStatus::Filled))
            .count()
    }

    /// Count open positions for a specific market name.
    pub async fn open_count_for_market(&self, market_name: &str) -> usize {
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| {
                p.market_name == market_name
                    && matches!(p.status, PositionStatus::Pending | PositionStatus::Filled)
            })
            .count()
    }

    /// Check if we already have a position (any status except Settled/Cancelled) in a specific slug.
    pub async fn has_position_in_window(&self, slug: &str) -> bool {
        let positions = self.positions.read().await;
        positions.iter().any(|p| {
            p.slug == slug
                && !matches!(
                    p.status,
                    PositionStatus::Settled | PositionStatus::Cancelled
                )
        })
    }

    /// Get all pending (live, unfilled) positions.
    pub async fn pending_positions(&self) -> Vec<Position> {
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| p.status == PositionStatus::Pending)
            .cloned()
            .collect()
    }

    /// Get pending positions whose window has closed (expired).
    pub async fn expired_pending(&self) -> Vec<Position> {
        let now = market::epoch_secs();
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| {
                p.status == PositionStatus::Pending
                    && now >= p.window_ts + p.window_seconds
            })
            .cloned()
            .collect()
    }

    /// Get pending positions that need tightening (adjust_interval_ms elapsed).
    pub async fn needs_tighten(&self, adjust_interval_ms: u64) -> Vec<Position> {
        let now = Instant::now();
        let interval = std::time::Duration::from_millis(adjust_interval_ms);
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| {
                p.status == PositionStatus::Pending
                    && p.mode == BotMode::Live
                    && now.duration_since(p.last_adjust_at) >= interval
            })
            .cloned()
            .collect()
    }

    /// Get filled positions past their window close that haven't started settlement.
    pub async fn filled_past_close(&self) -> Vec<Position> {
        let now = market::epoch_secs();
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| {
                p.status == PositionStatus::Filled
                    && !p.settlement_started
                    && now >= p.window_ts + p.window_seconds
            })
            .cloned()
            .collect()
    }

    /// Mark a position as filled.
    pub async fn mark_filled(&self, id: u64, fill_price: Decimal, fill_size: Decimal) {
        let mut positions = self.positions.write().await;
        if let Some(pos) = positions.iter_mut().find(|p| p.id == id) {
            pos.status = PositionStatus::Filled;
            pos.fill_price = Some(fill_price);
            pos.fill_size = Some(fill_size);
        }
    }

    /// Mark a position as expired (unfilled, window closed).
    pub async fn mark_expired(&self, id: u64) {
        let mut positions = self.positions.write().await;
        if let Some(pos) = positions.iter_mut().find(|p| p.id == id) {
            pos.status = PositionStatus::Expired;
        }
    }

    /// Mark settlement as started for a position.
    pub async fn mark_settlement_started(&self, id: u64) {
        let mut positions = self.positions.write().await;
        if let Some(pos) = positions.iter_mut().find(|p| p.id == id) {
            pos.settlement_started = true;
        }
    }

    /// Mark a position as settled with outcome and P&L.
    pub async fn mark_settled(&self, id: u64, outcome: TradeOutcome, pnl: Decimal) {
        let mut positions = self.positions.write().await;
        if let Some(pos) = positions.iter_mut().find(|p| p.id == id) {
            pos.status = PositionStatus::Settled;
            pos.outcome = Some(outcome);
            pos.pnl = Some(pnl);
        }
    }

    /// Update order info after a tighten (new order_id, new price).
    pub async fn update_tighten(&self, id: u64, new_order_id: String, new_price: Decimal) {
        let mut positions = self.positions.write().await;
        if let Some(pos) = positions.iter_mut().find(|p| p.id == id) {
            pos.order_id = Some(new_order_id);
            pos.current_price = new_price;
            pos.tighten_count += 1;
            pos.last_adjust_at = Instant::now();
        }
    }

    /// Today's P&L across all settled positions.
    pub async fn daily_pnl(&self) -> Decimal {
        let positions = self.positions.read().await;
        positions
            .iter()
            .filter(|p| p.status == PositionStatus::Settled)
            .filter_map(|p| p.pnl)
            .sum()
    }

    /// Prune settled/expired positions older than `max_age_s` to prevent unbounded growth.
    pub async fn prune_old(&self, max_age_s: u64) {
        let cutoff = market::epoch_secs().saturating_sub(max_age_s);
        let mut positions = self.positions.write().await;
        positions.retain(|p| {
            !matches!(
                p.status,
                PositionStatus::Settled | PositionStatus::Expired | PositionStatus::Cancelled
            ) || p.window_ts + p.window_seconds > cutoff
        });
    }
}
