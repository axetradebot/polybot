use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;
use tracing::info;

use crate::types::TradeRecord;

/// A row from the `active_positions` table, used to recover positions across restarts.
#[derive(Debug, Clone)]
pub struct PersistedPosition {
    pub id: i64,
    pub market_name: String,
    pub asset: String,
    pub window_seconds: u64,
    pub window_ts: u64,
    pub slug: String,
    pub direction: String,
    pub token_id: String,
    pub condition_id: String,
    pub neg_risk: bool,
    pub edge_score: f64,
    pub delta_pct: String,
    pub initial_price: String,
    pub current_price: String,
    pub max_price: String,
    pub best_ask_at_entry: String,
    pub contracts: String,
    pub bet_size_usd: String,
    pub order_id: Option<String>,
    pub tighten_count: u32,
    pub open_price: String,
    pub status: String,
    pub fill_price: Option<String>,
    pub fill_size: Option<String>,
    pub mode: String,
}

pub struct TradeDb {
    conn: Mutex<Connection>,
}

impl TradeDb {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open database: {}", path.display()))?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS trades (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp        TEXT NOT NULL,
                market_name      TEXT NOT NULL DEFAULT '',
                asset            TEXT NOT NULL DEFAULT '',
                window_seconds   INTEGER NOT NULL DEFAULT 300,
                window_ts        INTEGER NOT NULL,
                slug             TEXT NOT NULL DEFAULT '',
                mode             TEXT NOT NULL,
                direction        TEXT NOT NULL,
                token_id         TEXT NOT NULL DEFAULT '',
                order_id         TEXT NOT NULL DEFAULT '',
                initial_price    TEXT NOT NULL DEFAULT '0',
                final_price      TEXT NOT NULL DEFAULT '0',
                tighten_count    INTEGER NOT NULL DEFAULT 0,
                best_ask_at_entry TEXT NOT NULL DEFAULT '0',
                filled           INTEGER NOT NULL DEFAULT 0,
                fill_price       TEXT,
                outcome          TEXT NOT NULL DEFAULT 'PENDING',
                pnl              TEXT NOT NULL DEFAULT '0',
                delta_pct        TEXT NOT NULL DEFAULT '0',
                edge_score       REAL NOT NULL DEFAULT 0.0,
                seconds_remaining REAL NOT NULL DEFAULT 0.0,
                contracts        TEXT NOT NULL DEFAULT '0',
                bet_size_usd     TEXT NOT NULL DEFAULT '0',
                open_price       TEXT NOT NULL DEFAULT '0',
                close_price      TEXT NOT NULL DEFAULT '0',
                skip_reason      TEXT
            );

            CREATE TABLE IF NOT EXISTS daily_summary (
                date            TEXT PRIMARY KEY,
                total_trades    INTEGER NOT NULL DEFAULT 0,
                wins            INTEGER NOT NULL DEFAULT 0,
                losses          INTEGER NOT NULL DEFAULT 0,
                fills           INTEGER NOT NULL DEFAULT 0,
                total_pnl       TEXT NOT NULL DEFAULT '0',
                mode            TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS scanner_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp        TEXT NOT NULL DEFAULT (datetime('now')),
                market_name      TEXT NOT NULL,
                window_ts        INTEGER NOT NULL,
                secs_remaining   INTEGER NOT NULL,
                direction        TEXT,
                delta_pct        REAL,
                open_price       TEXT,
                current_price    TEXT,
                best_ask         TEXT,
                best_bid         TEXT,
                spread           TEXT,
                depth_at_ask     TEXT,
                suggested_entry  TEXT,
                max_entry        TEXT,
                edge_score       REAL,
                result           TEXT NOT NULL,
                detail           TEXT
            );

            CREATE TABLE IF NOT EXISTS window_summary (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                market_name      TEXT NOT NULL,
                window_ts        INTEGER NOT NULL,
                open_price       TEXT NOT NULL,
                close_price      TEXT NOT NULL DEFAULT '0',
                max_delta_pct    REAL NOT NULL DEFAULT 0,
                peak_direction   TEXT,
                trade_placed     INTEGER NOT NULL DEFAULT 0,
                trade_won        INTEGER,
                entry_price      TEXT,
                pnl              TEXT,
                skip_reason      TEXT,
                UNIQUE(market_name, window_ts)
            );

            CREATE TABLE IF NOT EXISTS shadow_trades (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp           TEXT NOT NULL DEFAULT (datetime('now')),
                market_name         TEXT NOT NULL,
                window_ts           INTEGER NOT NULL,
                window_seconds      INTEGER NOT NULL,
                open_price          TEXT NOT NULL,
                close_price         TEXT,
                delta_at_entry      REAL NOT NULL,
                direction_at_entry  TEXT NOT NULL,
                actual_outcome      TEXT,
                would_have_won      INTEGER,
                best_ask_at_entry   TEXT,
                seconds_before_close INTEGER NOT NULL,
                was_traded          INTEGER NOT NULL DEFAULT 0,
                skip_reason         TEXT,
                UNIQUE(market_name, window_ts)
            );

            CREATE TABLE IF NOT EXISTS shadow_timing (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                window_id               TEXT NOT NULL,
                market_name             TEXT NOT NULL,
                t_seconds               INTEGER NOT NULL,
                delta_pct               REAL NOT NULL,
                direction               TEXT NOT NULL,
                binance_price           REAL NOT NULL,
                window_open_price       REAL NOT NULL,
                best_ask_winner         REAL,
                best_ask_loser          REAL,
                ask_depth_winner        INTEGER DEFAULT 0,
                ask_depth_loser         INTEGER DEFAULT 0,
                total_ask_size_winner   REAL DEFAULT 0.0,
                could_have_filled       INTEGER DEFAULT 0,
                hypothetical_entry_price REAL,
                would_have_won          INTEGER,
                actual_outcome          TEXT,
                timestamp               TEXT NOT NULL,
                UNIQUE(window_id, market_name, t_seconds)
            );

            CREATE TABLE IF NOT EXISTS resolution_audit (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                market_name      TEXT NOT NULL,
                window_ts        INTEGER NOT NULL,
                slug             TEXT NOT NULL,
                our_open         TEXT NOT NULL,
                our_close        TEXT NOT NULL,
                our_direction    TEXT NOT NULL,
                poly_resolution  TEXT,
                matched          INTEGER,
                price_source     TEXT NOT NULL DEFAULT 'chainlink',
                checked_at       TEXT,
                created_at       TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(market_name, window_ts)
            );
            CREATE INDEX IF NOT EXISTS idx_resolution_audit_market
                ON resolution_audit(market_name);

            CREATE TABLE IF NOT EXISTS price_divergence (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp        TEXT NOT NULL DEFAULT (datetime('now')),
                asset            TEXT NOT NULL,
                bybit_price      REAL NOT NULL,
                chainlink_price  REAL NOT NULL,
                diff_usd         REAL NOT NULL,
                diff_pct         REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS active_positions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                market_name      TEXT NOT NULL,
                asset            TEXT NOT NULL,
                window_seconds   INTEGER NOT NULL,
                window_ts        INTEGER NOT NULL,
                slug             TEXT NOT NULL,
                direction        TEXT NOT NULL,
                token_id         TEXT NOT NULL,
                condition_id     TEXT NOT NULL DEFAULT '',
                neg_risk         INTEGER NOT NULL DEFAULT 0,
                edge_score       REAL NOT NULL DEFAULT 0.0,
                delta_pct        TEXT NOT NULL DEFAULT '0',
                initial_price    TEXT NOT NULL,
                current_price    TEXT NOT NULL,
                max_price        TEXT NOT NULL,
                best_ask_at_entry TEXT NOT NULL DEFAULT '0',
                contracts        TEXT NOT NULL,
                bet_size_usd     TEXT NOT NULL DEFAULT '0',
                order_id         TEXT,
                tighten_count    INTEGER NOT NULL DEFAULT 0,
                open_price       TEXT NOT NULL DEFAULT '0',
                status           TEXT NOT NULL DEFAULT 'PENDING',
                fill_price       TEXT,
                fill_size        TEXT,
                mode             TEXT NOT NULL,
                created_at       TEXT NOT NULL DEFAULT (datetime('now'))
            );",
        )
        .context("Failed to initialize database schema")?;

        // Drop legacy columns that conflict with the current schema.
        let drop_migrations = [
            "ALTER TABLE trades DROP COLUMN market_slug",
        ];
        for sql in &drop_migrations {
            let _ = conn.execute(sql, []);
        }

        // Migrate existing databases that lack newer columns (silently skip if already present).
        let migrations = [
            "ALTER TABLE trades ADD COLUMN market_name TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN asset TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN window_seconds INTEGER NOT NULL DEFAULT 300",
            "ALTER TABLE trades ADD COLUMN slug TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN edge_score REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE trades ADD COLUMN seconds_remaining REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE trades ADD COLUMN contracts TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN bet_size_usd TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN open_price TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN close_price TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN initial_price TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN final_price TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN tighten_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE trades ADD COLUMN best_ask_at_entry TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN fill_price TEXT",
            "ALTER TABLE trades ADD COLUMN skip_reason TEXT",
            "ALTER TABLE trades ADD COLUMN token_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN order_id TEXT NOT NULL DEFAULT ''",
            "ALTER TABLE trades ADD COLUMN best_bid TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN spread TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN depth_at_ask TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN fill_latency_ms INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE trades ADD COLUMN market_type TEXT NOT NULL DEFAULT '5min'",
            "ALTER TABLE shadow_trades ADD COLUMN velocity_5s REAL",
            "ALTER TABLE shadow_trades ADD COLUMN velocity_15s REAL",
            "ALTER TABLE shadow_trades ADD COLUMN range_30s REAL",
            "ALTER TABLE shadow_trades ADD COLUMN signal_score REAL",
            // TA indicators on shadow_timing for T-120 analysis
            "ALTER TABLE shadow_timing ADD COLUMN velocity_5s REAL",
            "ALTER TABLE shadow_timing ADD COLUMN velocity_15s REAL",
            "ALTER TABLE shadow_timing ADD COLUMN acceleration REAL",
            "ALTER TABLE shadow_timing ADD COLUMN range_30s REAL",
            "ALTER TABLE shadow_timing ADD COLUMN signal_score REAL",
            "ALTER TABLE shadow_timing ADD COLUMN tick_count_30s INTEGER DEFAULT 0",
            // Orderbook imbalance and volume ratio for signal analysis
            "ALTER TABLE shadow_timing ADD COLUMN ob_imbalance REAL",
            "ALTER TABLE shadow_timing ADD COLUMN volume_ratio REAL",
            "ALTER TABLE shadow_trades ADD COLUMN ob_imbalance REAL",
            "ALTER TABLE shadow_trades ADD COLUMN volume_ratio REAL",
            "ALTER TABLE trades ADD COLUMN platform TEXT DEFAULT 'polymarket'",
            "ALTER TABLE shadow_trades ADD COLUMN platform TEXT DEFAULT 'polymarket'",
        ];
        for sql in &migrations {
            let _ = conn.execute(sql, []);
        }

        // Create indexes after migrations so columns exist for old databases.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_trades_window ON trades(window_ts);
             CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades(outcome);
             CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
             CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_name);
             CREATE INDEX IF NOT EXISTS idx_scanlog_window ON scanner_log(window_ts);
             CREATE INDEX IF NOT EXISTS idx_scanlog_market ON scanner_log(market_name);
             CREATE INDEX IF NOT EXISTS idx_scanlog_ts ON scanner_log(timestamp);
             CREATE INDEX IF NOT EXISTS idx_winsummary_mkt ON window_summary(market_name, window_ts);
             CREATE INDEX IF NOT EXISTS idx_shadow_mkt ON shadow_trades(market_name, window_ts);
             CREATE INDEX IF NOT EXISTS idx_shadow_ts ON shadow_trades(window_ts);
             CREATE INDEX IF NOT EXISTS idx_shadow_timing_market ON shadow_timing(market_name);
             CREATE INDEX IF NOT EXISTS idx_shadow_timing_window ON shadow_timing(window_id);
             CREATE INDEX IF NOT EXISTS idx_shadow_timing_t ON shadow_timing(t_seconds);",
        )
        .context("Failed to create indexes")?;

        info!(path = %path.display(), "Trade database opened");

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO trades (
                timestamp, market_name, asset, market_type, window_seconds, window_ts, slug,
                mode, direction, token_id, order_id, initial_price, final_price,
                tighten_count, best_ask_at_entry, filled, fill_price, outcome,
                pnl, delta_pct, edge_score, seconds_remaining, contracts,
                bet_size_usd, open_price, close_price, skip_reason,
                best_bid, spread, depth_at_ask, fill_latency_ms
            ) VALUES (
                ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,
                ?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30,?31
            )",
            params![
                trade.timestamp.to_rfc3339(),
                trade.market_name,
                trade.asset,
                trade.market_type,
                trade.window_seconds as i64,
                trade.window_ts as i64,
                trade.slug,
                trade.mode,
                trade.direction,
                trade.token_id,
                trade.order_id,
                trade.initial_price.to_string(),
                trade.final_price.to_string(),
                trade.tighten_count,
                trade.best_ask_at_entry.to_string(),
                trade.filled as i32,
                trade.fill_price.map(|p| p.to_string()),
                trade.outcome,
                trade.pnl.to_string(),
                trade.delta_pct.to_string(),
                trade.edge_score,
                trade.seconds_remaining,
                trade.contracts.to_string(),
                trade.bet_size_usd.to_string(),
                trade.open_price.to_string(),
                trade.close_price.to_string(),
                trade.skip_reason,
                trade.best_bid.to_string(),
                trade.spread.to_string(),
                trade.depth_at_ask.to_string(),
                trade.fill_latency_ms,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn update_outcome(
        &self,
        order_id: &str,
        outcome: &str,
        pnl: Decimal,
        close_price: Decimal,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE trades SET outcome = ?1, pnl = ?2, close_price = ?3 WHERE order_id = ?4",
            params![outcome, pnl.to_string(), close_price.to_string(), order_id],
        )?;
        Ok(())
    }

    /// Get all filled trades for reconciliation (re-checking outcomes).
    /// Returns (order_id, slug, token_id, outcome, fill_price, contracts) for
    /// trades that were filled.
    pub fn get_filled_trades_for_reconciliation(&self) -> Result<Vec<(String, String, String, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT order_id, slug, token_id, outcome, COALESCE(fill_price, initial_price), contracts
             FROM trades
             WHERE filled = 1 AND order_id != ''
             ORDER BY id DESC"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row?);
        }
        Ok(result)
    }

    /// Get stats for today, grouped by market.
    #[allow(dead_code)]
    pub fn market_stats_today(&self) -> Result<Vec<MarketDayStats>> {
        let conn = self.conn.lock().unwrap();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let mut stmt = conn.prepare(
            "SELECT market_name,
                    SUM(CASE WHEN filled = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN outcome = 'WIN' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN outcome = 'LOSS' THEN 1 ELSE 0 END),
                    COALESCE(SUM(CAST(pnl AS REAL)), 0)
             FROM trades
             WHERE date(timestamp) = ?1 AND market_name != ''
             GROUP BY market_name
             ORDER BY market_name",
        )?;

        let rows = stmt.query_map(params![today], |row| {
            Ok(MarketDayStats {
                market_name: row.get(0)?,
                fills: row.get(1)?,
                wins: row.get(2)?,
                losses: row.get(3)?,
                pnl: row
                    .get::<_, String>(4)
                    .unwrap_or_default()
                    .parse::<Decimal>()
                    .unwrap_or_default(),
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get overall stats for today.
    #[allow(dead_code)]
    pub fn stats_today(&self) -> Result<(u32, u32, u32, Decimal)> {
        let conn = self.conn.lock().unwrap();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let total: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM trades WHERE date(timestamp) = ?1 AND filled = 1",
                params![today],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let wins: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM trades WHERE date(timestamp) = ?1 AND outcome = 'WIN'",
                params![today],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let losses: u32 = conn
            .query_row(
                "SELECT COUNT(*) FROM trades WHERE date(timestamp) = ?1 AND outcome = 'LOSS'",
                params![today],
                |row| row.get(0),
            )
            .unwrap_or(0);

        let pnl_str: String = conn
            .query_row(
                "SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0) FROM trades WHERE date(timestamp) = ?1",
                params![today],
                |row| row.get(0),
            )
            .unwrap_or_else(|_| "0".to_string());

        let pnl: Decimal = pnl_str.parse().unwrap_or_default();

        Ok((total, wins, losses, pnl))
    }

    /// Persist a position so it survives bot restarts.
    pub fn save_position(
        &self,
        market_name: &str,
        asset: &str,
        window_seconds: u64,
        window_ts: u64,
        slug: &str,
        direction: &str,
        token_id: &str,
        condition_id: &str,
        neg_risk: bool,
        edge_score: f64,
        delta_pct: &str,
        initial_price: &str,
        current_price: &str,
        max_price: &str,
        best_ask_at_entry: &str,
        contracts: &str,
        bet_size_usd: &str,
        order_id: Option<&str>,
        tighten_count: u32,
        open_price: &str,
        status: &str,
        fill_price: Option<&str>,
        fill_size: Option<&str>,
        mode: &str,
    ) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO active_positions (
                market_name, asset, window_seconds, window_ts, slug, direction,
                token_id, condition_id, neg_risk, edge_score, delta_pct,
                initial_price, current_price, max_price, best_ask_at_entry,
                contracts, bet_size_usd, order_id, tighten_count, open_price,
                status, fill_price, fill_size, mode
            ) VALUES (
                ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24
            )",
            params![
                market_name, asset, window_seconds as i64, window_ts as i64,
                slug, direction, token_id, condition_id, neg_risk as i32,
                edge_score, delta_pct, initial_price, current_price, max_price,
                best_ask_at_entry, contracts, bet_size_usd, order_id,
                tighten_count, open_price, status, fill_price, fill_size, mode,
            ],
        )?;
        Ok(conn.last_insert_rowid())
    }

    /// Update status of a persisted position (e.g. PENDING → FILLED → SETTLED).
    pub fn update_position_status(
        &self,
        db_id: i64,
        status: &str,
        fill_price: Option<&str>,
        fill_size: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE active_positions SET status = ?1, fill_price = COALESCE(?2, fill_price), fill_size = COALESCE(?3, fill_size) WHERE id = ?4",
            params![status, fill_price, fill_size, db_id],
        )?;
        Ok(())
    }

    /// Remove a position from active tracking (after settlement or confirmed expiry).
    pub fn remove_position(&self, db_id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM active_positions WHERE id = ?1", params![db_id])?;
        Ok(())
    }

    /// Load all active (non-settled, non-expired) positions for recovery after restart.
    pub fn load_active_positions(&self) -> Result<Vec<PersistedPosition>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, market_name, asset, window_seconds, window_ts, slug, direction,
                    token_id, condition_id, neg_risk, edge_score, delta_pct,
                    initial_price, current_price, max_price, best_ask_at_entry,
                    contracts, bet_size_usd, order_id, tighten_count, open_price,
                    status, fill_price, fill_size, mode
             FROM active_positions
             WHERE status IN ('PENDING', 'FILLED')"
        )?;

        let rows = stmt.query_map([], |row: &rusqlite::Row<'_>| {
            Ok(PersistedPosition {
                id: row.get(0)?,
                market_name: row.get(1)?,
                asset: row.get(2)?,
                window_seconds: row.get::<_, i64>(3)? as u64,
                window_ts: row.get::<_, i64>(4)? as u64,
                slug: row.get(5)?,
                direction: row.get(6)?,
                token_id: row.get(7)?,
                condition_id: row.get(8)?,
                neg_risk: row.get::<_, i32>(9)? != 0,
                edge_score: row.get(10)?,
                delta_pct: row.get(11)?,
                initial_price: row.get(12)?,
                current_price: row.get(13)?,
                max_price: row.get(14)?,
                best_ask_at_entry: row.get(15)?,
                contracts: row.get(16)?,
                bet_size_usd: row.get(17)?,
                order_id: row.get(18)?,
                tighten_count: row.get::<_, u32>(19)?,
                open_price: row.get(20)?,
                status: row.get(21)?,
                fill_price: row.get(22)?,
                fill_size: row.get(23)?,
                mode: row.get(24)?,
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn insert_scanner_log(
        &self,
        eval: &crate::scanner::ScanEvaluation,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO scanner_log (
                market_name, window_ts, secs_remaining, direction, delta_pct,
                open_price, current_price, best_ask, best_bid, spread,
                depth_at_ask, suggested_entry, max_entry, edge_score, result, detail
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                eval.market_name,
                eval.window_ts as i64,
                eval.secs_remaining as i64,
                eval.direction,
                eval.delta_pct,
                eval.open_price.map(|d| d.to_string()),
                eval.current_price.map(|d| d.to_string()),
                eval.best_ask.map(|d| d.to_string()),
                eval.best_bid.map(|d| d.to_string()),
                eval.spread.map(|d| d.to_string()),
                eval.depth_at_ask.map(|d| d.to_string()),
                eval.suggested_entry.map(|d| d.to_string()),
                eval.max_entry.map(|d| d.to_string()),
                eval.edge_score,
                eval.result,
                eval.detail,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_window_summary(
        &self,
        market_name: &str,
        window_ts: u64,
        open_price: &str,
        close_price: &str,
        max_delta_pct: f64,
        peak_direction: Option<&str>,
        trade_placed: bool,
        trade_won: Option<bool>,
        entry_price: Option<&str>,
        pnl: Option<&str>,
        skip_reason: Option<&str>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO window_summary (
                market_name, window_ts, open_price, close_price, max_delta_pct,
                peak_direction, trade_placed, trade_won, entry_price, pnl, skip_reason
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
            ON CONFLICT(market_name, window_ts) DO UPDATE SET
                close_price = excluded.close_price,
                max_delta_pct = MAX(window_summary.max_delta_pct, excluded.max_delta_pct),
                peak_direction = COALESCE(excluded.peak_direction, window_summary.peak_direction),
                trade_placed = MAX(window_summary.trade_placed, excluded.trade_placed),
                trade_won = COALESCE(excluded.trade_won, window_summary.trade_won),
                entry_price = COALESCE(excluded.entry_price, window_summary.entry_price),
                pnl = COALESCE(excluded.pnl, window_summary.pnl),
                skip_reason = COALESCE(excluded.skip_reason, window_summary.skip_reason)",
            params![
                market_name,
                window_ts as i64,
                open_price,
                close_price,
                max_delta_pct,
                peak_direction,
                trade_placed as i32,
                trade_won.map(|w| w as i32),
                entry_price,
                pnl,
                skip_reason,
            ],
        )?;
        Ok(())
    }

    /// Record a shadow trade for a window (first write wins via INSERT OR IGNORE).
    pub fn insert_shadow_trade(
        &self,
        market_name: &str,
        window_ts: u64,
        window_seconds: u64,
        open_price: &str,
        delta_at_entry: f64,
        direction_at_entry: &str,
        best_ask_at_entry: Option<&str>,
        seconds_before_close: u64,
        was_traded: bool,
        skip_reason: Option<&str>,
        velocity_5s: Option<f64>,
        velocity_15s: Option<f64>,
        range_30s: Option<f64>,
        signal_score: Option<f64>,
        ob_imbalance: Option<f64>,
        volume_ratio: Option<f64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO shadow_trades (
                market_name, window_ts, window_seconds, open_price,
                delta_at_entry, direction_at_entry, best_ask_at_entry,
                seconds_before_close, was_traded, skip_reason,
                velocity_5s, velocity_15s, range_30s, signal_score,
                ob_imbalance, volume_ratio
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)",
            params![
                market_name,
                window_ts as i64,
                window_seconds as i64,
                open_price,
                delta_at_entry,
                direction_at_entry,
                best_ask_at_entry,
                seconds_before_close as i64,
                was_traded as i32,
                skip_reason,
                velocity_5s,
                velocity_15s,
                range_30s,
                signal_score,
                ob_imbalance,
                volume_ratio,
            ],
        )?;
        Ok(())
    }

    /// Settle a shadow trade with the actual close price and outcome.
    pub fn settle_shadow_trade(
        &self,
        market_name: &str,
        window_ts: u64,
        close_price: &str,
        actual_direction: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE shadow_trades SET
                close_price = ?1,
                actual_outcome = ?2,
                would_have_won = CASE
                    WHEN ?2 = 'FLAT' THEN 0
                    WHEN direction_at_entry = ?2 THEN 1
                    ELSE 0
                END
             WHERE market_name = ?3 AND window_ts = ?4 AND close_price IS NULL",
            params![close_price, actual_direction, market_name, window_ts as i64],
        )?;
        Ok(())
    }

    /// Mark an existing shadow trade as actually traded, updating entry data to
    /// reflect the opportunity that triggered the real trade (not the first eval).
    #[allow(clippy::too_many_arguments)]
    pub fn mark_shadow_traded(
        &self,
        market_name: &str,
        window_ts: u64,
        delta_at_entry: f64,
        direction_at_entry: &str,
        best_ask_at_entry: Option<&str>,
        seconds_before_close: u64,
        signal_score: Option<f64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE shadow_trades SET was_traded = 1,
                delta_at_entry = ?3,
                direction_at_entry = ?4,
                best_ask_at_entry = ?5,
                seconds_before_close = ?6,
                signal_score = ?7
             WHERE market_name = ?1 AND window_ts = ?2",
            params![
                market_name,
                window_ts as i64,
                delta_at_entry,
                direction_at_entry,
                best_ask_at_entry,
                seconds_before_close as i64,
                signal_score,
            ],
        )?;
        Ok(())
    }

    /// Record a shadow timing snapshot for a specific T-second.
    #[allow(clippy::too_many_arguments)]
    pub fn insert_shadow_timing(
        &self,
        window_id: &str,
        market_name: &str,
        t_seconds: u64,
        delta_pct: f64,
        direction: &str,
        binance_price: f64,
        window_open_price: f64,
        best_ask_winner: Option<f64>,
        best_ask_loser: Option<f64>,
        ask_depth_winner: i64,
        ask_depth_loser: i64,
        total_ask_size_winner: f64,
        could_have_filled: bool,
        hypothetical_entry_price: Option<f64>,
        velocity_5s: Option<f64>,
        velocity_15s: Option<f64>,
        acceleration: Option<f64>,
        range_30s: Option<f64>,
        signal_score: Option<f64>,
        tick_count_30s: i64,
        ob_imbalance: Option<f64>,
        volume_ratio: Option<f64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO shadow_timing (
                window_id, market_name, t_seconds, delta_pct, direction,
                binance_price, window_open_price,
                best_ask_winner, best_ask_loser, ask_depth_winner, ask_depth_loser,
                total_ask_size_winner, could_have_filled, hypothetical_entry_price,
                would_have_won, actual_outcome,
                velocity_5s, velocity_15s, acceleration, range_30s, signal_score, tick_count_30s,
                ob_imbalance, volume_ratio,
                timestamp
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,NULL,NULL,?15,?16,?17,?18,?19,?20,?21,?22,?23)",
            params![
                window_id,
                market_name,
                t_seconds as i64,
                delta_pct,
                direction,
                binance_price,
                window_open_price,
                best_ask_winner,
                best_ask_loser,
                ask_depth_winner,
                ask_depth_loser,
                total_ask_size_winner,
                could_have_filled as i64,
                hypothetical_entry_price,
                velocity_5s,
                velocity_15s,
                acceleration,
                range_30s,
                signal_score,
                tick_count_30s,
                ob_imbalance,
                volume_ratio,
                chrono::Utc::now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    /// After a window resolves, update all shadow_timing rows with the actual outcome.
    /// Wins are NOT counted if the predicted token ask was > $0.95 (false winrate inflation).
    pub fn settle_shadow_timing(&self, window_id: &str, actual_outcome: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE shadow_timing SET
                actual_outcome = ?1,
                would_have_won = CASE
                    WHEN direction = ?1 AND (best_ask_winner IS NULL OR best_ask_winner <= 0.95) THEN 1
                    ELSE 0
                END
             WHERE window_id = ?2 AND actual_outcome IS NULL",
            params![actual_outcome, window_id],
        )?;
        Ok(())
    }

    /// Re-settle shadow_trades using the authoritative Polymarket resolution,
    /// overwriting the earlier price-based inference.
    pub fn resettle_shadow_with_poly(
        &self,
        market_name: &str,
        window_ts: u64,
        poly_resolution: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "UPDATE shadow_trades SET
                actual_outcome = ?1,
                would_have_won = CASE
                    WHEN ?1 = 'FLAT' THEN 0
                    WHEN direction_at_entry = ?1 THEN 1
                    ELSE 0
                END
             WHERE market_name = ?2 AND window_ts = ?3",
            params![poly_resolution, market_name, window_ts as i64],
        )?;
        Ok(affected)
    }

    /// Re-settle shadow_timing rows using the authoritative Polymarket resolution,
    /// overwriting the earlier price-based inference.
    /// Wins are NOT counted if the predicted token ask was > $0.95 (false winrate inflation).
    pub fn resettle_shadow_timing_with_poly(
        &self,
        market_name: &str,
        window_ts: u64,
        poly_resolution: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let suffix = format!("%-{}", window_ts);
        let affected = conn.execute(
            "UPDATE shadow_timing SET
                actual_outcome = ?1,
                would_have_won = CASE
                    WHEN direction = ?1 AND (best_ask_winner IS NULL OR best_ask_winner <= 0.95) THEN 1
                    ELSE 0
                END
             WHERE market_name = ?2 AND window_id LIKE ?3",
            params![poly_resolution, market_name, suffix],
        )?;
        Ok(affected)
    }

    pub fn insert_resolution_audit(
        &self,
        market_name: &str,
        window_ts: u64,
        slug: &str,
        our_open: &str,
        our_close: &str,
        our_direction: &str,
        price_source: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO resolution_audit
                (market_name, window_ts, slug, our_open, our_close, our_direction, price_source)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![market_name, window_ts as i64, slug, our_open, our_close, our_direction, price_source],
        )?;
        Ok(())
    }

    pub fn update_resolution_audit(
        &self,
        market_name: &str,
        window_ts: u64,
        poly_resolution: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE resolution_audit SET
                poly_resolution = ?1,
                matched = CASE WHEN our_direction = ?1 THEN 1 ELSE 0 END,
                checked_at = datetime('now')
             WHERE market_name = ?2 AND window_ts = ?3 AND poly_resolution IS NULL",
            params![poly_resolution, market_name, window_ts as i64],
        )?;
        Ok(())
    }

    /// Backfill: re-settle all shadow data using known Polymarket resolutions.
    /// Returns the number of shadow_trades rows corrected.
    pub fn backfill_shadow_from_poly(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT market_name, window_ts, poly_resolution FROM resolution_audit
             WHERE poly_resolution IS NOT NULL"
        )?;
        let rows: Vec<(String, i64, String)> = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?.filter_map(|r| r.ok()).collect();
        drop(stmt);

        let mut corrected = 0usize;
        for (market_name, window_ts, poly_res) in &rows {
            let n = conn.execute(
                "UPDATE shadow_trades SET
                    actual_outcome = ?1,
                    would_have_won = CASE
                        WHEN ?1 = 'FLAT' THEN 0
                        WHEN direction_at_entry = ?1 THEN 1
                        ELSE 0
                    END
                 WHERE market_name = ?2 AND window_ts = ?3",
                params![poly_res, market_name, window_ts],
            )?;
            corrected += n;

            let suffix = format!("%-{}", window_ts);
            conn.execute(
                "UPDATE shadow_timing SET
                    actual_outcome = ?1,
                    would_have_won = CASE WHEN direction = ?1 THEN 1 ELSE 0 END
                 WHERE market_name = ?2 AND window_id LIKE ?3",
                params![poly_res, market_name, suffix],
            )?;
        }
        Ok(corrected)
    }

    pub fn get_resolution_audit_open(&self, market_name: &str, window_ts: u64) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT our_open FROM resolution_audit WHERE market_name = ?1 AND window_ts = ?2"
        )?;
        let result = stmt.query_row(params![market_name, window_ts as i64], |row| {
            row.get::<_, String>(0)
        });
        match result {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get all audit rows where we haven't checked Polymarket's resolution yet.
    pub fn pending_resolution_audits(&self) -> Result<Vec<(String, u64, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT market_name, window_ts, slug FROM resolution_audit
             WHERE poly_resolution IS NULL
             ORDER BY window_ts ASC"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    pub fn insert_price_divergence(
        &self,
        asset: &str,
        bybit_price: f64,
        chainlink_price: f64,
    ) -> Result<()> {
        let diff_usd = bybit_price - chainlink_price;
        let diff_pct = if chainlink_price.abs() > 1e-12 {
            (diff_usd / chainlink_price) * 100.0
        } else {
            0.0
        };
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO price_divergence (asset, bybit_price, chainlink_price, diff_usd, diff_pct)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![asset, bybit_price, chainlink_price, diff_usd, diff_pct],
        )?;
        Ok(())
    }

    pub fn daily_analytics(&self) -> Result<DailyAnalytics> {
        let conn = self.conn.lock().unwrap();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let mut delta_buckets = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT
                    CASE
                        WHEN CAST(delta_pct AS REAL) < 0.08 THEN '0.06-0.08'
                        WHEN CAST(delta_pct AS REAL) < 0.10 THEN '0.08-0.10'
                        WHEN CAST(delta_pct AS REAL) < 0.15 THEN '0.10-0.15'
                        ELSE '0.15+'
                    END as bucket,
                    COUNT(*) as trades,
                    SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(COALESCE(SUM(CAST(pnl AS REAL)), 0), 4) as total_pnl
                 FROM trades
                 WHERE date(timestamp) = ?1 AND filled = 1
                 GROUP BY bucket ORDER BY bucket"
            )?;
            let rows = stmt.query_map(params![today], |row| {
                Ok(AnalyticsBucket {
                    label: row.get(0)?,
                    trades: row.get(1)?,
                    wins: row.get(2)?,
                    pnl: row.get::<_, String>(3).unwrap_or_default().parse().unwrap_or(0.0),
                })
            })?;
            for r in rows { if let Ok(b) = r { delta_buckets.push(b); } }
        }

        let mut timing_buckets = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT
                    CASE
                        WHEN seconds_remaining > 15 THEN 'T-20 to T-15'
                        WHEN seconds_remaining > 10 THEN 'T-15 to T-10'
                        WHEN seconds_remaining > 5  THEN 'T-10 to T-5'
                        ELSE 'T-5 to T-4'
                    END as bucket,
                    COUNT(*) as trades,
                    SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(COALESCE(SUM(CAST(pnl AS REAL)), 0), 4) as total_pnl
                 FROM trades
                 WHERE date(timestamp) = ?1 AND filled = 1
                 GROUP BY bucket ORDER BY bucket"
            )?;
            let rows = stmt.query_map(params![today], |row| {
                Ok(AnalyticsBucket {
                    label: row.get(0)?,
                    trades: row.get(1)?,
                    wins: row.get(2)?,
                    pnl: row.get::<_, String>(3).unwrap_or_default().parse().unwrap_or(0.0),
                })
            })?;
            for r in rows { if let Ok(b) = r { timing_buckets.push(b); } }
        }

        let mut market_stats = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT market_name,
                    COUNT(*) as trades,
                    SUM(CASE WHEN outcome='WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN outcome='LOSS' THEN 1 ELSE 0 END) as losses,
                    ROUND(COALESCE(SUM(CAST(pnl AS REAL)), 0), 4) as total_pnl
                 FROM trades
                 WHERE date(timestamp) = ?1 AND filled = 1 AND market_name != ''
                 GROUP BY market_name ORDER BY market_name"
            )?;
            let rows = stmt.query_map(params![today], |row| {
                Ok(MarketDayStats {
                    market_name: row.get(0)?,
                    fills: row.get(1)?,
                    wins: row.get(2)?,
                    losses: row.get(3)?,
                    pnl: row.get::<_, String>(4).unwrap_or_default().parse::<Decimal>().unwrap_or_default(),
                })
            })?;
            for r in rows { if let Ok(s) = r { market_stats.push(s); } }
        }

        let missed_windows: u32 = conn.query_row(
            "SELECT COUNT(*) FROM window_summary WHERE date(datetime(window_ts, 'unixepoch')) = ?1 AND trade_placed = 0",
            params![today],
            |row| row.get(0),
        ).unwrap_or(0);

        Ok(DailyAnalytics {
            delta_buckets,
            timing_buckets,
            market_stats,
            missed_windows,
        })
    }
}

#[derive(Debug)]
pub struct AnalyticsBucket {
    pub label: String,
    pub trades: u32,
    pub wins: u32,
    pub pnl: f64,
}

#[derive(Debug)]
pub struct DailyAnalytics {
    pub delta_buckets: Vec<AnalyticsBucket>,
    pub timing_buckets: Vec<AnalyticsBucket>,
    pub market_stats: Vec<MarketDayStats>,
    pub missed_windows: u32,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct MarketDayStats {
    pub market_name: String,
    pub fills: u32,
    pub wins: u32,
    pub losses: u32,
    pub pnl: Decimal,
}
