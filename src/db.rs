use anyhow::{Context, Result};
use rust_decimal::Decimal;
use rusqlite::{params, Connection};
use std::path::Path;
use std::sync::Mutex;
use tracing::info;

use crate::types::TradeRecord;

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
            );",
        )
        .context("Failed to initialize database schema")?;

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
        ];
        for sql in &migrations {
            let _ = conn.execute(sql, []);
        }

        // Create indexes after migrations so columns exist for old databases.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_trades_window ON trades(window_ts);
             CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades(outcome);
             CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
             CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_name);",
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
                timestamp, market_name, asset, window_seconds, window_ts, slug,
                mode, direction, token_id, order_id, initial_price, final_price,
                tighten_count, best_ask_at_entry, filled, fill_price, outcome,
                pnl, delta_pct, edge_score, seconds_remaining, contracts,
                bet_size_usd, open_price, close_price, skip_reason
            ) VALUES (
                ?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,
                ?18,?19,?20,?21,?22,?23,?24,?25,?26
            )",
            params![
                trade.timestamp.to_rfc3339(),
                trade.market_name,
                trade.asset,
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
