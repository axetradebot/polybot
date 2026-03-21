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
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp       TEXT NOT NULL,
                window_ts       INTEGER NOT NULL,
                market_slug     TEXT NOT NULL,
                direction       TEXT NOT NULL,
                token_id        TEXT NOT NULL,
                order_id        TEXT NOT NULL,
                tier_name       TEXT NOT NULL DEFAULT 'ob_aware',
                entry_price     TEXT NOT NULL,
                size            TEXT NOT NULL,
                cost_usd        TEXT NOT NULL,
                outcome         TEXT NOT NULL DEFAULT 'PENDING',
                pnl             TEXT NOT NULL DEFAULT '0',
                btc_open_price  TEXT NOT NULL,
                btc_close_price TEXT NOT NULL DEFAULT '0',
                delta_pct       TEXT NOT NULL,
                seconds_at_entry TEXT NOT NULL DEFAULT '0',
                mode            TEXT NOT NULL,
                filled          INTEGER NOT NULL DEFAULT 0,
                initial_price   TEXT NOT NULL DEFAULT '0',
                tighten_count   INTEGER NOT NULL DEFAULT 0,
                best_ask_at_entry TEXT NOT NULL DEFAULT '0',
                skip_reason     TEXT
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

            CREATE INDEX IF NOT EXISTS idx_trades_window ON trades(window_ts);
            CREATE INDEX IF NOT EXISTS idx_trades_outcome ON trades(outcome);
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
            CREATE INDEX IF NOT EXISTS idx_trades_tier ON trades(tier_name);",
        )
        .context("Failed to initialize database schema")?;

        // Migrate existing databases that lack newer columns.
        let migrations = [
            "ALTER TABLE trades ADD COLUMN tier_name TEXT NOT NULL DEFAULT 'single'",
            "ALTER TABLE trades ADD COLUMN seconds_at_entry TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN initial_price TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN tighten_count INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE trades ADD COLUMN best_ask_at_entry TEXT NOT NULL DEFAULT '0'",
            "ALTER TABLE trades ADD COLUMN skip_reason TEXT",
        ];
        for sql in &migrations {
            let _ = conn.execute(sql, []);
        }

        info!(path = %path.display(), "Trade database opened");

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn insert_trade(&self, trade: &TradeRecord) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO trades (
                timestamp, window_ts, market_slug, direction, token_id,
                order_id, tier_name, entry_price, size, cost_usd, outcome, pnl,
                btc_open_price, btc_close_price, delta_pct, seconds_at_entry, mode, filled,
                initial_price, tighten_count, best_ask_at_entry, skip_reason
            ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22)",
            params![
                trade.timestamp.to_rfc3339(),
                trade.window_ts,
                trade.market_slug,
                trade.direction,
                trade.token_id,
                trade.order_id,
                trade.tier_name,
                trade.entry_price.to_string(),
                trade.size.to_string(),
                trade.cost_usd.to_string(),
                trade.outcome,
                trade.pnl.to_string(),
                trade.btc_open_price.to_string(),
                trade.btc_close_price.to_string(),
                trade.delta_pct.to_string(),
                trade.seconds_at_entry.to_string(),
                trade.mode,
                trade.filled as i32,
                trade.initial_price.to_string(),
                trade.tighten_count,
                trade.best_ask_at_entry.to_string(),
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
        btc_close_price: Decimal,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE trades SET outcome = ?1, pnl = ?2, btc_close_price = ?3 WHERE order_id = ?4",
            params![outcome, pnl.to_string(), btc_close_price.to_string(), order_id],
        )?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn daily_pnl(&self, date: &str) -> Result<Decimal> {
        let conn = self.conn.lock().unwrap();
        let result: String = conn
            .query_row(
                "SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0) FROM trades WHERE date(timestamp) = ?1",
                params![date],
                |row| row.get(0),
            )
            .unwrap_or_else(|_| "0".to_string());

        Ok(result.parse().unwrap_or_default())
    }

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

    #[allow(dead_code)]
    pub fn tier_stats_today(&self) -> Result<Vec<(String, u32, u32, u32, Decimal)>> {
        let conn = self.conn.lock().unwrap();
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();

        let mut stmt = conn.prepare(
            "SELECT tier_name,
                    SUM(CASE WHEN filled = 1 THEN 1 ELSE 0 END),
                    SUM(CASE WHEN outcome = 'WIN' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN outcome = 'LOSS' THEN 1 ELSE 0 END),
                    COALESCE(SUM(CAST(pnl AS REAL)), 0)
             FROM trades
             WHERE date(timestamp) = ?1
             GROUP BY tier_name
             ORDER BY tier_name",
        )?;

        let rows = stmt.query_map(params![today], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, u32>(2)?,
                row.get::<_, u32>(3)?,
                row.get::<_, String>(4)
                    .unwrap_or_default()
                    .parse::<Decimal>()
                    .unwrap_or_default(),
            ))
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn recent_trades(&self, limit: u32) -> Result<Vec<TradeRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, timestamp, window_ts, market_slug, direction, token_id,
                    order_id, tier_name, entry_price, size, cost_usd, outcome, pnl,
                    btc_open_price, btc_close_price, delta_pct, seconds_at_entry, mode, filled,
                    initial_price, tighten_count, best_ask_at_entry, skip_reason
             FROM trades ORDER BY id DESC LIMIT ?1",
        )?;

        let rows = stmt.query_map(params![limit], |row| {
            Ok(TradeRecord {
                id: Some(row.get::<_, i64>(0)?),
                timestamp: chrono::DateTime::parse_from_rfc3339(&row.get::<_, String>(1)?)
                    .unwrap_or_default()
                    .with_timezone(&chrono::Utc),
                window_ts: row.get::<_, i64>(2)? as u64,
                market_slug: row.get(3)?,
                direction: row.get(4)?,
                token_id: row.get(5)?,
                order_id: row.get(6)?,
                tier_name: row.get(7)?,
                entry_price: row.get::<_, String>(8)?.parse().unwrap_or_default(),
                size: row.get::<_, String>(9)?.parse().unwrap_or_default(),
                cost_usd: row.get::<_, String>(10)?.parse().unwrap_or_default(),
                outcome: row.get(11)?,
                pnl: row.get::<_, String>(12)?.parse().unwrap_or_default(),
                btc_open_price: row.get::<_, String>(13)?.parse().unwrap_or_default(),
                btc_close_price: row.get::<_, String>(14)?.parse().unwrap_or_default(),
                delta_pct: row.get::<_, String>(15)?.parse().unwrap_or_default(),
                seconds_at_entry: row.get::<_, String>(16)?.parse().unwrap_or_default(),
                mode: row.get(17)?,
                filled: row.get::<_, i32>(18)? != 0,
                initial_price: row.get::<_, String>(19).unwrap_or_default().parse().unwrap_or_default(),
                tighten_count: row.get::<_, u32>(20).unwrap_or(0),
                best_ask_at_entry: row.get::<_, String>(21).unwrap_or_default().parse().unwrap_or_default(),
                skip_reason: row.get::<_, Option<String>>(22).unwrap_or(None),
            })
        })?;

        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}
