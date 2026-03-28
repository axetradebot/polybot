use rusqlite::Connection;

fn main() {
    let db_path = std::env::args().nth(1).unwrap_or_else(|| "trades.db".to_string());
    let conn = Connection::open(&db_path).expect("Failed to open database");

    println!("\n=== SHADOW TRADE WIN RATE BY DELTA BUCKET ===\n");
    println!(
        "{:<16} {:<12} {:>6} {:>6} {:>8}",
        "Market", "Delta", "Total", "Wins", "Win%"
    );
    println!("{}", "-".repeat(52));

    let mut stmt = conn
        .prepare(
            "SELECT market_name,
                    CASE
                        WHEN delta_at_entry < 0.05 THEN '<0.05'
                        WHEN delta_at_entry < 0.07 THEN '0.05-0.07'
                        WHEN delta_at_entry < 0.10 THEN '0.07-0.10'
                        WHEN delta_at_entry < 0.15 THEN '0.10-0.15'
                        WHEN delta_at_entry < 0.20 THEN '0.15-0.20'
                        WHEN delta_at_entry < 0.30 THEN '0.20-0.30'
                        ELSE '0.30+'
                    END as delta_bucket,
                    COUNT(*) as total,
                    SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins
             FROM shadow_trades
             WHERE actual_outcome IS NOT NULL
             GROUP BY market_name, delta_bucket
             ORDER BY market_name, delta_bucket",
        )
        .unwrap();

    let rows = stmt
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap();

    for row in rows {
        let (market, bucket, total, wins) = row.unwrap();
        let pct = if total > 0 {
            100.0 * wins as f64 / total as f64
        } else {
            0.0
        };
        println!(
            "{:<16} {:<12} {:>6} {:>6} {:>7.1}%",
            market, bucket, total, wins, pct
        );
    }

    println!("\n\n=== SHADOW TRADE WIN RATE BY MARKET (overall) ===\n");
    println!(
        "{:<16} {:>6} {:>6} {:>6} {:>8}",
        "Market", "Total", "Wins", "Loss", "Win%"
    );
    println!("{}", "-".repeat(46));

    let mut stmt2 = conn
        .prepare(
            "SELECT market_name,
                    COUNT(*) as total,
                    SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN would_have_won=0 THEN 1 ELSE 0 END) as losses
             FROM shadow_trades
             WHERE actual_outcome IS NOT NULL
             GROUP BY market_name
             ORDER BY market_name",
        )
        .unwrap();

    let rows2 = stmt2
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })
        .unwrap();

    for row in rows2 {
        let (market, total, wins, losses) = row.unwrap();
        let pct = if total > 0 {
            100.0 * wins as f64 / total as f64
        } else {
            0.0
        };
        println!(
            "{:<16} {:>6} {:>6} {:>6} {:>7.1}%",
            market, total, wins, losses, pct
        );
    }

    println!("\n\n=== SHADOW TIMING: WIN RATE BY T-SECONDS ===\n");
    println!(
        "{:<16} {:>4} {:>6} {:>6} {:>8} {:>10}",
        "Market", "T", "Total", "Wins", "Win%", "AvgDelta%"
    );
    println!("{}", "-".repeat(56));

    let mut stmt3 = conn
        .prepare(
            "SELECT market_name,
                    t_seconds,
                    COUNT(*) as total,
                    SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
                    AVG(delta_pct) as avg_delta
             FROM shadow_timing
             WHERE actual_outcome IS NOT NULL
             GROUP BY market_name, t_seconds
             ORDER BY market_name, t_seconds DESC",
        )
        .unwrap();

    let rows3 = stmt3
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, f64>(4)?,
            ))
        })
        .unwrap();

    for row in rows3 {
        let (market, t, total, wins, avg_delta) = row.unwrap();
        let pct = if total > 0 {
            100.0 * wins as f64 / total as f64
        } else {
            0.0
        };
        println!(
            "{:<16} {:>4} {:>6} {:>6} {:>7.1}% {:>9.4}%",
            market, t, total, wins, pct, avg_delta
        );
    }

    println!("\n\n=== BEST T-SECOND PER MARKET (highest win rate, min 5 samples) ===\n");
    println!(
        "{:<16} {:>6} {:>6} {:>6} {:>8} {:>10}",
        "Market", "BestT", "Total", "Wins", "Win%", "AvgDelta%"
    );
    println!("{}", "-".repeat(58));

    let mut stmt4 = conn
        .prepare(
            "SELECT market_name, t_seconds, total, wins,
                    ROUND(100.0*wins/total, 1) as win_pct, avg_delta
             FROM (
                 SELECT market_name,
                        t_seconds,
                        COUNT(*) as total,
                        SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
                        AVG(delta_pct) as avg_delta,
                        ROW_NUMBER() OVER (
                            PARTITION BY market_name
                            ORDER BY CAST(SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) AS REAL)/COUNT(*) DESC,
                                     COUNT(*) DESC
                        ) as rn
                 FROM shadow_timing
                 WHERE actual_outcome IS NOT NULL
                 GROUP BY market_name, t_seconds
                 HAVING COUNT(*) >= 5
             )
             WHERE rn = 1
             ORDER BY market_name",
        )
        .unwrap();

    let rows4 = stmt4
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, f64>(4)?,
                row.get::<_, f64>(5)?,
            ))
        })
        .unwrap();

    for row in rows4 {
        let (market, t, total, wins, pct, avg_delta) = row.unwrap();
        println!(
            "{:<16} {:>6} {:>6} {:>6} {:>7.1}% {:>9.4}%",
            market, t, total, wins, pct, avg_delta
        );
    }

    println!();
}
