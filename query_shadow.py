import sqlite3

conn = sqlite3.connect('trades.db')
c = conn.cursor()

print("=== SHADOW TRADES: Win Rate by Market ===")
c.execute("""
    SELECT market_name,
           COUNT(*) as total,
           SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
           SUM(CASE WHEN would_have_won=0 THEN 1 ELSE 0 END) as losses,
           ROUND(100.0 * SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
           ROUND(AVG(delta_at_entry), 4) as avg_delta,
           ROUND(AVG(best_ask_at_entry), 4) as avg_ask
    FROM shadow_trades
    WHERE actual_outcome IS NOT NULL
    GROUP BY market_name
    ORDER BY win_pct DESC
""")
for row in c.fetchall():
    wins = row[2] or 0
    losses = row[3] or 0
    wp = row[4] or 0
    ad = row[5] or 0
    aa = row[6] or 0
    print(f"  {row[0]:15s} | {row[1]:3d} total | {wins:2d}W/{losses:2d}L | {wp:5.1f}% | avg_delta={ad:.4f} | avg_ask={aa:.4f}")

print()
print("=== SHADOW TRADES: Win Rate by Delta Bucket ===")
c.execute("""
    SELECT CASE
        WHEN delta_at_entry < 0.10 THEN 'lt_0.10'
        WHEN delta_at_entry < 0.15 THEN '0.10-0.15'
        WHEN delta_at_entry < 0.20 THEN '0.15-0.20'
        WHEN delta_at_entry < 0.30 THEN '0.20-0.30'
        ELSE '0.30+'
    END as delta_bucket,
    COUNT(*) as total,
    SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
    ROUND(100.0 * SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
    ROUND(AVG(best_ask_at_entry), 4) as avg_ask
    FROM shadow_trades
    WHERE actual_outcome IS NOT NULL
    GROUP BY delta_bucket
    ORDER BY delta_bucket
""")
for row in c.fetchall():
    wins = row[2] or 0
    wp = row[3] or 0
    aa = row[4] or 0
    print(f"  {row[0]:12s} | {row[1]:3d} total | {wins:2d}W | {wp:5.1f}% | avg_ask={aa:.4f}")

print()
print("=== SHADOW TRADES: Win Rate by Ask Price Bucket ===")
c.execute("""
    SELECT CASE
        WHEN best_ask_at_entry < 0.40 THEN 'lt_0.40'
        WHEN best_ask_at_entry < 0.50 THEN '0.40-0.50'
        WHEN best_ask_at_entry < 0.60 THEN '0.50-0.60'
        WHEN best_ask_at_entry < 0.70 THEN '0.60-0.70'
        WHEN best_ask_at_entry < 0.80 THEN '0.70-0.80'
        ELSE '0.80+'
    END as price_bucket,
    COUNT(*) as total,
    SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
    ROUND(100.0 * SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct
    FROM shadow_trades
    WHERE actual_outcome IS NOT NULL
    GROUP BY price_bucket
    ORDER BY price_bucket
""")
for row in c.fetchall():
    wins = row[2] or 0
    wp = row[3] or 0
    print(f"  {row[0]:12s} | {row[1]:3d} total | {wins:2d}W | {wp:5.1f}%")

print()
print("=== SHADOW TIMING: Best T-seconds by Market ===")
c.execute("""
    SELECT market_name, t_seconds,
           COUNT(*) as total,
           SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) as wins,
           ROUND(100.0 * SUM(CASE WHEN would_have_won=1 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_pct,
           ROUND(AVG(delta_pct), 4) as avg_delta
    FROM shadow_timing
    WHERE actual_outcome IS NOT NULL
    GROUP BY market_name, t_seconds
    ORDER BY market_name, t_seconds
""")
for row in c.fetchall():
    wins = row[3] or 0
    wp = row[4] or 0
    ad = row[5] or 0
    print(f"  {row[0]:15s} | T-{row[1]:2d}s | {row[2]:3d} total | {wins:2d}W | {wp:5.1f}% | avg_delta={ad:.4f}")

print()
print("=== RECENT SHADOW TRADES (last 20) ===")
c.execute("""
    SELECT timestamp, market_name, direction_at_entry, delta_at_entry, 
           best_ask_at_entry, actual_outcome, would_have_won, was_traded
    FROM shadow_trades
    ORDER BY timestamp DESC
    LIMIT 20
""")
for row in c.fetchall():
    delta = row[3] or 0
    ask = row[4] or 0
    outcome = row[5] or '?'
    won = 'WIN' if row[6] == 1 else ('LOSS' if row[6] == 0 else '?')
    traded = 'LIVE' if row[7] == 1 else 'shadow'
    print(f"  {row[1]:15s} | {row[2]:5s} | delta={delta:.4f} | ask={ask:.4f} | outcome={outcome:5s} | {won:4s} | {traded}")

conn.close()
