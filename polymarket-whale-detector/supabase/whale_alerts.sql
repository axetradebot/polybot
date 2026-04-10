-- Run in Supabase SQL editor (PolyEdge AI project)

CREATE TABLE IF NOT EXISTS whale_alerts (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    trade_id TEXT UNIQUE NOT NULL,
    market_question TEXT NOT NULL,
    market_slug TEXT,
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    outcome TEXT NOT NULL,
    size NUMERIC NOT NULL,
    price NUMERIC NOT NULL,
    usd_value NUMERIC NOT NULL,
    wallet_address TEXT NOT NULL,
    wallet_trade_count INTEGER,
    insider_score NUMERIC,
    is_potential_insider BOOLEAN DEFAULT FALSE,
    price_before NUMERIC,
    price_after NUMERIC,
    price_impact_pct NUMERIC,
    is_cluster BOOLEAN DEFAULT FALSE,
    cluster_total_usd NUMERIC,
    cluster_trade_count INTEGER,
    polymarket_url TEXT
);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_created_at ON whale_alerts(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_whale_alerts_insider ON whale_alerts(is_potential_insider) WHERE is_potential_insider = TRUE;
CREATE INDEX IF NOT EXISTS idx_whale_alerts_usd ON whale_alerts(usd_value DESC);

-- Optional: enable Realtime for this table in Supabase Dashboard → Database → Replication
