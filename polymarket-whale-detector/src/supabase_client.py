"""Insert whale alerts into Supabase."""

from __future__ import annotations

import logging
import threading
from typing import Any

from supabase import Client, create_client

from .config import Config
from .trade_monitor import WhaleAlertPayload

log = logging.getLogger(__name__)

_sb_lock = threading.Lock()
_sb_instance: Client | None = None


def _client(cfg: Config) -> Client:
    global _sb_instance
    if _sb_instance is not None:
        return _sb_instance
    with _sb_lock:
        if _sb_instance is not None:
            return _sb_instance
        _sb_instance = create_client(cfg.supabase_url, cfg.supabase_service_key)
        return _sb_instance


def insert_whale_alert(cfg: Config, p: WhaleAlertPayload) -> bool:
    """Synchronous insert (runs in thread pool from async main)."""
    try:
        sb = _client(cfg)
        row: dict[str, Any] = {
            "trade_id": p.trade_id,
            "market_question": p.market_question,
            "market_slug": p.market_slug or None,
            "condition_id": p.condition_id,
            "token_id": p.token_id,
            "side": p.side,
            "outcome": p.outcome,
            "size": p.size,
            "price": p.price,
            "usd_value": p.usd_value,
            "wallet_address": p.wallet_address,
            "wallet_trade_count": p.wallet_trade_count,
            "insider_score": p.insider_score,
            "is_potential_insider": p.is_potential_insider,
            "price_before": p.price_before,
            "price_after": p.price_after,
            "price_impact_pct": p.price_impact_pct,
            "is_cluster": p.is_cluster,
            "cluster_total_usd": p.cluster_total_usd,
            "cluster_trade_count": p.cluster_trade_count,
            "polymarket_url": p.polymarket_url,
        }
        sb.table("whale_alerts").insert(row).execute()
        return True
    except Exception as e:
        err = str(e).lower()
        if "duplicate" in err or "unique" in err:
            log.debug("Supabase duplicate trade_id skipped: %s", p.trade_id)
            return True
        log.error("Supabase insert failed for %s: %s", p.trade_id, e)
        return False


def test_connection(cfg: Config) -> bool:
    """Lightweight check: client init + optional count query."""
    try:
        sb = _client(cfg)
        sb.table("whale_alerts").select("id").limit(1).execute()
        return True
    except Exception as e:
        log.error("Supabase connection test failed: %s", e)
        return False
