"""Load environment and tuning constants."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

_ENV_LOADED = False


def _ensure_env() -> None:
    global _ENV_LOADED
    if _ENV_LOADED:
        return
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.is_file():
        load_dotenv(env_path)
    else:
        load_dotenv()
    _ENV_LOADED = True


def _req(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise ValueError(f"Missing required environment variable: {name}")
    return v


def _opt(name: str, default: str) -> str:
    v = os.environ.get(name, "").strip()
    return v if v else default


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


@dataclass(frozen=True)
class Config:
    clob_api_url: str
    gamma_api_url: str
    data_api_url: str
    telegram_bot_token: str
    telegram_group_chat_id: str
    supabase_url: str
    supabase_service_key: str
    whale_threshold_usd: float
    mega_whale_threshold_usd: float
    poll_interval_seconds: int
    insider_score_threshold: float
    known_wallets_path: Path
    market_cache_refresh_seconds: int = 900
    seen_trade_ids_max: int = 10_000
    cluster_window_seconds: int = 300
    alert_cooldown_seconds: int = 600
    telegram_max_per_minute: int = 20
    wallet_lookups_per_cycle: int = 10
    wallet_trade_count_cache_ttl: int = 3600
    heartbeat_interval_seconds: int = 300
    api_backoff_initial: int = 30
    api_backoff_max: int = 300
    ignore_near_resolution_seconds: int = 3600
    ignore_price_above: float = 0.95
    ignore_price_below: float = 0.05


def load_config() -> Config:
    _ensure_env()
    base = Path(__file__).resolve().parent.parent
    kw_path = os.environ.get("KNOWN_WALLETS_PATH", "").strip()
    known = Path(kw_path) if kw_path else base / "known_wallets.json"
    return Config(
        clob_api_url=_opt("CLOB_API_URL", "https://clob.polymarket.com").rstrip("/"),
        gamma_api_url=_opt("GAMMA_API_URL", "https://gamma-api.polymarket.com").rstrip("/"),
        data_api_url=_opt("DATA_API_URL", "https://data-api.polymarket.com").rstrip("/"),
        telegram_bot_token=_req("WHALE_ALERT_BOT_TOKEN"),
        telegram_group_chat_id=_req("WHALE_ALERT_CHAT_ID"),
        supabase_url=_req("SUPABASE_URL"),
        supabase_service_key=_req("SUPABASE_SERVICE_KEY"),
        whale_threshold_usd=_float("WHALE_THRESHOLD_USD", 5000.0),
        mega_whale_threshold_usd=_float("MEGA_WHALE_THRESHOLD_USD", 25000.0),
        poll_interval_seconds=max(5, _int("POLL_INTERVAL_SECONDS", 30)),
        insider_score_threshold=_float("INSIDER_SCORE_THRESHOLD", 0.6),
        known_wallets_path=known,
        ignore_near_resolution_seconds=_int("IGNORE_NEAR_RESOLUTION_SECONDS", 3600),
    )


def validate_config_present() -> list[tuple[str, bool, str]]:
    """Return checklist rows: (name, ok, detail). Does not raise."""
    _ensure_env()
    checks: list[tuple[str, bool, str]] = []
    for key in (
        "WHALE_ALERT_BOT_TOKEN",
        "WHALE_ALERT_CHAT_ID",
        "SUPABASE_URL",
        "SUPABASE_SERVICE_KEY",
    ):
        ok = bool(os.environ.get(key, "").strip())
        checks.append((key, ok, "set" if ok else "missing"))
    for key, default in (
        ("CLOB_API_URL", "https://clob.polymarket.com"),
        ("GAMMA_API_URL", "https://gamma-api.polymarket.com"),
        ("DATA_API_URL", "https://data-api.polymarket.com"),
    ):
        ok = bool(os.environ.get(key, "").strip() or default)
        checks.append((key, True, os.environ.get(key, default) or default))
    return checks
