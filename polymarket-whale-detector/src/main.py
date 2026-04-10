"""Entry point: market cache refresh, trade polling, Telegram + Supabase."""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from dataclasses import dataclass, field

import httpx

from .config import load_config, validate_config_present
from .known_wallets import KnownWalletsStore
from .market_cache import MarketCache
from .supabase_client import insert_whale_alert, test_connection
from .telegram_bot import TelegramAlerter
from .trade_monitor import TradeMonitor
from .wallet_analyzer import WalletAnalyzer

log = logging.getLogger("whale_detector")


@dataclass
class PeriodStats:
    polls: int = 0
    trades_fetched: int = 0
    whales: int = 0
    alerts_enqueued: int = 0


@dataclass
class RunState:
    stats: PeriodStats = field(default_factory=PeriodStats)
    api_backoff: float = 0.0
    poll_interval_effective: float = 0.0


async def startup_checks(http: httpx.AsyncClient, tg: TelegramAlerter, cfg, mcache: MarketCache) -> None:
    log.info("Startup validation:")
    for name, ok, detail in validate_config_present():
        log.info("  %s: %s (%s)", name, "OK" if ok else "MISSING", detail)

    ok_data = False
    try:
        r = await http.get(f"{cfg.data_api_url}/trades", params={"limit": 1}, timeout=30.0)
        ok_data = r.status_code == 200
        log.info("  Data API trades: %s", "OK" if ok_data else f"HTTP {r.status_code}")
    except Exception as e:
        log.error("  Data API trades: ERROR %s", e)

    try:
        tok = mcache.any_token_id()
        if tok:
            r = await http.get(f"{cfg.clob_api_url}/book", params={"token_id": tok}, timeout=30.0)
            log.info("  CLOB order book sample: %s", "OK" if r.status_code == 200 else f"HTTP {r.status_code}")
        else:
            r = await http.get(cfg.clob_api_url.rstrip("/") + "/", timeout=15.0)
            log.info("  CLOB base: %s", "OK" if r.status_code < 500 else f"HTTP {r.status_code}")
    except Exception as e:
        log.error("  CLOB reachability: ERROR %s", e)

    if await tg.send_test_message("Whale detector online 🐋"):
        log.info("  Telegram: OK (test message sent)")
    else:
        log.error("  Telegram: FAILED — alerts may not deliver; continuing with retries")

    if await asyncio.to_thread(test_connection, cfg):
        log.info("  Supabase: OK")
    else:
        log.error("  Supabase: FAILED — rows will not persist until connectivity is restored")


async def market_refresh_loop(mcache: MarketCache, http: httpx.AsyncClient, interval: int) -> None:
    while True:
        await asyncio.sleep(interval)
        try:
            await mcache.refresh(http)
        except Exception as e:
            log.warning("Gamma cache refresh failed: %s", e)


async def run() -> None:
    cfg = load_config()
    state = RunState(poll_interval_effective=float(cfg.poll_interval_seconds))
    err_backoff = float(cfg.api_backoff_initial)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )

    mcache = MarketCache(cfg.gamma_api_url, refresh_seconds=cfg.market_cache_refresh_seconds)
    known = KnownWalletsStore(cfg.known_wallets_path)
    analyzer = WalletAnalyzer(
        cfg.data_api_url,
        cfg.clob_api_url,
        mcache,
        known,
        cfg.insider_score_threshold,
        cache_ttl=cfg.wallet_trade_count_cache_ttl,
    )
    monitor = TradeMonitor(cfg, mcache, analyzer)
    tg = TelegramAlerter(cfg)

    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    async with httpx.AsyncClient(timeout=httpx.Timeout(60.0), limits=limits) as http:
        try:
            n = await mcache.refresh(http)
            log.info("Initial Gamma cache: %s tokens", n)
        except Exception as e:
            log.error("Initial market cache failed (will retry on schedule): %s", e)

        await tg.start()
        await startup_checks(http, tg, cfg, mcache)

        refresh_task = asyncio.create_task(
            market_refresh_loop(mcache, http, cfg.market_cache_refresh_seconds),
            name="gamma-refresh",
        )
        last_hb = time.monotonic()
        try:
            while True:
                try:
                    alerts, nrows = await monitor.poll(http)
                    state.stats.trades_fetched += nrows
                    state.stats.polls += 1
                    state.stats.whales += len(alerts)
                    state.api_backoff = 0.0
                    err_backoff = float(cfg.api_backoff_initial)
                    state.poll_interval_effective = float(cfg.poll_interval_seconds)

                    for a in alerts:
                        await tg.enqueue(a)
                        state.stats.alerts_enqueued += 1
                        await asyncio.to_thread(insert_whale_alert, cfg, a)

                except RuntimeError as e:
                    if "rate_limited" in str(e).lower():
                        state.api_backoff = min(state.api_backoff + 30.0, float(cfg.api_backoff_max))
                    else:
                        raise
                except Exception as e:
                    log.exception("Poll error: %s", e)
                    state.api_backoff = min(err_backoff, float(cfg.api_backoff_max))
                    err_backoff = min(err_backoff * 2, float(cfg.api_backoff_max))

                if time.monotonic() - last_hb >= cfg.heartbeat_interval_seconds:
                    log.info(
                        "Whale Detector alive — monitored %s trades in %s polls, "
                        "%s whale events, %s alerts enqueued in last %ss",
                        state.stats.trades_fetched,
                        state.stats.polls,
                        state.stats.whales,
                        state.stats.alerts_enqueued,
                        cfg.heartbeat_interval_seconds,
                    )
                    state.stats = PeriodStats()
                    last_hb = time.monotonic()

                sleep_s = state.poll_interval_effective + state.api_backoff
                await asyncio.sleep(sleep_s)
        finally:
            refresh_task.cancel()
            try:
                await refresh_task
            except asyncio.CancelledError:
                pass
            await tg.stop()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Shutdown requested")


if __name__ == "__main__":
    main()
