"""Telegram outbound queue with rate limits and formatted whale alerts."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from telegram import Bot

from .config import Config
from .trade_monitor import WhaleAlertPayload

log = logging.getLogger(__name__)

FOOTER = "Powered by PolyEdge AI — polyedgeai.com"


def _short_wallet(addr: str) -> str:
    a = addr.strip()
    if len(a) <= 14:
        return a
    return f"{a[:6]}…{a[-4:]}"


def _fmt_ts(ts: int | None) -> str:
    if ts is None:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def format_alert(p: WhaleAlertPayload) -> str:
    """Build message text including required footer."""
    lines: list[str] = []
    if p.alert_subtype == "cluster":
        if p.is_mega:
            lines.append("🚨🐋 MEGA WHALE CLUSTER 🐋🚨")
        else:
            lines.append("🐋 WHALE CLUSTER DETECTED")
        lines.append("")
        lines.append(f"📊 Market: {p.market_question}")
        lines.append(
            f"💰 Total: ${p.cluster_total_usd:,.0f} across {p.cluster_trade_count} trades on {p.outcome.upper()}"
        )
        ap = p.price_before or 0
        fp = p.price_after or 0
        if ap and fp:
            pct = (fp - ap) / ap * 100.0
            lines.append(f"📈 Avg Price: ${ap:.2f}, Final: ${fp:.2f} ({pct:+.1f}%)")
        lines.append(f"👛 Wallet: {_short_wallet(p.wallet_address)}")
        if p.window_start_ts and p.window_end_ts:
            t0 = datetime.fromtimestamp(p.window_start_ts, tz=timezone.utc).strftime("%H:%M")
            t1 = datetime.fromtimestamp(p.window_end_ts, tz=timezone.utc).strftime("%H:%M")
            lines.append(f"⏰ Window: {t0} — {t1} UTC")
        else:
            lines.append(f"⏰ Time: {_fmt_ts(None)}")
        lines.append("")
        lines.append(f"🔗 {p.polymarket_url}")
    elif p.alert_subtype == "mega":
        lines.append("🚨🐋 MEGA WHALE ALERT 🐋🚨")
        lines.append("")
        lines.append(f"📊 Market: {p.market_question}")
        lines.append(f"💰 Trade: ${p.usd_value:,.0f} on {p.outcome.upper()} at ${p.price:.2f}")
        if p.price_before is not None and p.price_after is not None:
            pct = (p.price_after - p.price_before) / p.price_before * 100.0 if p.price_before else 0.0
            lines.append(f"📈 Price Impact: ${p.price_before:.2f} → ${p.price_after:.2f} ({pct:+.1f}%)")
        wc = p.wallet_trade_count
        wc_s = f"{wc} prior trades" if wc is not None else "unknown history"
        lines.append(f"👛 Wallet: {_short_wallet(p.wallet_address)} ({wc_s})")
        if p.is_potential_insider and p.insider_score is not None:
            lines.append(f"🔍 POTENTIAL INSIDER — Score: {p.insider_score:.2f}")
            lines.append("")
            lines.append("⚠️ New wallet, large directional bet, significant price impact")
        lines.append(f"⏰ Time: {_fmt_ts(p.trade_ts)}")
        lines.append("")
        lines.append(f"🔗 {p.polymarket_url}")
        lines.append("")
        lines.append(FOOTER)
        return "\n".join(lines)
    else:
        lines.append("🐋 WHALE ALERT")
        lines.append("")
        lines.append(f"📊 Market: {p.market_question}")
        lines.append(f"💰 Trade: ${p.usd_value:,.0f} on {p.outcome.upper()} at ${p.price:.2f}")
        if p.price_before is not None and p.price_after is not None and p.price_impact_pct is not None:
            lines.append(
                f"📈 Price Impact: ${p.price_before:.2f} → ${p.price_after:.2f} ({p.price_impact_pct:+.1f}%)"
            )
        wc = p.wallet_trade_count
        wc_s = f"{wc} prior trades" if wc is not None else "unknown history"
        lines.append(f"👛 Wallet: {_short_wallet(p.wallet_address)} ({wc_s})")
        lines.append(f"⏰ Time: {_fmt_ts(p.trade_ts)}")
        lines.append("")
        lines.append(f"🔗 {p.polymarket_url}")

    lines.append("")
    lines.append(FOOTER)
    return "\n".join(lines)


class TelegramAlerter:
    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._queue: asyncio.Queue[WhaleAlertPayload | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._sent_recent: list[float] = []
        self._fail_backoff = 5.0

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._worker(), name="telegram-worker")

    async def stop(self) -> None:
        if self._task:
            await self._queue.put(None)
            await self._task
            self._task = None

    async def enqueue(self, payload: WhaleAlertPayload) -> None:
        await self._queue.put(payload)

    async def send_test_message(self, text: str) -> bool:
        try:
            bot = Bot(self._cfg.telegram_bot_token)
            await bot.send_message(
                chat_id=self._cfg.telegram_group_chat_id,
                text=text,
                read_timeout=30,
                write_timeout=30,
                connect_timeout=30,
            )
            return True
        except Exception as e:
            log.error("Telegram test failed: %s", e)
            return False

    def _prune_send_times(self) -> None:
        cutoff = time.monotonic() - 60.0
        self._sent_recent = [t for t in self._sent_recent if t >= cutoff]

    async def _worker(self) -> None:
        bot = Bot(self._cfg.telegram_bot_token)
        while True:
            item = await self._queue.get()
            if item is None:
                self._queue.task_done()
                break
            try:
                self._prune_send_times()
                if len(self._sent_recent) >= self._cfg.telegram_max_per_minute:
                    wait = 60.0 - (time.monotonic() - self._sent_recent[0])
                    if wait > 0:
                        await asyncio.sleep(wait)
                    self._prune_send_times()
                text = format_alert(item)
                await bot.send_message(
                    chat_id=self._cfg.telegram_group_chat_id,
                    text=text,
                    read_timeout=45,
                    write_timeout=45,
                    connect_timeout=45,
                )
                self._sent_recent.append(time.monotonic())
                self._fail_backoff = 5.0
            except Exception as e:
                log.warning("Telegram send error: %s — retrying", e)
                await self._queue.put(item)
                await asyncio.sleep(self._fail_backoff)
                self._fail_backoff = min(self._fail_backoff * 2, 120.0)
            finally:
                self._queue.task_done()
