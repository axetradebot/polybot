"""Poll recent trades, detect whales, clusters, and build alert payloads."""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

import httpx

from .config import Config
from .market_cache import MarketCache
from .wallet_analyzer import InsiderAnalysis, WalletAnalyzer

log = logging.getLogger(__name__)


@dataclass
class WhaleAlertPayload:
    trade_id: str
    market_question: str
    market_slug: str
    condition_id: str
    token_id: str
    side: str
    outcome: str
    size: float
    price: float
    usd_value: float
    wallet_address: str
    wallet_trade_count: int | None
    insider_score: float | None
    is_potential_insider: bool
    price_before: float | None
    price_after: float | None
    price_impact_pct: float | None
    is_cluster: bool
    cluster_total_usd: float | None
    cluster_trade_count: int | None
    polymarket_url: str
    is_mega: bool
    alert_subtype: str  # standard | mega | cluster
    trade_ts: int | None = None
    window_start_ts: int | None = None
    window_end_ts: int | None = None


@dataclass
class _Member:
    ts: int
    usd: float
    trade_id: str
    price: float
    raw: dict[str, Any]


@dataclass
class TradeMonitorState:
    seen_trade_ids: deque[str] = field(default_factory=deque)
    seen_set: set[str] = field(default_factory=set)
    cluster_buffers: dict[tuple[str, str, str], list[_Member]] = field(default_factory=dict)
    cooldown_until: dict[tuple[str, str], float] = field(default_factory=dict)

    def remember_seen(self, tid: str, max_ids: int) -> bool:
        """Return True if new."""
        if tid in self.seen_set:
            return False
        while len(self.seen_trade_ids) >= max_ids:
            old = self.seen_trade_ids.popleft()
            self.seen_set.discard(old)
        self.seen_trade_ids.append(tid)
        self.seen_set.add(tid)
        return True


def _stable_trade_id(t: dict[str, Any]) -> str:
    tx = t.get("transactionHash") or t.get("transaction_hash")
    asset = t.get("asset") or t.get("asset_id") or ""
    ts = t.get("timestamp")
    if tx:
        return f"{tx}:{asset}:{ts}"
    w = t.get("proxyWallet") or t.get("trader") or ""
    return f"{w}:{asset}:{ts}:{t.get('price')}:{t.get('size')}"


def _parse_trade(t: dict[str, Any]) -> dict[str, Any] | None:
    try:
        wallet = str(t.get("proxyWallet") or t.get("trader") or "").strip()
        if not wallet:
            return None
        asset = str(t.get("asset") or t.get("asset_id") or "").strip()
        cid = str(t.get("conditionId") or t.get("market") or "").strip()
        side = str(t.get("side") or "").upper()
        price = float(t.get("price") or 0)
        size = float(t.get("size") or 0)
        ts = int(t.get("timestamp") or 0)
    except (TypeError, ValueError):
        return None
    if not asset or not cid or side not in ("BUY", "SELL") or ts <= 0:
        return None
    usd = size * price
    return {
        "raw": t,
        "wallet": wallet,
        "asset": asset,
        "condition_id": cid,
        "side": side,
        "price": price,
        "size": size,
        "ts": ts,
        "usd": usd,
        "id": _stable_trade_id(t),
    }


def _polymarket_url(mcache: MarketCache, condition_id: str, trade: dict[str, Any], slug_fallback: str) -> str:
    ev = mcache.event_slug(condition_id)
    if ev:
        return f"https://polymarket.com/event/{ev}"
    es = trade.get("eventSlug") or trade.get("event_slug")
    if es:
        return f"https://polymarket.com/event/{es}"
    if slug_fallback:
        return f"https://polymarket.com/event/{slug_fallback}"
    return "https://polymarket.com"


class TradeMonitor:
    def __init__(
        self,
        cfg: Config,
        mcache: MarketCache,
        analyzer: WalletAnalyzer,
    ) -> None:
        self.cfg = cfg
        self.mcache = mcache
        self.analyzer = analyzer
        self.state = TradeMonitorState()
        self._bootstrapped = False

    def _cooldown_ok(self, condition_id: str, wallet: str) -> bool:
        key = (condition_id.lower(), wallet.lower())
        until = self.state.cooldown_until.get(key)
        return until is None or time.time() >= until

    def _touch_cooldown(self, condition_id: str, wallet: str) -> None:
        key = (condition_id.lower(), wallet.lower())
        self.state.cooldown_until[key] = time.time() + self.cfg.alert_cooldown_seconds

    def _prune_clusters(self, now_ts: int) -> None:
        cutoff = now_ts - self.cfg.cluster_window_seconds
        for k in list(self.state.cluster_buffers.keys()):
            buf = self.state.cluster_buffers[k]
            buf[:] = [x for x in buf if x.ts >= cutoff]
            if not buf:
                del self.state.cluster_buffers[k]

    def _prune_cooldowns(self) -> None:
        now = time.time()
        expired = [k for k, v in self.state.cooldown_until.items() if v < now]
        for k in expired:
            del self.state.cooldown_until[k]

    async def poll(self, client: httpx.AsyncClient) -> tuple[list[WhaleAlertPayload], int]:
        r = await client.get(
            f"{self.cfg.data_api_url}/trades",
            params={"limit": 100},
            timeout=45.0,
        )
        if r.status_code == 429:
            log.warning("Data API returned 429 on trades poll")
            raise RuntimeError("rate_limited")
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list):
            return [], 0
        nrows = len(rows)

        if not self._bootstrapped:
            for t in rows:
                if not isinstance(t, dict):
                    continue
                p = _parse_trade(t)
                if p:
                    self.state.remember_seen(p["id"], self.cfg.seen_trade_ids_max)
            self._bootstrapped = True
            log.info("Bootstrap: marked %s recent trade IDs as seen (no alerts)", nrows)
            return [], nrows

        parsed: list[dict[str, Any]] = []
        skipped_resolution = 0
        skipped_price = 0
        for t in rows:
            if not isinstance(t, dict):
                continue
            p = _parse_trade(t)
            if not p:
                continue
            if not self.state.remember_seen(p["id"], self.cfg.seen_trade_ids_max):
                continue
            if p["price"] >= self.cfg.ignore_price_above or p["price"] <= self.cfg.ignore_price_below:
                skipped_price += 1
                continue
            if self.mcache.is_near_resolution(p["condition_id"], self.cfg.ignore_near_resolution_seconds):
                skipped_resolution += 1
                continue
            parsed.append(p)
        if skipped_price:
            log.debug("Skipped %s trades at extreme prices (>=%.2f or <=%.2f)",
                      skipped_price, self.cfg.ignore_price_above, self.cfg.ignore_price_below)
        if skipped_resolution:
            log.debug("Skipped %s trades on markets within %sm of resolution",
                      skipped_resolution, self.cfg.ignore_near_resolution_seconds // 60)

        if not parsed:
            return [], nrows

        now_ts = int(time.time())
        alerts: list[WhaleAlertPayload] = []

        # Add to cluster buffers (most recent batch first in API — re-sort by ts)
        parsed.sort(key=lambda x: x["ts"])
        for p in parsed:
            key = (p["wallet"].lower(), p["condition_id"].lower(), p["side"])
            buf = self.state.cluster_buffers.setdefault(key, [])
            buf.append(
                _Member(
                    ts=p["ts"],
                    usd=p["usd"],
                    trade_id=p["id"],
                    price=p["price"],
                    raw=p["raw"],
                )
            )

        self._prune_clusters(now_ts)
        self._prune_cooldowns()

        # Cluster alerts (>=2 trades, sum >= threshold)
        for key, members in list(self.state.cluster_buffers.items()):
            if len(members) < 2:
                continue
            total = sum(m.usd for m in members)
            if total < self.cfg.whale_threshold_usd:
                continue
            wallet, cid, side = key
            if not self._cooldown_ok(cid, wallet):
                continue
            first_ts = min(m.ts for m in members)
            last_ts = max(m.ts for m in members)
            sample = members[-1].raw
            payload = await self._build_alert(
                client,
                trade_id=f"cluster:{wallet}:{cid}:{side}:{first_ts}",
                trade=sample,
                usd_override=total,
                is_cluster=True,
                cluster_members=members,
                window_start_ts=first_ts,
                window_end_ts=last_ts,
            )
            if payload:
                alerts.append(payload)
                self._touch_cooldown(cid, wallet)
            del self.state.cluster_buffers[key]

        # Single-trade whales: exactly one fill in the 5m window and it clears the threshold
        singles: list[dict[str, Any]] = []
        for p in parsed:
            if p["usd"] < self.cfg.whale_threshold_usd:
                continue
            key = (p["wallet"].lower(), p["condition_id"].lower(), p["side"])
            mems = self.state.cluster_buffers.get(key, [])
            if len(mems) != 1 or mems[0].trade_id != p["id"]:
                continue
            singles.append(p)

        singles.sort(key=lambda x: -x["usd"])
        analysis_budget = self.cfg.wallet_lookups_per_cycle
        for p in singles:
            if not self._cooldown_ok(p["condition_id"], p["wallet"]):
                continue
            use_analysis = analysis_budget > 0
            if use_analysis:
                analysis_budget -= 1
            payload = await self._build_alert(
                client,
                trade_id=p["id"],
                trade=p["raw"],
                usd_override=p["usd"],
                is_cluster=False,
                cluster_members=None,
                force_analyze=use_analysis,
            )
            if payload:
                alerts.append(payload)
                self._touch_cooldown(p["condition_id"], p["wallet"])
                key = (p["wallet"].lower(), p["condition_id"].lower(), p["side"])
                self.state.cluster_buffers.pop(key, None)

        return alerts, nrows

    async def _build_alert(
        self,
        client: httpx.AsyncClient,
        *,
        trade_id: str,
        trade: dict[str, Any],
        usd_override: float,
        is_cluster: bool,
        cluster_members: list[_Member] | None,
        window_start_ts: int | None = None,
        window_end_ts: int | None = None,
        force_analyze: bool = True,
    ) -> WhaleAlertPayload | None:
        wallet = str(trade.get("proxyWallet") or trade.get("trader") or "")
        asset = str(trade.get("asset") or "")
        cid = str(trade.get("conditionId") or "")
        side = str(trade.get("side") or "").upper()
        price = float(trade.get("price") or 0)
        size = float(trade.get("size") or 0)
        trade_ts = int(trade.get("timestamp") or 0) or None
        if trade_ts:
            delay_s = int(time.time()) - trade_ts
            log.info("Detection delay: %ss (trade at %s, detected now)", delay_s, trade_ts)
        ti = self.mcache.get_token(asset)
        question = (ti.question if ti else None) or str(trade.get("title") or "Unknown market")
        slug = (ti.slug if ti else None) or str(trade.get("slug") or "")
        outcome = (ti.outcome if ti else None) or str(trade.get("outcome") or "")
        if not outcome and isinstance(trade.get("outcomeIndex"), int) and ti:
            outcome = ti.outcome
        if not outcome:
            outcome = "Yes" if str(trade.get("outcome", "")).lower() in ("yes", "y") else str(trade.get("outcome") or "—")

        url = _polymarket_url(self.mcache, cid, trade, slug)

        price_after: float | None = None
        price_impact_pct: float | None = None
        try:
            price_after = await self.analyzer.midpoint(client, asset)
            if price_after is not None and price > 0:
                price_impact_pct = (price_after - price) / price * 100.0
        except Exception as e:
            log.debug("Midpoint fetch failed: %s", e)

        insider: InsiderAnalysis | None = None
        if force_analyze and not is_cluster:
            try:
                insider = await self.analyzer.analyze(
                    client,
                    wallet=wallet,
                    token_id=asset,
                    condition_id=cid,
                    usd_notional=usd_override,
                    trade_price=price,
                )
            except Exception as e:
                log.warning("Insider analysis failed: %s", e)

        is_mega = usd_override >= self.cfg.mega_whale_threshold_usd
        if is_cluster:
            subtype = "cluster"
        elif is_mega:
            subtype = "mega"
        else:
            subtype = "standard"

        avg_price: float | None = None
        final_price: float | None = None
        if is_cluster and cluster_members:
            tot_size = sum((m.raw.get("size") or 0) for m in cluster_members) or 0.0
            avg_price = (
                sum((m.raw.get("price") or 0) * (m.raw.get("size") or 0) for m in cluster_members) / tot_size
                if tot_size > 0
                else price
            )
            final_price = cluster_members[-1].price

        wc = insider.wallet_trade_count if insider else None
        ins_score = insider.insider_score if insider else None
        pot = insider.is_potential_insider if insider else False

        if is_cluster:
            cluster_impact = None
            if price_after is not None and avg_price and avg_price > 0:
                cluster_impact = (price_after - avg_price) / avg_price * 100.0
            elif avg_price and final_price:
                cluster_impact = (final_price - avg_price) / avg_price * 100.0
            return WhaleAlertPayload(
                trade_id=trade_id,
                market_question=question,
                market_slug=slug,
                condition_id=cid,
                token_id=asset,
                side=side,
                outcome=outcome,
                size=sum(m.raw.get("size") or 0 for m in cluster_members or []),
                price=final_price or price,
                usd_value=usd_override,
                wallet_address=wallet,
                wallet_trade_count=None,
                insider_score=None,
                is_potential_insider=False,
                price_before=avg_price,
                price_after=price_after if price_after is not None else final_price,
                price_impact_pct=cluster_impact,
                is_cluster=True,
                cluster_total_usd=usd_override,
                cluster_trade_count=len(cluster_members or []),
                polymarket_url=url,
                is_mega=is_mega,
                alert_subtype=subtype,
                trade_ts=trade_ts,
                window_start_ts=window_start_ts,
                window_end_ts=window_end_ts,
            )

        return WhaleAlertPayload(
            trade_id=trade_id,
            market_question=question,
            market_slug=slug,
            condition_id=cid,
            token_id=asset,
            side=side,
            outcome=outcome,
            size=size,
            price=price,
            usd_value=usd_override,
            wallet_address=wallet,
            wallet_trade_count=wc,
            insider_score=ins_score,
            is_potential_insider=pot,
            price_before=price,
            price_after=price_after,
            price_impact_pct=price_impact_pct,
            is_cluster=False,
            cluster_total_usd=None,
            cluster_trade_count=None,
            polymarket_url=url,
            is_mega=is_mega,
            alert_subtype=subtype,
            trade_ts=trade_ts,
        )
