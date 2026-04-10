"""Insider probability scoring with caching and rate limits."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import httpx

from .known_wallets import KnownWalletsStore
from .market_cache import MarketCache

log = logging.getLogger(__name__)


@dataclass
class WalletSnapshot:
    total_trades: int
    distinct_markets: int


@dataclass
class InsiderAnalysis:
    insider_score: float
    is_potential_insider: bool
    trade_count_score: float
    size_vs_market_score: float
    known_mm_score: float
    price_impact_score: float
    wallet_trade_count: int


def _trade_count_factor(total: int) -> float:
    if total < 5:
        return 1.0
    if total < 20:
        return 0.5
    if total < 100:
        return 0.2
    return 0.0


def _size_vs_market_ratio(usd_notional: float, vol24: float | None) -> float:
    if not vol24 or vol24 <= 0:
        return 0.3
    ratio = usd_notional / vol24
    return max(0.0, min(1.0, ratio / 0.10))


def _price_impact_factor(price_before: float, price_after: float | None) -> float:
    if price_after is None or price_before <= 0:
        return 0.2
    pct = abs(price_after - price_before) / price_before * 100.0
    return max(0.0, min(1.0, pct / 2.0))


class WalletAnalyzer:
    def __init__(
        self,
        data_api_url: str,
        clob_url: str,
        market_cache: MarketCache,
        known: KnownWalletsStore,
        insider_threshold: float,
        cache_ttl: int = 3600,
    ) -> None:
        self._data = data_api_url.rstrip("/")
        self._clob = clob_url.rstrip("/")
        self._markets = market_cache
        self._known = known
        self._insider_threshold = insider_threshold
        self._cache_ttl = cache_ttl
        self._count_cache: dict[str, tuple[float, WalletSnapshot]] = {}

    async def fetch_wallet_snapshot(self, client: httpx.AsyncClient, wallet: str) -> WalletSnapshot:
        w = wallet.lower()
        now = time.time()
        hit = self._count_cache.get(w)
        if hit and now - hit[0] < self._cache_ttl:
            return hit[1]

        markets_seen: set[str] = set()
        total = 0
        offset = 0
        limit = 500
        max_offset = 5000
        while offset < max_offset:
            r = await client.get(
                f"{self._data}/trades",
                params={"user": w, "limit": limit, "offset": offset},
                timeout=45.0,
            )
            if r.status_code == 429:
                log.warning("Data API rate limit during wallet snapshot for %s…", w[:10])
                break
            r.raise_for_status()
            rows = r.json()
            if not isinstance(rows, list) or not rows:
                break
            for t in rows:
                total += 1
                cid = t.get("conditionId")
                if cid:
                    markets_seen.add(str(cid).lower())
            if len(rows) < limit:
                break
            offset += limit

        snap = WalletSnapshot(total_trades=total, distinct_markets=len(markets_seen))
        self._count_cache[w] = (now, snap)
        self._known.maybe_auto_classify(w, snap.total_trades, snap.distinct_markets)
        return snap

    async def midpoint(self, client: httpx.AsyncClient, token_id: str) -> float | None:
        r = await client.get(
            f"{self._clob}/book",
            params={"token_id": str(token_id)},
            timeout=20.0,
        )
        if r.status_code != 200:
            return None
        data: dict[str, Any] = r.json()
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        if not bids or not asks:
            return None
        try:
            best_bid = float(bids[0].get("price"))
            best_ask = float(asks[0].get("price"))
        except (TypeError, ValueError, KeyError, IndexError):
            return None
        return (best_bid + best_ask) / 2.0

    async def analyze(
        self,
        client: httpx.AsyncClient,
        *,
        wallet: str,
        token_id: str,
        condition_id: str,
        usd_notional: float,
        trade_price: float,
    ) -> InsiderAnalysis:
        snap = await self.fetch_wallet_snapshot(client, wallet)
        tc = _trade_count_factor(snap.total_trades)
        vol = self._markets.volume_24h_usd(condition_id)
        sv = _size_vs_market_ratio(usd_notional, vol)
        known = self._known.is_known(wallet)
        mm_component = 0.0 if known else 1.0

        mid_after = await self.midpoint(client, token_id)
        pi = _price_impact_factor(trade_price, mid_after)

        # Weighted sum per spec
        insider_score = 0.3 * tc + 0.2 * sv + 0.3 * mm_component + 0.2 * pi
        insider_score = max(0.0, min(1.0, insider_score))

        return InsiderAnalysis(
            insider_score=insider_score,
            is_potential_insider=insider_score >= self._insider_threshold,
            trade_count_score=tc,
            size_vs_market_score=sv,
            known_mm_score=mm_component,
            price_impact_score=pi,
            wallet_trade_count=snap.total_trades,
        )
