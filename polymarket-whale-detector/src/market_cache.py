"""In-memory cache of active Gamma markets (token_id → metadata)."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class TokenInfo:
    question: str
    slug: str
    condition_id: str
    outcome: str  # "Yes" or "No"


class MarketCache:
    def __init__(self, gamma_base_url: str, refresh_seconds: int = 900) -> None:
        self._gamma = gamma_base_url.rstrip("/")
        self.refresh_seconds = refresh_seconds
        self._by_token: dict[str, TokenInfo] = {}
        self._volume24h_by_condition: dict[str, float] = {}
        self._event_slug_by_condition: dict[str, str] = {}

    @property
    def token_count(self) -> int:
        return len(self._by_token)

    def any_token_id(self) -> str | None:
        for tid in self._by_token:
            return str(tid)
        return None

    def get_token(self, token_id: str) -> TokenInfo | None:
        return self._by_token.get(str(token_id))

    def volume_24h_usd(self, condition_id: str) -> float | None:
        return self._volume24h_by_condition.get(condition_id.lower())

    def event_slug(self, condition_id: str) -> str | None:
        return self._event_slug_by_condition.get(condition_id.lower())

    async def refresh(self, client: httpx.AsyncClient) -> int:
        """Fetch all active markets; return number of token rows indexed."""
        self._by_token.clear()
        self._volume24h_by_condition.clear()
        self._event_slug_by_condition.clear()
        offset = 0
        limit = 500
        total_markets = 0
        while True:
            url = f"{self._gamma}/markets"
            params: dict[str, Any] = {"closed": "false", "limit": limit, "offset": offset}
            r = await client.get(url, params=params, timeout=60.0)
            r.raise_for_status()
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            for m in batch:
                total_markets += 1
                self._ingest_market(m)
            if len(batch) < limit:
                break
            offset += limit
        log.info("Gamma cache: %s active markets, %s outcome tokens indexed", total_markets, len(self._by_token))
        return len(self._by_token)

    def _ingest_market(self, m: dict[str, Any]) -> None:
        q = str(m.get("question") or "")
        slug = str(m.get("slug") or "")
        cid = str(m.get("conditionId") or "")
        if not cid:
            return
        cid_l = cid.lower()
        vol = m.get("volume24hrClob")
        if vol is not None:
            try:
                self._volume24h_by_condition[cid_l] = float(vol)
            except (TypeError, ValueError):
                pass
        events = m.get("events") or []
        if isinstance(events, list) and events:
            ev = events[0]
            if isinstance(ev, dict) and ev.get("slug"):
                self._event_slug_by_condition[cid_l] = str(ev["slug"])

        raw_tokens = m.get("clobTokenIds")
        raw_outcomes = m.get("outcomes")
        if isinstance(raw_tokens, str):
            try:
                token_ids = json.loads(raw_tokens)
            except json.JSONDecodeError:
                token_ids = []
        elif isinstance(raw_tokens, list):
            token_ids = raw_tokens
        else:
            token_ids = []
        if isinstance(raw_outcomes, str):
            try:
                outcomes = json.loads(raw_outcomes)
            except json.JSONDecodeError:
                outcomes = ["Yes", "No"]
        elif isinstance(raw_outcomes, list):
            outcomes = [str(x) for x in raw_outcomes]
        else:
            outcomes = ["Yes", "No"]
        for i, tid in enumerate(token_ids):
            label = outcomes[i] if i < len(outcomes) else f"Outcome {i}"
            self._by_token[str(tid)] = TokenInfo(
                question=q,
                slug=slug,
                condition_id=cid,
                outcome=label,
            )
