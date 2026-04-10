"""Known market-maker addresses and auto-learned classification."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

log = logging.getLogger(__name__)

# Manual seed — extend over time (lowercase hex)
DEFAULT_SEED: set[str] = set()


@dataclass
class KnownWalletsStore:
    """Persist known MMs; auto-add high-activity wallets."""

    path: Path
    addresses: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.addresses |= {a.lower() for a in DEFAULT_SEED}
        self._load()

    def _load(self) -> None:
        if not self.path.is_file():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            log.warning("Could not load %s: %s", self.path, e)
            return
        addrs = raw.get("known_market_makers") or []
        if isinstance(addrs, list):
            self.addresses |= {str(a).lower() for a in addrs}

    def save(self) -> None:
        payload = {"known_market_makers": sorted(self.addresses)}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def is_known(self, wallet: str) -> bool:
        return wallet.lower() in self.addresses

    def maybe_auto_classify(self, wallet: str, total_trades: int, distinct_markets: int) -> bool:
        """
        If wallet has >200 trades across >20 markets, classify as MM.
        Returns True if newly added.
        """
        w = wallet.lower()
        if w in self.addresses:
            return False
        if total_trades > 200 and distinct_markets > 20:
            self.addresses.add(w)
            log.info(
                "Auto-classified market maker: %s… (trades=%s, markets=%s)",
                w[:10],
                total_trades,
                distinct_markets,
            )
            self.save()
            return True
        return False
