from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import json
import hashlib
from typing import Any


@dataclass(frozen=True)
class ReplayConfig:
    start: datetime | None = None
    end: datetime | None = None
    market_external_ids: tuple[str, ...] = ()
    strategy_name: str = "alpha_scoring_v2"
    execution_mode: str = "paper"
    speed: float = 0.0
    scanner_window_size: int = 10
    market_limit: int | None = None

    def stable_id(self) -> str:
        payload = asdict(self)
        normalized = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()
        return digest[:16]


@dataclass(frozen=True)
class HistoricalSnapshot:
    external_id: str
    title: str
    platform: str
    status: str
    yes_price: float | None
    no_price: float | None
    spread: float | None
    volume_24h: float | None
    liquidity: float | None
    best_bid: float | None
    best_ask: float | None
    captured_at: datetime


@dataclass
class ReplayStepResult:
    captured_at: datetime
    snapshots_applied: int
    scanner: dict[str, int]
    confirmations: dict[str, int]
    execution: dict[str, int]
    reconciliation: dict[str, int]
    summary: dict[str, Any]
    totals: dict[str, int]
    regime_counts: dict[str, int]
    exposure: float
    equity: float
    drawdown: float


@dataclass
class ReplayRunResult:
    run_id: str
    config: ReplayConfig
    steps: list[ReplayStepResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": asdict(self.config),
            "steps": [asdict(s) for s in self.steps],
        }
