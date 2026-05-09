from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from sqlalchemy import select

from backend.db import SessionLocal
from backend.models import Market, MarketSnapshot
from backend.replay.replay_engine import ReplayEngine
from backend.replay.replay_models import HistoricalSnapshot, ReplayConfig


def _load_snapshots_from_db(config: ReplayConfig) -> list[HistoricalSnapshot]:
    session = SessionLocal()
    try:
        q = (
            select(Market, MarketSnapshot)
            .join(MarketSnapshot, MarketSnapshot.market_id == Market.id)
            .order_by(MarketSnapshot.captured_at.asc(), Market.external_id.asc())
        )

        if config.start is not None:
            q = q.where(MarketSnapshot.captured_at >= config.start.replace(tzinfo=None))
        if config.end is not None:
            q = q.where(MarketSnapshot.captured_at <= config.end.replace(tzinfo=None))
        if config.market_external_ids:
            q = q.where(Market.external_id.in_(list(config.market_external_ids)))

        rows = session.execute(q).all()
        out: list[HistoricalSnapshot] = []
        for market, snap in rows:
            out.append(
                HistoricalSnapshot(
                    external_id=str(market.external_id),
                    title=str(market.title),
                    platform=str(market.platform),
                    status=str(market.status),
                    yes_price=snap.yes_price,
                    no_price=snap.no_price,
                    spread=snap.spread,
                    volume_24h=snap.volume_24h,
                    liquidity=snap.liquidity,
                    best_bid=snap.best_bid,
                    best_ask=snap.best_ask,
                    captured_at=snap.captured_at,
                )
            )
        return out
    finally:
        session.close()


def run_replay(
    *,
    config: ReplayConfig,
    snapshots: Iterable[HistoricalSnapshot] | None = None,
    output_dir: str | Path | None = None,
) -> Path:
    snap_list = list(snapshots) if snapshots is not None else _load_snapshots_from_db(config)
    engine = ReplayEngine(config=config)
    result = engine.run(snap_list)

    out_dir = Path(output_dir or Path(__file__).resolve().parent / "runs")
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"replay_{result.run_id}.json"
    payload = result.to_dict()
    out_path.write_text(json.dumps(payload, sort_keys=True, default=str, indent=2), encoding="utf-8")
    return out_path


__all__ = [
    "ReplayConfig",
    "HistoricalSnapshot",
    "run_replay",
]
