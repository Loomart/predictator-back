from __future__ import annotations

import csv
from bisect import bisect_left
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import MarketSnapshot, Signal


TERMINAL_STATUSES = ("CONFIRMED", "INVALIDATED", "EXPIRED")


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _decision_time(signal: Signal) -> datetime:
    return signal.last_evaluated_at or signal.created_at


def _time_to_decision_seconds(signal: Signal) -> float:
    start = signal.created_at
    end = _decision_time(signal)
    return max(0.0, (end - start).total_seconds())


def _compute_moves(
    signal: Signal,
    future_snapshots: list[MarketSnapshot],
) -> tuple[float | None, float | None]:
    reference_price = _as_float(signal.reference_price)
    if reference_price is None or reference_price <= 0:
        return None, None

    direction = (signal.direction or "").upper()
    prices = [_as_float(snapshot.yes_price) for snapshot in future_snapshots]
    prices = [p for p in prices if p is not None]

    if not prices:
        return None, None

    # Express moves in directional terms: positive is favorable.
    directional_moves = []
    for price in prices:
        raw_move = price - reference_price
        if direction == "DOWN":
            raw_move = -raw_move
        directional_moves.append(raw_move)

    max_favorable = max(directional_moves)
    max_adverse = min(directional_moves)
    return max_favorable, max_adverse


def _future_window_for_signal(
    market_snapshots: list[MarketSnapshot],
    signal: Signal,
    horizon_snapshots: int,
) -> list[MarketSnapshot]:
    if not market_snapshots or horizon_snapshots <= 0:
        return []

    decision_at = _decision_time(signal)
    times = [snapshot.captured_at for snapshot in market_snapshots]
    start_idx = bisect_left(times, decision_at)
    end_idx = start_idx + horizon_snapshots
    return market_snapshots[start_idx:end_idx]


def build_signal_confirmation_analysis_rows(
    session: Session,
    horizon_snapshots: int = 20,
) -> list[dict[str, Any]]:
    """
    Build analysis rows for terminal confirmation outcomes.

    Designed as a batch utility and intentionally kept out of the main pipeline.
    """
    terminal_signals = list(
        session.scalars(
            select(Signal)
            .where(Signal.status.in_(TERMINAL_STATUSES))
            .order_by(Signal.market_id.asc(), Signal.created_at.asc())
        )
    )

    if not terminal_signals:
        return []

    signals_by_market: dict[int, list[Signal]] = {}
    for signal in terminal_signals:
        signals_by_market.setdefault(signal.market_id, []).append(signal)

    rows: list[dict[str, Any]] = []

    for market_id, market_signals in signals_by_market.items():
        min_decision_at = min(_decision_time(signal) for signal in market_signals)

        market_snapshots = list(
            session.scalars(
                select(MarketSnapshot)
                .where(MarketSnapshot.market_id == market_id)
                .where(MarketSnapshot.captured_at >= min_decision_at)
                .order_by(MarketSnapshot.captured_at.asc())
            )
        )

        for signal in market_signals:
            future_window = _future_window_for_signal(
                market_snapshots=market_snapshots,
                signal=signal,
                horizon_snapshots=horizon_snapshots,
            )
            max_favorable_move, max_adverse_move = _compute_moves(signal, future_window)

            rows.append(
                {
                    "signal_id": signal.id,
                    "market_id": signal.market_id,
                    "status": signal.status,
                    "direction": signal.direction,
                    "confirmation_score": signal.confirmation_score,
                    "time_to_decision_seconds": round(_time_to_decision_seconds(signal), 4),
                    "max_favorable_move": max_favorable_move,
                    "max_adverse_move": max_adverse_move,
                    "decision_at": _decision_time(signal).isoformat(),
                    "horizon_snapshots": horizon_snapshots,
                }
            )

    return rows


def export_signal_confirmation_analysis_csv(
    session: Session,
    output_path: str | Path,
    horizon_snapshots: int = 20,
) -> dict[str, Any]:
    """
    Export terminal confirmation outcomes for offline threshold tuning.
    """
    rows = build_signal_confirmation_analysis_rows(
        session=session,
        horizon_snapshots=horizon_snapshots,
    )

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "signal_id",
        "market_id",
        "status",
        "direction",
        "confirmation_score",
        "time_to_decision_seconds",
        "max_favorable_move",
        "max_adverse_move",
        "decision_at",
        "horizon_snapshots",
    ]

    with output.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return {
        "output_path": str(output),
        "rows_written": len(rows),
        "horizon_snapshots": horizon_snapshots,
    }
