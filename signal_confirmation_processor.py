from __future__ import annotations

from datetime import datetime
import json
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from confirmation_engine import (
    compute_directional_delta,
    compute_persistence,
    evaluate_confirmation,
)
from models import MarketSnapshot, Signal

logger = logging.getLogger(__name__)


DEFAULT_CONFIRMATION_CONFIG: dict[str, Any] = {
    "confirmation_threshold": 0.65,
    "invalidation_score": -0.15,
    "min_liquidity_ratio": 0.5,
    "good_liquidity_ratio": 1.0,
    "max_spread_multiplier": 2.0,
    "price_move_scale": 0.03,
    "w_price_move": 0.45,
    "w_persistence": 0.30,
    "w_liquidity": 0.20,
    "w_spread_penalty": 0.15,
    "min_confirmation_snapshots": 3,
    "min_persistence_for_confirmation": 0.5,
    "enable_confirmation_logs": False,
}


def process_signal_confirmations(
    session: Session,
    market_id: int,
    config: dict[str, Any] | None = None,
) -> dict[str, int]:
    """
    Process confirmation lifecycle for active signals of a market.

    Query strategy:
    - 1 query for candidate signals (WATCH/CONFIRMING)
    - 1 query for all snapshots needed across those signals
    """
    effective_config = {**DEFAULT_CONFIRMATION_CONFIG, **(config or {})}

    signals = list(
        session.scalars(
            select(Signal)
            .where(Signal.market_id == market_id)
            .where(Signal.status.in_(["WATCH", "CONFIRMING"]))
            .order_by(Signal.created_at.asc())
        )
    )

    if not signals:
        return {
            "market_id": market_id,
            "signals_considered": 0,
            "signals_updated": 0,
            "confirmed": 0,
            "invalidated": 0,
            "expired": 0,
            "confirming": 0,
        }

    min_created_at = min(signal.created_at for signal in signals)

    snapshots = list(
        session.scalars(
            select(MarketSnapshot)
            .where(MarketSnapshot.market_id == market_id)
            .where(MarketSnapshot.captured_at >= min_created_at)
            .order_by(MarketSnapshot.captured_at.asc())
        )
    )

    now = datetime.utcnow()

    updated = 0
    confirmed = 0
    invalidated = 0
    expired = 0
    confirming = 0

    for signal in signals:
        signal_snapshots = [s for s in snapshots if s.captured_at >= signal.created_at]
        latest_snapshot = signal_snapshots[-1] if signal_snapshots else None

        new_status, score = evaluate_confirmation(signal, signal_snapshots, effective_config)

        # Business rule: WATCH must move to CONFIRMING after first evaluation,
        # unless it reached a terminal state.
        if signal.status == "WATCH" and new_status not in {"CONFIRMED", "INVALIDATED", "EXPIRED"}:
            new_status = "CONFIRMING"

        old_status = signal.status
        old_score = signal.confirmation_score

        signal.status = new_status
        signal.confirmation_score = score
        signal.last_evaluated_at = now

        if effective_config.get("enable_confirmation_logs", False):
            latest_price = latest_snapshot.yes_price if latest_snapshot is not None else signal.reference_price
            payload = {
                "event": "signal_confirmation_evaluated",
                "signal_id": signal.id,
                "market_id": signal.market_id,
                "status_before": old_status,
                "status_after": new_status,
                "confirmation_score": score,
                "price_delta": compute_directional_delta(signal, latest_price),
                "persistence": compute_persistence(signal, signal_snapshots),
                "liquidity": latest_snapshot.liquidity if latest_snapshot is not None else None,
                "spread": latest_snapshot.spread if latest_snapshot is not None else None,
            }
            logger.info(json.dumps(payload, separators=(",", ":"), default=str))

        if old_status != signal.status or old_score != signal.confirmation_score:
            updated += 1

        if new_status == "CONFIRMED":
            confirmed += 1
        elif new_status == "INVALIDATED":
            invalidated += 1
        elif new_status == "EXPIRED":
            expired += 1
        elif new_status == "CONFIRMING":
            confirming += 1

    session.commit()

    return {
        "market_id": market_id,
        "signals_considered": len(signals),
        "signals_updated": updated,
        "confirmed": confirmed,
        "invalidated": invalidated,
        "expired": expired,
        "confirming": confirming,
    }
