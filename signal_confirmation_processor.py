from __future__ import annotations

from datetime import datetime, UTC
import json
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.confirmation_config import DEFAULT_CONFIRMATION_CONFIG
from backend.confirmation_engine import (
    SignalStatus,
    evaluate_confirmation,
)
from backend.models import MarketSnapshot, Signal

logger = logging.getLogger(__name__)


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
            .where(Signal.status.in_([SignalStatus.WATCH.value, SignalStatus.CONFIRMING.value]))
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

    now = datetime.now(UTC).replace(tzinfo=None)

    updated = 0
    confirmed = 0
    invalidated = 0
    expired = 0
    confirming = 0

    for signal in signals:
        signal_snapshots = [s for s in snapshots if s.captured_at >= signal.created_at]
        result = evaluate_confirmation(signal, signal_snapshots, effective_config)

        # Business rule: WATCH must move to CONFIRMING after first evaluation,
        # unless it reached a terminal state.
        if (
            signal.status == SignalStatus.WATCH.value
            and result.status_after
            not in {SignalStatus.CONFIRMED, SignalStatus.INVALIDATED, SignalStatus.EXPIRED}
        ):
            result_status_after = SignalStatus.CONFIRMING
        else:
            result_status_after = result.status_after

        old_status = signal.status
        old_score = signal.confirmation_score

        signal.status = result_status_after.value
        signal.confirmation_score = result.final_score
        signal.last_evaluated_at = now

        if effective_config.get("enable_confirmation_debug_logging", False):
            payload = {
                "event": "signal_confirmation_evaluated",
                **result.to_dict(),
                "signal_status_after_applied": result_status_after.value,
            }
            logger.info(json.dumps(payload, separators=(",", ":"), default=str))

        if old_status != signal.status or old_score != signal.confirmation_score:
            updated += 1

        if result_status_after == SignalStatus.CONFIRMED:
            confirmed += 1
        elif result_status_after == SignalStatus.INVALIDATED:
            invalidated += 1
        elif result_status_after == SignalStatus.EXPIRED:
            expired += 1
        elif result_status_after == SignalStatus.CONFIRMING:
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
