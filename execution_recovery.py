from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.models import Fill, Order, Position, Signal
from backend.reconciliation import rebuild_positions_from_fills


DEFAULT_RECOVERY_CONFIG: dict[str, Any] = {
    "stale_order_seconds": 900,
    "in_flight_statuses": ("SUBMITTED", "PENDING", "OPEN", "PARTIAL"),
}


def mark_stale_orders(session: Session, *, config: dict[str, Any] | None = None) -> dict[str, int]:
    effective = {**DEFAULT_RECOVERY_CONFIG, **(config or {})}
    seconds = int(effective.get("stale_order_seconds") or 0)
    statuses = tuple(effective.get("in_flight_statuses") or ())

    if seconds <= 0 or not statuses:
        return {"stale_marked": 0}

    cutoff = datetime.now(UTC).replace(tzinfo=None) - timedelta(seconds=seconds)
    candidates = list(
        session.scalars(
            select(Order)
            .where(Order.status.in_(statuses))
            .where(Order.created_at <= cutoff)
            .order_by(Order.created_at.asc())
        )
    )

    marked = 0
    for order in candidates:
        fill_count = int(session.scalar(select(func.count(Fill.id)).where(Fill.order_id == order.id)) or 0)
        if fill_count > 0:
            continue
        order.status = "STALE"
        marked += 1

    session.commit()
    return {"stale_marked": marked}


def recover_trading_state(session: Session, *, config: dict[str, Any] | None = None) -> dict[str, int]:
    stale = mark_stale_orders(session, config=config)
    rebuilt = rebuild_positions_from_fills(session)

    executed_updates = 0
    orders = list(session.scalars(select(Order).order_by(Order.id.asc())))
    for order in orders:
        signal = session.scalars(select(Signal).where(Signal.id == order.signal_id).limit(1)).first()
        if signal is None:
            continue

        if order.status in {"FILLED", "REJECTED", "CANCELLED", "STALE"}:
            if not bool(getattr(signal, "is_executed", False)):
                signal.is_executed = True
                executed_updates += 1

    session.commit()
    return {
        **stale,
        **rebuilt,
        "signals_marked_executed": executed_updates,
    }

