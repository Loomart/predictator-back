from __future__ import annotations

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.models import MarketSnapshot, Order, Position


def trading_summary(session: Session) -> dict[str, Any]:
    open_statuses = ("PENDING", "OPEN", "PARTIAL")

    open_orders = session.scalar(select(func.count(Order.id)).where(Order.status.in_(open_statuses)))
    total_orders = session.scalar(select(func.count(Order.id)))
    total_filled_orders = session.scalar(select(func.count(Order.id)).where(Order.status == "FILLED"))
    total_rejected_orders = session.scalar(select(func.count(Order.id)).where(Order.status == "REJECTED"))

    positions = list(session.scalars(select(Position).order_by(Position.market_id.asc())))
    total_abs_position = sum(abs(float(p.quantity or 0.0)) for p in positions)
    total_realized_pnl = sum(float(p.realized_pnl or 0.0) for p in positions)

    return {
        "orders": {
            "open": int(open_orders or 0),
            "total": int(total_orders or 0),
            "filled": int(total_filled_orders or 0),
            "rejected": int(total_rejected_orders or 0),
        },
        "positions": {
            "count": len(positions),
            "total_abs_quantity": round(total_abs_position, 6),
            "total_realized_pnl": round(total_realized_pnl, 6),
        },
    }


def positions_detail(session: Session) -> list[dict[str, Any]]:
    positions = list(session.scalars(select(Position).order_by(Position.market_id.asc())))
    if not positions:
        return []

    market_ids = [p.market_id for p in positions]
    snapshots = list(
        session.scalars(
            select(MarketSnapshot)
            .where(MarketSnapshot.market_id.in_(market_ids))
            .order_by(MarketSnapshot.market_id.asc(), MarketSnapshot.captured_at.desc())
        )
    )

    latest_by_market: dict[int, MarketSnapshot] = {}
    for snap in snapshots:
        if snap.market_id not in latest_by_market:
            latest_by_market[snap.market_id] = snap

    rows: list[dict[str, Any]] = []
    for p in positions:
        mark = None
        snap = latest_by_market.get(p.market_id)
        if snap is not None and snap.yes_price is not None:
            mark = float(snap.yes_price)

        qty = float(p.quantity or 0.0)
        avg = float(p.avg_price) if p.avg_price is not None else None

        unrealized = None
        if mark is not None and avg is not None:
            unrealized = (mark - avg) * qty

        rows.append(
            {
                "market_id": int(p.market_id),
                "quantity": round(qty, 6),
                "avg_price": round(avg, 6) if avg is not None else None,
                "mark_price": round(mark, 6) if mark is not None else None,
                "unrealized_pnl": round(float(unrealized), 6) if unrealized is not None else None,
                "realized_pnl": round(float(p.realized_pnl or 0.0), 6),
                "updated_at": p.updated_at.isoformat() if getattr(p, "updated_at", None) else None,
            }
        )

    return rows
