from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.models import MarketSnapshot, Order, Position, Signal


DEFAULT_RISK_CONFIG: dict[str, Any] = {
    "min_liquidity": 5000.0,
    "max_spread": 0.08,
    "max_open_orders": 10,
    "max_open_orders_per_market": 1,
    "max_position_abs_per_market": 100.0,
    "max_total_abs_position": 300.0,
    "max_orders_per_day": 0,
    "max_traded_quantity_per_day": 0.0,
    "base_quantity": 10.0,
    "max_edge": 0.05,
    "open_order_statuses": ("PENDING", "OPEN", "PARTIAL"),
}


def _clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str
    side: str | None = None
    quantity: float | None = None
    order_type: str | None = None
    limit_price: float | None = None


def get_latest_snapshot(session: Session, market_id: int) -> MarketSnapshot | None:
    return session.scalars(
        select(MarketSnapshot)
        .where(MarketSnapshot.market_id == market_id)
        .order_by(MarketSnapshot.captured_at.desc())
        .limit(1)
    ).first()


def propose_order_for_signal(
    session: Session,
    signal: Signal,
    config: dict[str, Any] | None = None,
) -> RiskDecision:
    effective = {**DEFAULT_RISK_CONFIG, **(config or {})}

    if str(getattr(signal, "status", "")).upper() != "CONFIRMED":
        return RiskDecision(allowed=False, reason="signal_not_confirmed")

    if bool(getattr(signal, "is_executed", False)):
        return RiskDecision(allowed=False, reason="signal_already_executed")

    direction = str(getattr(signal, "direction", "") or "").upper()
    if direction == "UP":
        side = "BUY"
    elif direction == "DOWN":
        side = "SELL"
    else:
        return RiskDecision(allowed=False, reason="missing_direction")

    latest_snapshot = get_latest_snapshot(session, signal.market_id)
    if latest_snapshot is None:
        return RiskDecision(allowed=False, reason="missing_market_snapshot")

    min_liquidity = _as_float(effective.get("min_liquidity"), 0.0)
    max_spread = _as_float(effective.get("max_spread"), 1.0)
    snapshot_liquidity = _as_float(latest_snapshot.liquidity, 0.0)
    snapshot_spread = _as_float(latest_snapshot.spread, 0.0)

    if snapshot_liquidity < min_liquidity:
        return RiskDecision(allowed=False, reason="liquidity_below_min")

    if snapshot_spread > max_spread:
        return RiskDecision(allowed=False, reason="spread_above_max")

    max_orders_per_day = int(_as_float(effective.get("max_orders_per_day"), 0.0))
    max_qty_per_day = _as_float(effective.get("max_traded_quantity_per_day"), 0.0)
    if max_orders_per_day > 0 or max_qty_per_day > 0:
        now = datetime.now(UTC).replace(tzinfo=None)
        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        if max_orders_per_day > 0:
            orders_today = session.scalar(select(func.count(Order.id)).where(Order.created_at >= day_start))
            if int(orders_today or 0) >= max_orders_per_day:
                return RiskDecision(allowed=False, reason="max_orders_per_day_reached")

        if max_qty_per_day > 0:
            qty_today = session.scalar(select(func.coalesce(func.sum(func.abs(Order.quantity)), 0.0)).where(Order.created_at >= day_start))
            if float(qty_today or 0.0) >= max_qty_per_day:
                return RiskDecision(allowed=False, reason="max_traded_quantity_per_day_reached")

    open_statuses = tuple(effective.get("open_order_statuses") or ())
    max_open_orders = int(_as_float(effective.get("max_open_orders"), 0.0))
    max_open_orders_per_market = int(_as_float(effective.get("max_open_orders_per_market"), 0.0))

    open_orders_total = session.scalar(
        select(func.count(Order.id)).where(Order.status.in_(open_statuses))
    )
    open_orders_total = int(open_orders_total or 0)
    if max_open_orders > 0 and open_orders_total >= max_open_orders:
        return RiskDecision(allowed=False, reason="max_open_orders_reached")

    open_orders_market = session.scalar(
        select(func.count(Order.id))
        .where(Order.market_id == signal.market_id)
        .where(Order.status.in_(open_statuses))
    )
    open_orders_market = int(open_orders_market or 0)
    if max_open_orders_per_market > 0 and open_orders_market >= max_open_orders_per_market:
        return RiskDecision(allowed=False, reason="max_open_orders_per_market_reached")

    position = session.scalars(
        select(Position).where(Position.market_id == signal.market_id).limit(1)
    ).first()
    current_qty = _as_float(getattr(position, "quantity", 0.0), 0.0)

    base_quantity = _as_float(effective.get("base_quantity"), 0.0)
    max_edge = max(1e-9, _as_float(effective.get("max_edge"), 0.05))
    confidence = _clamp(_as_float(getattr(signal, "confidence", 0.0), 0.0))
    edge = max(0.0, _as_float(getattr(signal, "edge_estimate", 0.0), 0.0))
    edge_factor = _clamp(edge / max_edge)

    quality = max(confidence, edge_factor)
    quantity = max(0.0, base_quantity * (0.25 + 0.75 * quality))

    signed_delta = quantity if side == "BUY" else -quantity
    projected_qty = current_qty + signed_delta

    max_abs_per_market = _as_float(effective.get("max_position_abs_per_market"), 0.0)
    if max_abs_per_market > 0 and abs(projected_qty) > max_abs_per_market:
        return RiskDecision(allowed=False, reason="max_position_per_market_reached")

    total_abs_qty = session.scalar(select(func.coalesce(func.sum(func.abs(Position.quantity)), 0.0)))
    total_abs_qty = float(total_abs_qty or 0.0)
    max_total_abs = _as_float(effective.get("max_total_abs_position"), 0.0)
    if max_total_abs > 0 and (total_abs_qty + abs(signed_delta)) > max_total_abs:
        return RiskDecision(allowed=False, reason="max_total_exposure_reached")

    return RiskDecision(
        allowed=True,
        reason="ok",
        side=side,
        quantity=round(quantity, 6),
        order_type="MARKET",
        limit_price=None,
    )
