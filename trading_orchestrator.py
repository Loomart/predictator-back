from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.execution_adapter import ExecutionAdapter
from backend.models import Fill, Order, Position, Signal
from backend.risk_manager import RiskDecision, propose_order_for_signal
from backend.trading_state import is_trading_enabled
from backend.circuit_breaker import is_open as is_execution_circuit_open, record_failure, record_success


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_or_create_position(session: Session, market_id: int) -> Position:
    pos = session.scalars(select(Position).where(Position.market_id == market_id).limit(1)).first()
    if pos is not None:
        return pos
    pos = Position(market_id=market_id, quantity=0.0, avg_price=None, realized_pnl=0.0)
    session.add(pos)
    session.flush()
    return pos


def _apply_fill_to_position(position: Position, *, side: str, price: float, quantity: float) -> None:
    signed_qty = quantity if side.upper() == "BUY" else -quantity
    old_qty = _as_float(position.quantity, 0.0)
    old_avg = position.avg_price
    new_qty = old_qty + signed_qty

    if old_qty == 0.0:
        position.quantity = new_qty
        position.avg_price = float(price)
        return

    same_direction = (old_qty >= 0 and signed_qty >= 0) or (old_qty <= 0 and signed_qty <= 0)
    if same_direction:
        old_cost = abs(old_qty) * float(old_avg or price)
        add_cost = abs(signed_qty) * float(price)
        denom = abs(new_qty)
        position.quantity = new_qty
        position.avg_price = (old_cost + add_cost) / denom if denom > 0 else float(price)
        return

    closing_qty = min(abs(old_qty), abs(signed_qty))
    entry_price = float(old_avg or price)
    pnl = closing_qty * (float(price) - entry_price) * (1.0 if old_qty > 0 else -1.0)
    position.realized_pnl = _as_float(position.realized_pnl, 0.0) + pnl

    position.quantity = new_qty
    if new_qty == 0.0:
        position.avg_price = None
    else:
        position.avg_price = float(price)


def execute_confirmed_signals(
    session: Session,
    adapter: ExecutionAdapter,
    *,
    limit: int = 25,
    risk_config: dict[str, Any] | None = None,
    execution_config: dict[str, Any] | None = None,
) -> dict[str, int]:
    if not is_trading_enabled():
        return {
            "signals_considered": 0,
            "signals_executed": 0,
            "signals_blocked": 0,
            "signals_rejected": 0,
            "trading_enabled": 0,
            "circuit_open": 0,
        }

    if is_execution_circuit_open():
        return {
            "signals_considered": 0,
            "signals_executed": 0,
            "signals_blocked": 0,
            "signals_rejected": 0,
            "trading_enabled": 1,
            "circuit_open": 1,
        }

    now = datetime.now(timezone.utc).replace(tzinfo=None)

    confirmed = list(
        session.scalars(
            select(Signal)
            .where(Signal.status == "CONFIRMED")
            .where(Signal.is_executed == False)
            .order_by(Signal.created_at.asc())
            .limit(limit)
        )
    )

    processed = 0
    executed = 0
    blocked = 0
    rejected = 0
    errored = 0

    for signal in confirmed:
        processed += 1

        existing_order = session.scalar(select(func.count(Order.id)).where(Order.signal_id == signal.id))
        if int(existing_order or 0) > 0:
            signal.is_executed = True
            signal.last_evaluated_at = now
            continue

        decision: RiskDecision = propose_order_for_signal(session, signal, config=risk_config)
        if not decision.allowed:
            blocked += 1
            continue

        external_id = f"signal:{signal.id}"

        order = Order(
            signal_id=signal.id,
            market_id=signal.market_id,
            side=str(decision.side),
            order_type=str(decision.order_type or "MARKET"),
            quantity=float(decision.quantity or 0.0),
            limit_price=decision.limit_price,
            status="PENDING",
            external_id=external_id,
            created_at=now,
            updated_at=now,
        )
        session.add(order)
        session.flush()

        try:
            fills = adapter.place_order(
                session,
                market_id=signal.market_id,
                side=str(decision.side),
                quantity=float(decision.quantity or 0.0),
                limit_price=decision.limit_price,
                external_id=external_id,
            )
        except Exception:
            record_failure()
            order.status = "ERROR"
            order.updated_at = now
            session.flush()
            errored += 1
            continue

        if not fills:
            order.status = "REJECTED"
            order.updated_at = now
            session.flush()
            rejected += 1
            continue

        record_success()

        filled_qty = 0.0
        last_price = None

        for f in fills:
            fill = Fill(
                order_id=order.id,
                price=float(f.price),
                quantity=float(f.quantity),
                fee=float(f.fee),
                filled_at=now,
                created_at=now,
            )
            session.add(fill)
            filled_qty += float(f.quantity)
            last_price = float(f.price)

        order.status = "FILLED" if filled_qty >= float(order.quantity) else "PARTIAL"
        order.updated_at = now
        session.flush()

        position = _get_or_create_position(session, signal.market_id)
        _apply_fill_to_position(position, side=str(order.side), price=float(last_price or 0.0), quantity=float(filled_qty))
        position.updated_at = now

        signal.is_executed = True
        signal.last_evaluated_at = now
        executed += 1

    session.commit()

    return {
        "signals_considered": processed,
        "signals_executed": executed,
        "signals_blocked": blocked,
        "signals_rejected": rejected,
        "trading_enabled": 1,
        "circuit_open": 0,
        "signals_errored": errored,
    }
