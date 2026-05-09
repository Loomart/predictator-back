from __future__ import annotations

from datetime import datetime, timezone
import logging
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from backend.execution_adapter import ExecutionAdapter
from backend.models import Fill, Order, Position, Signal
from backend.risk_manager import RiskDecision, propose_order_for_signal
from backend.trading_state import is_trading_enabled
from backend.circuit_breaker import is_open as is_execution_circuit_open, record_failure, record_success
from backend.retry_policy import RetryPolicy, retry_call
from backend.logging_utils import bind_context, log_event
from backend.execution_errors import ExecutionDryRun


logger = logging.getLogger(__name__)


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

    retry_cfg = execution_config or {}
    retry_policy = RetryPolicy(
        max_attempts=int(retry_cfg.get("max_attempts", 3)),
        base_delay_seconds=float(retry_cfg.get("base_delay_seconds", 0.05)),
        max_delay_seconds=float(retry_cfg.get("max_delay_seconds", 0.25)),
    )

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

        with bind_context(signal_id=int(signal.id)):
            expected_external_id = f"signal:{signal.id}"
            order = session.scalars(
                select(Order)
                .where(Order.external_id == expected_external_id)
                .order_by(Order.created_at.desc())
                .limit(1)
            ).first()
            if order is None:
                order = session.scalars(
                    select(Order)
                    .where(Order.signal_id == signal.id)
                    .order_by(Order.created_at.desc())
                    .limit(1)
                ).first()

            if order is not None and order.status in {"FILLED", "REJECTED", "CANCELLED", "STALE"}:
                signal.is_executed = True
                signal.last_evaluated_at = now
                session.commit()
                continue

            if order is None:
                decision: RiskDecision = propose_order_for_signal(session, signal, config=risk_config)
                if not decision.allowed:
                    blocked += 1
                    continue

                order = Order(
                    signal_id=signal.id,
                    market_id=signal.market_id,
                    side=str(decision.side),
                    order_type=str(decision.order_type or "MARKET"),
                    quantity=float(decision.quantity or 0.0),
                    limit_price=decision.limit_price,
                    status="SUBMITTED",
                    external_id=expected_external_id,
                    created_at=now,
                    updated_at=now,
                    retry_count=0,
                    last_error=None,
                )
                session.add(order)
                session.commit()
                session.refresh(order)
                with bind_context(order_id=int(order.id)):
                    log_event(
                        logger,
                        "order_submitted",
                        market_id=int(order.market_id),
                        side=str(order.side),
                        quantity=float(order.quantity),
                        limit_price=order.limit_price,
                        external_id=str(order.external_id or ""),
                    )

            external_id = str(order.external_id or expected_external_id)

            def _is_retriable(exc: Exception) -> bool:
                return not isinstance(exc, ValueError)

            def _place() -> list[Any]:
                return adapter.place_order(
                    session,
                    market_id=signal.market_id,
                    side=str(order.side),
                    quantity=float(order.quantity),
                    limit_price=order.limit_price,
                    external_id=external_id,
                )

            try:
                fills = retry_call(_place, policy=retry_policy, is_retriable=_is_retriable)
            except ExecutionDryRun:
                order.status = "DRY_RUN"
                order.updated_at = now
                session.commit()
                with bind_context(order_id=int(order.id)):
                    log_event(
                        logger,
                        "order_dry_run",
                        market_id=int(order.market_id),
                        side=str(order.side),
                        quantity=float(order.quantity),
                    )
                continue
            except Exception as exc:
                record_failure()
                order.status = "ERROR"
                order.retry_count = int(getattr(order, "retry_count", 0) or 0) + 1
                order.last_error = exc.__class__.__name__
                order.updated_at = now
                session.commit()
                errored += 1
                with bind_context(order_id=int(order.id)):
                    log_event(
                        logger,
                        "order_place_failed",
                        level="error",
                        market_id=int(order.market_id),
                        error=exc.__class__.__name__,
                        retry_count=int(order.retry_count),
                    )
                continue

            if not fills:
                order.status = "REJECTED"
                order.updated_at = now
                session.commit()
                rejected += 1
                signal.is_executed = True
                signal.last_evaluated_at = now
                session.commit()
                with bind_context(order_id=int(order.id)):
                    log_event(
                        logger,
                        "order_rejected",
                        market_id=int(order.market_id),
                        side=str(order.side),
                        quantity=float(order.quantity),
                    )
                continue

            record_success()

            filled_qty = 0.0
            last_price = None
            for f in fills:
                existing = session.scalar(
                    select(func.count(Fill.id))
                    .where(Fill.order_id == order.id)
                    .where(Fill.price == float(f.price))
                    .where(Fill.quantity == float(f.quantity))
                    .where(Fill.fee == float(f.fee))
                    .where(Fill.filled_at == now)
                )
                if int(existing or 0) > 0:
                    continue
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
            _apply_fill_to_position(
                position,
                side=str(order.side),
                price=float(last_price or 0.0),
                quantity=float(filled_qty),
            )
            position.updated_at = now

            signal.is_executed = True
            signal.last_evaluated_at = now
            executed += 1

            session.commit()
            with bind_context(order_id=int(order.id)):
                log_event(
                    logger,
                    "order_filled",
                    market_id=int(order.market_id),
                    side=str(order.side),
                    quantity=float(filled_qty),
                    price=float(last_price or 0.0),
                    status=str(order.status),
                )


    return {
        "signals_considered": processed,
        "signals_executed": executed,
        "signals_blocked": blocked,
        "signals_rejected": rejected,
        "trading_enabled": 1,
        "circuit_open": 0,
        "signals_errored": errored,
    }
