from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.execution_adapter import ExecutionAdapter, ExecutionFill
from backend.models import MarketSnapshot


DEFAULT_PAPER_EXECUTION_CONFIG: dict[str, Any] = {
    "slippage_bps": 10.0,
    "fee_bps": 0.0,
}


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _latest_snapshot(session: Session, market_id: int) -> MarketSnapshot | None:
    return session.scalars(
        select(MarketSnapshot)
        .where(MarketSnapshot.market_id == market_id)
        .order_by(MarketSnapshot.captured_at.desc())
        .limit(1)
    ).first()


def _apply_slippage(price: float, *, side: str, slippage_bps: float) -> float:
    factor = max(0.0, slippage_bps) / 10_000.0
    if side.upper() == "BUY":
        return price * (1.0 + factor)
    return price * (1.0 - factor)


class PaperExecutionAdapter(ExecutionAdapter):
    def __init__(self, config: dict[str, Any] | None = None):
        self.config = {**DEFAULT_PAPER_EXECUTION_CONFIG, **(config or {})}

    def place_order(
        self,
        session: Session,
        *,
        market_id: int,
        side: str,
        quantity: float,
        limit_price: float | None = None,
        external_id: str | None = None,
    ) -> list[ExecutionFill]:
        snapshot = _latest_snapshot(session, market_id)
        if snapshot is None:
            return []

        best_bid = _as_float(snapshot.best_bid, 0.0)
        best_ask = _as_float(snapshot.best_ask, 0.0)
        mid = _as_float(snapshot.yes_price, 0.0)

        if best_bid > 0.0 and best_ask > 0.0 and best_ask >= best_bid:
            buy_price = best_ask
            sell_price = best_bid
        else:
            spread = _as_float(snapshot.spread, 0.0)
            buy_price = min(1.0, max(0.0, mid + spread / 2.0))
            sell_price = min(1.0, max(0.0, mid - spread / 2.0))

        side_u = side.upper()
        raw_price = buy_price if side_u == "BUY" else sell_price
        raw_price = min(1.0, max(0.0, raw_price))

        if limit_price is not None:
            lp = float(limit_price)
            if side_u == "BUY" and raw_price > lp:
                return []
            if side_u == "SELL" and raw_price < lp:
                return []

        slippage_bps = _as_float(self.config.get("slippage_bps"), 0.0)
        fee_bps = _as_float(self.config.get("fee_bps"), 0.0)

        exec_price = _apply_slippage(raw_price, side=side_u, slippage_bps=slippage_bps)
        exec_price = min(1.0, max(0.0, exec_price))

        fee = abs(exec_price * quantity) * (max(0.0, fee_bps) / 10_000.0)
        return [ExecutionFill(price=round(exec_price, 6), quantity=round(float(quantity), 6), fee=round(fee, 6))]
