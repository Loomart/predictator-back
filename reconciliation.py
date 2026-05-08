from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.models import Fill, Order, Position


def rebuild_positions_from_fills(session: Session) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    orders = list(session.scalars(select(Order).order_by(Order.id.asc())))
    if not orders:
        return {"positions_rebuilt": 0, "fills_processed": 0}

    order_ids = [o.id for o in orders]
    fills = list(session.scalars(select(Fill).where(Fill.order_id.in_(order_ids)).order_by(Fill.id.asc())))

    order_by_id: dict[int, Order] = {o.id: o for o in orders}

    position_by_market: dict[int, Position] = {}
    fills_processed = 0

    for fill in fills:
        order = order_by_id.get(fill.order_id)
        if order is None:
            continue

        market_id = int(order.market_id)
        pos = position_by_market.get(market_id)
        if pos is None:
            pos = session.scalars(select(Position).where(Position.market_id == market_id).limit(1)).first()
            if pos is None:
                pos = Position(market_id=market_id, quantity=0.0, avg_price=None, realized_pnl=0.0)
                session.add(pos)
                session.flush()
            pos.quantity = 0.0
            pos.avg_price = None
            pos.realized_pnl = 0.0
            position_by_market[market_id] = pos

        side = str(order.side or "").upper()
        signed_qty = float(fill.quantity) if side == "BUY" else -float(fill.quantity)
        price = float(fill.price)
        old_qty = float(pos.quantity or 0.0)
        old_avg = pos.avg_price
        new_qty = old_qty + signed_qty

        if old_qty == 0.0:
            pos.quantity = new_qty
            pos.avg_price = price
        else:
            same_direction = (old_qty >= 0 and signed_qty >= 0) or (old_qty <= 0 and signed_qty <= 0)
            if same_direction:
                base_price = float(old_avg or price)
                old_cost = abs(old_qty) * base_price
                add_cost = abs(signed_qty) * price
                denom = abs(new_qty)
                pos.quantity = new_qty
                pos.avg_price = (old_cost + add_cost) / denom if denom > 0 else price
            else:
                closing_qty = min(abs(old_qty), abs(signed_qty))
                entry_price = float(old_avg or price)
                pnl = closing_qty * (price - entry_price) * (1.0 if old_qty > 0 else -1.0)
                pos.realized_pnl = float(pos.realized_pnl or 0.0) + pnl
                pos.quantity = new_qty
                pos.avg_price = None if new_qty == 0.0 else price

        pos.updated_at = now
        fills_processed += 1

    session.commit()
    return {"positions_rebuilt": len(position_by_market), "fills_processed": fills_processed}

