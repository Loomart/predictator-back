from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.orm import Session


@dataclass(frozen=True)
class ExecutionFill:
    price: float
    quantity: float
    fee: float = 0.0


class ExecutionAdapter:
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
        raise NotImplementedError
