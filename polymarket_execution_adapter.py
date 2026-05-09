from __future__ import annotations

import os
from typing import Any

import requests
from sqlalchemy.orm import Session

from backend.execution_adapter import ExecutionAdapter, ExecutionFill
from backend.execution_errors import ExecutionDryRun
from backend.models import Market


def polymarket_config_status() -> dict[str, Any]:
    required = [
        "POLYMARKET_EXECUTION_ENABLED",
        "POLYMARKET_API_URL",
        "POLYMARKET_API_KEY",
    ]
    present = {key: bool(os.getenv(key)) for key in required}
    enabled = str(os.getenv("POLYMARKET_EXECUTION_ENABLED", "false")).strip().lower() in {"1", "true", "yes", "on"}
    dry_run = str(os.getenv("POLYMARKET_DRY_RUN", "true")).strip().lower() in {"1", "true", "yes", "on"}
    ok = enabled and all(present.values())
    return {
        "enabled": enabled,
        "ok": ok,
        "dry_run": dry_run,
        "present": present,
    }


class PolymarketExecutionAdapter(ExecutionAdapter):
    def __init__(self):
        status = polymarket_config_status()
        if not status["ok"]:
            raise ValueError("Polymarket execution is not configured")
        self._dry_run = bool(status.get("dry_run"))
        self._base_url = str(os.getenv("POLYMARKET_API_URL", "")).rstrip("/")
        self._api_key = str(os.getenv("POLYMARKET_API_KEY", ""))
        self._orders_path = str(os.getenv("POLYMARKET_ORDERS_PATH", "/orders")).strip() or "/orders"
        if not self._orders_path.startswith("/"):
            self._orders_path = f"/{self._orders_path}"
        self._timeout = float(os.getenv("POLYMARKET_TIMEOUT_SECONDS", "15"))

    @staticmethod
    def _as_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _parse_fills(self, payload: Any, *, fallback_quantity: float) -> list[ExecutionFill]:
        rows: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            maybe_fills = payload.get("fills")
            if isinstance(maybe_fills, list):
                rows = [row for row in maybe_fills if isinstance(row, dict)]
            elif isinstance(payload.get("fill"), dict):
                rows = [payload["fill"]]
            elif any(key in payload for key in ("price", "avg_price", "filled_price")):
                rows = [payload]
        elif isinstance(payload, list):
            rows = [row for row in payload if isinstance(row, dict)]

        fills: list[ExecutionFill] = []
        for row in rows:
            price = self._as_float(
                row.get("price", row.get("avg_price", row.get("filled_price"))),
                default=0.0,
            )
            quantity = self._as_float(
                row.get("quantity", row.get("filled_quantity", fallback_quantity)),
                default=fallback_quantity,
            )
            fee = self._as_float(row.get("fee", 0.0), default=0.0)
            if price <= 0.0 or quantity <= 0.0:
                continue
            fills.append(ExecutionFill(price=price, quantity=quantity, fee=fee))
        return fills

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
        if self._dry_run:
            raise ExecutionDryRun("polymarket_dry_run")

        market = session.get(Market, int(market_id))
        market_ref: str | int = int(market_id)
        if market is not None and getattr(market, "external_id", None):
            market_ref = str(market.external_id)

        payload: dict[str, Any] = {
            "market_id": market_ref,
            "side": side.upper(),
            "quantity": float(quantity),
            "type": "LIMIT" if limit_price is not None else "MARKET",
        }
        if external_id:
            payload["client_order_id"] = str(external_id)
        if limit_price is not None:
            payload["limit_price"] = float(limit_price)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self._api_key}",
            "X-API-Key": self._api_key,
        }

        url = f"{self._base_url}{self._orders_path}"
        try:
            response = requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=self._timeout,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            raise RuntimeError(f"Polymarket place_order failed: {exc}") from exc
        except ValueError as exc:
            raise RuntimeError("Polymarket place_order returned non-JSON response") from exc

        return self._parse_fills(data, fallback_quantity=float(quantity))
