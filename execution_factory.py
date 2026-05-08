from __future__ import annotations

import os
from typing import Any

from backend.execution_adapter import ExecutionAdapter
from backend.paper_execution_adapter import PaperExecutionAdapter
from backend.polymarket_execution_adapter import PolymarketExecutionAdapter, polymarket_config_status


def get_execution_mode() -> str:
    return str(os.getenv("EXECUTION_MODE", "paper")).strip().lower()


def get_execution_adapter() -> ExecutionAdapter:
    mode = get_execution_mode()
    if mode == "paper":
        return PaperExecutionAdapter()
    if mode == "polymarket":
        return PolymarketExecutionAdapter()
    raise ValueError(f"Unknown EXECUTION_MODE: {mode}")


def execution_status() -> dict[str, Any]:
    mode = get_execution_mode()
    status: dict[str, Any] = {
        "mode": mode,
    }
    if mode == "polymarket":
        status["polymarket"] = polymarket_config_status()
    return status


def execution_health() -> dict[str, Any]:
    mode = get_execution_mode()
    payload: dict[str, Any] = {
        "mode": mode,
        "ok": True,
    }
    if mode == "paper":
        return payload
    if mode == "polymarket":
        pm = polymarket_config_status()
        payload["polymarket"] = pm
        payload["ok"] = bool(pm.get("ok"))
        return payload
    payload["ok"] = False
    payload["error"] = "unknown_execution_mode"
    return payload
