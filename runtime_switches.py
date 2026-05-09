from __future__ import annotations

import os
from typing import Any

from backend.market_filters import normalize_set, parse_csv_values


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_csv_set(name: str) -> set[str]:
    return normalize_set(parse_csv_values(os.getenv(name)))


def load_runtime_switches() -> dict[str, Any]:
    return {
        "filters": {
            "market_category_allowlist": _env_csv_set("MARKET_CATEGORY_ALLOWLIST"),
            "market_title_include": _env_csv_set("MARKET_TITLE_INCLUDE"),
            "market_external_id_allowlist": _env_csv_set("MARKET_EXTERNAL_ID_ALLOWLIST"),
        },
        "scanner": {
            "market_limit": _env_int("SCANNER_MARKET_LIMIT", 25),
            "min_history": _env_int("SCANNER_MIN_HISTORY", 3),
            "wait_liquidity_threshold": _env_float("SCANNER_WAIT_LIQUIDITY_THRESHOLD", 0.20),
            "wait_noise_threshold": _env_float("SCANNER_WAIT_NOISE_THRESHOLD", 0.18),
            "wait_stability_threshold": _env_float("SCANNER_WAIT_STABILITY_THRESHOLD", 0.25),
            "strong_enter_score_threshold": _env_float("SCANNER_STRONG_ENTER_SCORE_THRESHOLD", 0.75),
            "strong_enter_momentum_threshold": _env_float("SCANNER_STRONG_ENTER_MOMENTUM_THRESHOLD", 0.55),
            "strong_enter_change_threshold": _env_float("SCANNER_STRONG_ENTER_CHANGE_THRESHOLD", 0.02),
            "enter_score_threshold": _env_float("SCANNER_ENTER_SCORE_THRESHOLD", 0.60),
            "enter_momentum_threshold": _env_float("SCANNER_ENTER_MOMENTUM_THRESHOLD", 0.45),
            "enter_change_threshold": _env_float("SCANNER_ENTER_CHANGE_THRESHOLD", 0.015),
            "watch_score_threshold": _env_float("SCANNER_WATCH_SCORE_THRESHOLD", 0.55),
            "watch_momentum_threshold": _env_float("SCANNER_WATCH_MOMENTUM_THRESHOLD", 0.30),
            "avoid_score_threshold": _env_float("SCANNER_AVOID_SCORE_THRESHOLD", 0.45),
        },
    }
