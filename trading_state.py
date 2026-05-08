from __future__ import annotations

import os


_enabled = True


def _env_default_enabled() -> bool:
    value = str(os.getenv("TRADING_ENABLED", "true")).strip().lower()
    return value in {"1", "true", "yes", "on"}


def is_trading_enabled() -> bool:
    return _enabled and _env_default_enabled()


def enable_trading() -> None:
    global _enabled
    _enabled = True


def disable_trading() -> None:
    global _enabled
    _enabled = False

