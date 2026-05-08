from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class CircuitBreakerState:
    failure_count: int = 0
    opened_until: datetime | None = None


_state = CircuitBreakerState()


DEFAULT_FAILURE_THRESHOLD = 3
DEFAULT_COOLDOWN_SECONDS = 120


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def is_open() -> bool:
    if _state.opened_until is None:
        return False
    return _now() < _state.opened_until


def record_failure(*, threshold: int = DEFAULT_FAILURE_THRESHOLD, cooldown_seconds: int = DEFAULT_COOLDOWN_SECONDS) -> None:
    _state.failure_count += 1
    if _state.failure_count >= max(1, int(threshold)):
        _state.opened_until = _now() + timedelta(seconds=max(1, int(cooldown_seconds)))


def record_success() -> None:
    _state.failure_count = 0
    _state.opened_until = None


def reset() -> None:
    record_success()


def status() -> dict[str, object]:
    opened_until = _state.opened_until
    return {
        "is_open": is_open(),
        "failure_count": _state.failure_count,
        "opened_until": opened_until.isoformat() if opened_until else None,
    }

