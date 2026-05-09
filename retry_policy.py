from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Callable, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay_seconds: float = 0.1
    max_delay_seconds: float = 1.0


def retry_call(
    fn: Callable[[], T],
    *,
    policy: RetryPolicy,
    is_retriable: Callable[[Exception], bool] | None = None,
) -> T:
    attempts = max(1, int(policy.max_attempts))
    base = max(0.0, float(policy.base_delay_seconds))
    cap = max(0.0, float(policy.max_delay_seconds))

    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            if is_retriable is not None and not is_retriable(exc):
                break

            delay = base * (2 ** (attempt - 1))
            if cap > 0.0:
                delay = min(delay, cap)
            if delay > 0.0:
                time.sleep(delay)

    assert last_exc is not None
    raise last_exc

