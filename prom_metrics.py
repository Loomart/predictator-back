from __future__ import annotations

from typing import Any

try:
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

    _ENABLED = True
except Exception:
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4"  # type: ignore
    _ENABLED = False


if _ENABLED:
    http_requests_total = Counter(
        "backend_http_requests_total",
        "Total HTTP requests",
        ["method", "path", "status"],
    )
    http_request_latency_seconds = Histogram(
        "backend_http_request_latency_seconds",
        "HTTP request latency (seconds)",
        ["method", "path"],
    )


def enabled() -> bool:
    return _ENABLED


def observe_request(*, method: str, path: str, status: int, elapsed_seconds: float) -> None:
    if not _ENABLED:
        return
    http_requests_total.labels(method=method, path=path, status=str(status)).inc()
    http_request_latency_seconds.labels(method=method, path=path).observe(max(0.0, float(elapsed_seconds)))


def render_latest() -> tuple[bytes, str]:
    if not _ENABLED:
        payload = b"# prom disabled: prometheus_client not installed\n"
        return payload, CONTENT_TYPE_LATEST
    return generate_latest(), CONTENT_TYPE_LATEST

