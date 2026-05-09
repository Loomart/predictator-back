from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
import json
from typing import Any, Iterator


_request_id: ContextVar[str | None] = ContextVar("request_id", default=None)
_signal_id: ContextVar[int | None] = ContextVar("signal_id", default=None)
_order_id: ContextVar[int | None] = ContextVar("order_id", default=None)
_replay_run_id: ContextVar[str | None] = ContextVar("replay_run_id", default=None)


@contextmanager
def bind_context(
    *,
    request_id: str | None = None,
    signal_id: int | None = None,
    order_id: int | None = None,
    replay_run_id: str | None = None,
) -> Iterator[None]:
    tokens = []
    if request_id is not None:
        tokens.append((_request_id, _request_id.set(request_id)))
    if signal_id is not None:
        tokens.append((_signal_id, _signal_id.set(signal_id)))
    if order_id is not None:
        tokens.append((_order_id, _order_id.set(order_id)))
    if replay_run_id is not None:
        tokens.append((_replay_run_id, _replay_run_id.set(replay_run_id)))
    try:
        yield
    finally:
        for var, tok in reversed(tokens):
            var.reset(tok)


def _base_fields() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ts": datetime.now(UTC).isoformat(),
    }
    rid = _request_id.get()
    sid = _signal_id.get()
    oid = _order_id.get()
    rpid = _replay_run_id.get()
    if rid is not None:
        payload["request_id"] = rid
    if sid is not None:
        payload["signal_id"] = sid
    if oid is not None:
        payload["order_id"] = oid
    if rpid is not None:
        payload["replay_run_id"] = rpid
    return payload


def log_event(logger, event: str, *, level: str = "info", **fields: Any) -> None:
    payload = {**_base_fields(), "event": event, **fields}
    msg = json.dumps(payload, separators=(",", ":"), default=str)
    fn = getattr(logger, level, None)
    if callable(fn):
        fn(msg)
    else:
        logger.info(msg)

