from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Callable, TypeVar

from sqlalchemy.orm import Session

from backend.models import JobRun


T = TypeVar("T")


def run_job(
    session: Session,
    *,
    job_type: str,
    fn: Callable[[], tuple[dict[str, Any], T]],
    source_name: str | None = None,
) -> T:
    started = datetime.now(UTC).replace(tzinfo=None)
    status = "success"
    summary: dict[str, Any] = {}
    error_message: str | None = None

    try:
        summary, result = fn()
    except Exception as exc:
        status = "failed"
        error_message = f"{exc.__class__.__name__}: {exc}"
        finished = datetime.now(UTC).replace(tzinfo=None)
        duration = max(0.0, (finished - started).total_seconds())
        session.add(
            JobRun(
                job_type=job_type,
                status=status,
                source_name=source_name,
                summary_json=json.dumps(summary, separators=(",", ":"), default=str),
                started_at=started,
                finished_at=finished,
                duration_seconds=duration,
                error_message=error_message,
            )
        )
        session.commit()
        raise

    finished = datetime.now(UTC).replace(tzinfo=None)
    duration = max(0.0, (finished - started).total_seconds())
    session.add(
        JobRun(
            job_type=job_type,
            status=status,
            source_name=source_name,
            summary_json=json.dumps(summary, separators=(",", ":"), default=str),
            started_at=started,
            finished_at=finished,
            duration_seconds=duration,
            error_message=error_message,
        )
    )
    session.commit()
    return result

