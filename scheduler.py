import time
from datetime import UTC, datetime
import logging

from backend.db import SessionLocal
from backend.ingest import get_market_source, sync_market_data
from backend.scanner import run_market_scanner
from backend.scheduler_state import is_running, set_running
from backend.signal_confirmation_processor import process_all_signal_confirmations
from backend.execution_factory import get_execution_adapter
from backend.trading_orchestrator import execute_confirmed_signals
from backend.job_run_recorder import run_job
from backend.logging_utils import log_event


logger = logging.getLogger(__name__)


INTERVAL_SECONDS = 300


def run_once():
    db = SessionLocal()

    try:
        def _fn():
            source = get_market_source()
            sync_summary = sync_market_data(db, source)
            scan_summary = run_market_scanner(db)
            confirmations_summary = process_all_signal_confirmations(db)
            execution_summary = execute_confirmed_signals(db, get_execution_adapter())
            summary = {
                "sync": sync_summary,
                "scanner": scan_summary,
                "confirmations": confirmations_summary,
                "execution": execution_summary,
            }
            return summary, summary

        summary = run_job(db, job_type="pipeline", fn=_fn)
        log_event(logger, "scheduler_pipeline_done", summary=summary)

    except Exception as exc:
        db.rollback()
        log_event(logger, "scheduler_pipeline_failed", level="error", error=exc.__class__.__name__)
        raise

    finally:
        db.close()


def main():
    log_event(logger, "scheduler_loop_start", interval_seconds=INTERVAL_SECONDS)

    set_running(True)

    while is_running():
        try:
            run_once()
        except Exception:
            log_event(logger, "scheduler_loop_error", level="error")

        log_event(logger, "scheduler_sleep", interval_seconds=INTERVAL_SECONDS)
        time.sleep(INTERVAL_SECONDS)

    log_event(logger, "scheduler_loop_stopped")

if __name__ == "__main__":
    main()
