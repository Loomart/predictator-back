import time
from datetime import UTC, datetime

from backend.db import SessionLocal
from backend.ingest import get_market_source, sync_market_data
from backend.scanner import run_market_scanner
from backend.scheduler_state import is_running, set_running
from backend.signal_confirmation_processor import process_all_signal_confirmations
from backend.execution_factory import get_execution_adapter
from backend.trading_orchestrator import execute_confirmed_signals


INTERVAL_SECONDS = 300


def run_once():
    db = SessionLocal()

    try:
        print(f"[SCHEDULER] Pipeline start at {datetime.now(UTC).replace(tzinfo=None).isoformat()} UTC")

        source = get_market_source()

        sync_summary = sync_market_data(db, source)
        scan_summary = run_market_scanner(db)
        confirmations_summary = process_all_signal_confirmations(db)
        execution_summary = execute_confirmed_signals(db, get_execution_adapter())

        print("[SCHEDULER] Sync summary:", sync_summary)
        print("[SCHEDULER] Scan summary:", scan_summary)
        print("[SCHEDULER] Confirmations summary:", confirmations_summary)
        print("[SCHEDULER] Execution summary:", execution_summary)
        print("[SCHEDULER] Pipeline done")

    except Exception as exc:
        db.rollback()
        print(f"[SCHEDULER] Pipeline failed: {exc}")
        raise

    finally:
        db.close()


def main():
    print(f"[SCHEDULER] Starting loop every {INTERVAL_SECONDS} seconds")

    set_running(True)

    while is_running():
        try:
            run_once()
        except Exception:
            print("[SCHEDULER] Error handled. Continuing loop.")

        print(f"[SCHEDULER] Sleeping {INTERVAL_SECONDS} seconds")
        time.sleep(INTERVAL_SECONDS)

    print("[SCHEDULER] Stopped")

if __name__ == "__main__":
    main()
