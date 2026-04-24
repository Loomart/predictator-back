import time
from datetime import datetime

from db import SessionLocal
from ingest.mock_source import MockMarketSource
from ingest.sync_markets import sync_market_data
from scanner import run_market_scanner
from scheduler_state import set_running, is_running


INTERVAL_SECONDS = 5  # 5 minutos


def run_once():
    db = SessionLocal()

    try:
        print(f"[SCHEDULER] Pipeline start at {datetime.utcnow().isoformat()} UTC")

        source = MockMarketSource()

        sync_summary = sync_market_data(db, source)
        scan_summary = run_market_scanner(db)

        print("[SCHEDULER] Sync summary:", sync_summary)
        print("[SCHEDULER] Scan summary:", scan_summary)
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