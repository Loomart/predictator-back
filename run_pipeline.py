#!/usr/bin/env python
"""
Pipeline runner for ingestion and scanning.

Runs market synchronization first, then executes the scanner.
Can be run directly with: python run_pipeline.py
"""

import sys
from datetime import datetime

from db import SessionLocal
from ingest import MockMarketSource, sync_market_data
from scanner import run_market_scanner


def _print_section(title: str) -> None:
    separator = "=" * 60
    print(f"\n{separator}")
    print(f"{title}")
    print(f"{separator}\n")


def main() -> int:
    """Execute the ingestion pipeline.

    Returns:
        Exit code: 0 for success, 1 for failure.
    """
    _print_section("[SYNC START]")

    sync_db = SessionLocal()
    try:
        source = MockMarketSource()
        stats = sync_market_data(sync_db, source)
        print(
            f"[SYNC COMPLETE] Created={stats['created']} "
            f"Updated={stats['updated']} Snapshots={stats['snapshots']}\n"
        )
    except Exception as error:
        print(f"[ERROR] Sync failed: {error}", file=sys.stderr)
        return 1
    finally:
        sync_db.close()
        print("[SYNC] Database session closed\n")

    _print_section("[SCAN START]")

    scan_db = SessionLocal()
    try:
        run_market_scanner(scan_db)
    except Exception as error:
        print(f"[ERROR] Scan failed: {error}", file=sys.stderr)
        return 1
    finally:
        scan_db.close()
        print("[SCAN] Database session closed\n")

    _print_section("[DONE]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
