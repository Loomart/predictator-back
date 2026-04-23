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
    print("[PIPELINE START]")

    db = SessionLocal()
    try:
        # Sync market data
        source = MockMarketSource()
        sync_market_data(db, source)
        print("[SYNC DONE]")

        # Run market scanner
        run_market_scanner(db)
        print("[SCAN DONE]")

        return 0

    except Exception as error:
        print(f"[ERROR] Pipeline failed: {error}", file=sys.stderr)
        return 1
    finally:
        db.close()
        print("[PIPELINE DONE]")


if __name__ == "__main__":
    sys.exit(main())
