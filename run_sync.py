#!/usr/bin/env python
"""
Market data synchronization runner.

Executes market data ingestion from source to database.
Can be run directly with: python run_sync.py
"""

import sys
from datetime import datetime

from db import SessionLocal
from ingest import get_market_source, sync_market_data


def main() -> int:
    """Run market data synchronization.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    db = SessionLocal()

    try:
        print(f"\n{'='*60}")
        print(f"Market Data Synchronization - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*60}\n")

        # Create source based on environment variable
        print("[INIT] Creating market source...")
        source = get_market_source()

        # Run sync
        print("[SYNC] Starting synchronization...\n")
        stats = sync_market_data(db, source)

        # Print results
        print(f"{'='*60}")
        print("Synchronization Results:")
        print(f"  ✓ Total markets received: {stats['total_markets_received']}")
        print(f"  ✓ Created markets: {stats['markets_created']}")
        print(f"  ✓ Updated markets: {stats['markets_updated']}")
        print(f"  ✓ Snapshots inserted: {stats['snapshots_inserted']}")
        print(f"  ✓ Snapshots skipped (duplicate): {stats['snapshots_skipped_duplicate']}")
        print(f"{'='*60}\n")

        return 0

    except Exception as e:
        print(f"\n[ERROR] Synchronization failed: {e}\n", file=sys.stderr)
        return 1

    finally:
        db.close()
        print("[DONE] Database session closed\n")


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
