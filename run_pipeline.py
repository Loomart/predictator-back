#!/usr/bin/env python
"""
Pipeline runner for ingestion and scanning.

Runs market synchronization first, then executes the scanner.
Can be run directly with: python run_pipeline.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

from backend.db import SessionLocal
from backend.ingest import get_market_source, sync_market_data
from backend.models import JobRun
from backend.scanner import run_market_scanner
from backend.signal_confirmation_processor import process_all_signal_confirmations
from backend.execution_factory import get_execution_adapter
from backend.trading_orchestrator import execute_confirmed_signals


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
    pipeline_start = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    source_name = os.getenv("MARKET_SOURCE", "mock")
    
    print("[PIPELINE START]")

    db = SessionLocal()
    try:
        # Sync market data
        print("[SYNC START]")
        sync_start = time.perf_counter()
        source = get_market_source()
        sync_stats = sync_market_data(db, source)
        sync_duration = time.perf_counter() - sync_start
        print(f"[SYNC SUMMARY] Duration: {sync_duration:.2f}s | {sync_stats}")

        # Run market scanner
        print("[SCAN START]")
        scan_start = time.perf_counter()
        scan_stats = run_market_scanner(db)
        scan_duration = time.perf_counter() - scan_start
        print(f"[SCAN SUMMARY] Duration: {scan_duration:.2f}s | {scan_stats}")

        print("[CONFIRMATIONS START]")
        confirmations_start = time.perf_counter()
        confirmations_stats = process_all_signal_confirmations(db)
        confirmations_duration = time.perf_counter() - confirmations_start
        print(
            f"[CONFIRMATIONS SUMMARY] Duration: {confirmations_duration:.2f}s | {confirmations_stats}"
        )

        print("[EXECUTION START]")
        execution_start = time.perf_counter()
        execution_stats = execute_confirmed_signals(db, get_execution_adapter())
        execution_duration = time.perf_counter() - execution_start
        print(f"[EXECUTION SUMMARY] Duration: {execution_duration:.2f}s | {execution_stats}")

        pipeline_duration = time.perf_counter() - pipeline_start
        finished_at = datetime.now(timezone.utc)
        
        # Save successful job run
        summary = {
            "sync": sync_stats,
            "scan": scan_stats,
            "confirmations": confirmations_stats,
            "execution": execution_stats,
            "sync_duration": round(sync_duration, 2),
            "scan_duration": round(scan_duration, 2),
            "confirmations_duration": round(confirmations_duration, 2),
            "execution_duration": round(execution_duration, 2),
        }
        job_run = JobRun(
            job_type="pipeline",
            status="success",
            source_name=source_name,
            summary_json=json.dumps(summary),
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=round(pipeline_duration, 2),
        )
        db.add(job_run)
        db.commit()
        
        print(f"[PIPELINE DONE] Total duration: {pipeline_duration:.2f}s")
        return 0

    except Exception as error:
        pipeline_duration = time.perf_counter() - pipeline_start
        finished_at = datetime.now(timezone.utc)
        
        # Save failed job run
        summary = {}
        job_run = JobRun(
            job_type="pipeline",
            status="failed",
            source_name=source_name,
            summary_json=json.dumps(summary),
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=round(pipeline_duration, 2),
            error_message=str(error),
        )
        db.add(job_run)
        db.commit()
        
        print(f"[ERROR] Pipeline failed: {error}", file=sys.stderr)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
