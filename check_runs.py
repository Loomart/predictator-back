#!/usr/bin/env python
"""
Check job runs in the database.

Usage: python check_runs.py [--limit N] [--status STATUS]
"""

import argparse
import json
from db import SessionLocal
from models import JobRun


def main():
    parser = argparse.ArgumentParser(description="Check job runs")
    parser.add_argument("--limit", type=int, default=10, help="Number of recent runs to show")
    parser.add_argument("--status", choices=["success", "failed"], help="Filter by status")
    args = parser.parse_args()

    db = SessionLocal()
    try:
        query = db.query(JobRun).order_by(JobRun.id.desc())
        if args.status:
            query = query.filter(JobRun.status == args.status)
        runs = query.limit(args.limit).all()

        print(f"{'='*80}")
        print(f"Recent Job Runs (limit: {args.limit})")
        print(f"{'='*80}")

        for run in runs:
            print(f"ID: {run.id}")
            print(f"  Type: {run.job_type}")
            print(f"  Status: {run.status}")
            print(f"  Source: {run.source_name or 'N/A'}")
            print(f"  Duration: {run.duration_seconds:.2f}s")
            print(f"  Started: {run.started_at}")
            print(f"  Finished: {run.finished_at}")
            if run.error_message:
                print(f"  Error: {run.error_message}")
            if run.summary_json:
                summary = json.loads(run.summary_json)
                print(f"  Summary: {summary}")
            print("-" * 40)

        print(f"Total runs shown: {len(runs)}")

    finally:
        db.close()


if __name__ == "__main__":
    main()