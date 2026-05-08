from __future__ import annotations

import argparse

from confirmation_analysis import export_signal_confirmation_analysis_csv
from db import SessionLocal


def main() -> int:
    parser = argparse.ArgumentParser(description="Export signal confirmation outcomes to CSV")
    parser.add_argument(
        "--output",
        default="backend/data/signal_confirmation_analysis.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--horizon-snapshots",
        type=int,
        default=20,
        help="Number of snapshots after decision to evaluate",
    )
    args = parser.parse_args()

    db = SessionLocal()
    try:
        result = export_signal_confirmation_analysis_csv(
            session=db,
            output_path=args.output,
            horizon_snapshots=args.horizon_snapshots,
        )
        print(result)
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
