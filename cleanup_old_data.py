from datetime import datetime, timedelta, timezone

from db import SessionLocal
from models import MarketSnapshot, Signal

try:
    from models import PipelineRun
except ImportError:
    PipelineRun = None

RETENTION_DAYS = 1


def cleanup_old_data(dry_run: bool = True):
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)

    db = SessionLocal()

    try:
        snapshots_query = db.query(MarketSnapshot).filter(
            MarketSnapshot.created_at < cutoff
        )

        signals_query = db.query(Signal).filter(
            Signal.created_at < cutoff
        )

        snapshots_count = snapshots_query.count()
        signals_count = signals_query.count()

        pipeline_runs_count = 0
        pipeline_runs_query = None

        if PipelineRun is not None:
            pipeline_runs_query = db.query(PipelineRun).filter(
                PipelineRun.created_at < cutoff
            )
            pipeline_runs_count = pipeline_runs_query.count()

        print("=" * 60)
        print("Cleanup old data")
        print(f"Retention days: {RETENTION_DAYS}")
        print(f"Cutoff: {cutoff}")
        print(f"Dry run: {dry_run}")
        print("=" * 60)
        print(f"Old snapshots: {snapshots_count}")
        print(f"Old signals: {signals_count}")
        print(f"Old pipeline runs: {pipeline_runs_count}")

        if dry_run:
            print("[DRY RUN] No data deleted.")
            return {
                "dry_run": True,
                "snapshots_to_delete": snapshots_count,
                "signals_to_delete": signals_count,
                "pipeline_runs_to_delete": pipeline_runs_count,
            }

        snapshots_query.delete(synchronize_session=False)
        signals_query.delete(synchronize_session=False)

        if pipeline_runs_query is not None:
            pipeline_runs_query.delete(synchronize_session=False)

        db.commit()

        print("[DONE] Old data deleted.")

        return {
            "dry_run": False,
            "snapshots_deleted": snapshots_count,
            "signals_deleted": signals_count,
            "pipeline_runs_deleted": pipeline_runs_count,
        }

    except Exception:
        db.rollback()
        raise

    finally:
        db.close()


if __name__ == "__main__":
    # Seguridad: por defecto solo simula.
    cleanup_old_data(dry_run=False)