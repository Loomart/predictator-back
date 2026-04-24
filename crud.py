from sqlalchemy.orm import Session, joinedload

from models import Market, MarketSnapshot, Signal, JobRun


def get_markets(db: Session):
    return db.query(Market).order_by(Market.created_at.desc()).all()


def get_market_by_id(db: Session, market_id: int):
    return (
        db.query(Market)
        .options(
            joinedload(Market.snapshots),
            joinedload(Market.signals),
        )
        .filter(Market.id == market_id)
        .first()
    )


def get_snapshots(db: Session):
    return db.query(MarketSnapshot).order_by(MarketSnapshot.captured_at.desc()).all()


def get_signals(db: Session):
    return db.query(Signal).order_by(Signal.created_at.desc()).all()


def get_pipeline_runs(db: Session):
    return db.query(JobRun).order_by(JobRun.created_at.desc()).all()


def get_pipeline_run_by_id(db: Session, run_id: int):
    return db.query(JobRun).filter(JobRun.id == run_id).first()