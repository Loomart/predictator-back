from sqlalchemy.orm import Session, joinedload

from backend.models import Fill, JobRun, Market, MarketSnapshot, Order, Position, Signal, SignalEvaluation


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


def get_orders(db: Session):
    return db.query(Order).order_by(Order.created_at.desc()).all()


def get_order_by_id(db: Session, order_id: int):
    return db.query(Order).filter(Order.id == order_id).first()


def get_fills(db: Session):
    return db.query(Fill).order_by(Fill.filled_at.desc()).all()


def get_positions(db: Session):
    return db.query(Position).order_by(Position.updated_at.desc()).all()


def get_position_by_market_id(db: Session, market_id: int):
    return db.query(Position).filter(Position.market_id == market_id).first()


def delete_market_cascade(db: Session, market_id: int) -> bool:
    market = db.query(Market).filter(Market.id == market_id).first()
    if market is None:
        return False

    order_ids = [row[0] for row in db.query(Order.id).filter(Order.market_id == market_id).all()]
    if order_ids:
        db.query(Fill).filter(Fill.order_id.in_(order_ids)).delete(synchronize_session=False)

    db.query(Order).filter(Order.market_id == market_id).delete(synchronize_session=False)
    db.query(Position).filter(Position.market_id == market_id).delete(synchronize_session=False)
    db.query(SignalEvaluation).filter(SignalEvaluation.market_id == market_id).delete(synchronize_session=False)
    db.query(Signal).filter(Signal.market_id == market_id).delete(synchronize_session=False)
    db.query(MarketSnapshot).filter(MarketSnapshot.market_id == market_id).delete(synchronize_session=False)
    db.delete(market)
    db.commit()
    return True
