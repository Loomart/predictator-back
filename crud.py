from sqlalchemy.orm import Session, joinedload

from models import Market, MarketSnapshot, Signal


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