from datetime import datetime, timedelta

from sqlalchemy.orm import Session

from backend.db import SessionLocal
from backend.models import Signal, MarketSnapshot, SignalEvaluation


EVALUATION_HORIZON_MINUTES = 60
MIN_MOVE = 0.01  # movimiento mínimo para considerar éxito


def get_entry_snapshot(db: Session, signal: Signal) -> MarketSnapshot | None:
    return (
        db.query(MarketSnapshot)
        .filter(MarketSnapshot.market_id == signal.market_id)
        .filter(MarketSnapshot.captured_at >= signal.created_at)
        .order_by(MarketSnapshot.captured_at.asc())
        .first()
    )


def get_future_snapshots(db: Session, signal: Signal):
    end_time = signal.created_at + timedelta(minutes=EVALUATION_HORIZON_MINUTES)

    return (
        db.query(MarketSnapshot)
        .filter(MarketSnapshot.market_id == signal.market_id)
        .filter(MarketSnapshot.captured_at >= signal.created_at)
        .filter(MarketSnapshot.captured_at <= end_time)
        .order_by(MarketSnapshot.captured_at.asc())
        .all()
    )


def already_evaluated(db: Session, signal: Signal) -> bool:
    existing = (
        db.query(SignalEvaluation)
        .filter(SignalEvaluation.signal_id == signal.id)
        .filter(SignalEvaluation.evaluation_horizon_minutes == EVALUATION_HORIZON_MINUTES)
        .first()
    )
    return existing is not None


def evaluate_signal(db: Session, signal: Signal) -> None:
    if already_evaluated(db, signal):
        return

    entry_snapshot = get_entry_snapshot(db, signal)
    future_snapshots = get_future_snapshots(db, signal)

    if not entry_snapshot or not future_snapshots:
        return

    entry_price = entry_snapshot.yes_price

    prices = [s.yes_price for s in future_snapshots if s.yes_price is not None]

    if not prices or entry_price is None:
        return

    max_price = max(prices)
    min_price = min(prices)

    best_move = max_price - entry_price
    worst_move = min_price - entry_price

    # Clasificación
    if best_move >= MIN_MOVE:
        is_success = True
        exit_price = max_price
        price_change = best_move
    elif worst_move <= -MIN_MOVE:
        is_success = False
        exit_price = min_price
        price_change = worst_move
    else:
        is_success = None
        exit_price = prices[-1]
        price_change = exit_price - entry_price

    evaluation = SignalEvaluation(
        signal_id=signal.id,
        market_id=signal.market_id,
        evaluation_horizon_minutes=EVALUATION_HORIZON_MINUTES,
        entry_price=entry_price,
        exit_price=exit_price,
        price_change=price_change,
        direction="up",
        is_success=is_success,
        evaluated_at=datetime.utcnow(),
    )

    db.add(evaluation)

    print(
        f"[EVAL] Signal {signal.id} | "
        f"entry={entry_price:.4f} | best={max_price:.4f} | worst={min_price:.4f} | "
        f"move={price_change:+.4f} | success={is_success}"
    )


def run_evaluation():
    db = SessionLocal()

    signals = (
        db.query(Signal)
        .filter(Signal.strategy_name == "alpha_scoring_v2")
        .filter(Signal.signal_type.in_(["ENTER", "STRONG_ENTER", "WATCH"]))
        .all()
    )

    processed = 0

    for signal in signals:
        evaluate_signal(db, signal)
        processed += 1

    db.commit()

    print(f"\n[EVALUATION COMPLETE] processed={processed}")


if __name__ == "__main__":
    run_evaluation()