from sqlalchemy import func, case

from backend.db import SessionLocal
from backend.models import Signal, SignalEvaluation


def pct(value: float) -> str:
    return f"{value * 100:.2f}%"


def run_report() -> None:
    db = SessionLocal()

    rows = (
        db.query(
            Signal.signal_type,
            func.count(SignalEvaluation.id).label("total"),
            func.sum(
                case((SignalEvaluation.is_success == True, 1), else_=0)
            ).label("wins"),
            func.sum(
                case((SignalEvaluation.is_success == False, 1), else_=0)
            ).label("losses"),
            func.avg(SignalEvaluation.price_change).label("avg_change"),
        )
        .join(Signal, Signal.id == SignalEvaluation.signal_id)
        .filter(Signal.strategy_name == "alpha_scoring_v2")
        .group_by(Signal.signal_type)
        .order_by(Signal.signal_type.asc())
        .all()
    )

    print("\nSIGNAL EVALUATION REPORT")
    print("=" * 80)

    for row in rows:
        total = int(row.total or 0)
        wins = int(row.wins or 0)
        losses = int(row.losses or 0)
        flats = total - wins - losses
        decided = wins + losses
        win_rate = wins / decided if decided > 0 else 0.0
        avg_change = float(row.avg_change or 0.0)

        print(
            f"{row.signal_type:16} "
            f"total={total:4d} "
            f"wins={wins:4d} "
            f"losses={losses:4d} "
            f"flat={flats:4d} "
            f"win_rate={pct(win_rate):>8} "
            f"avg_change={avg_change:+.4f}"
        )

    db.close()


if __name__ == "__main__":
    run_report()