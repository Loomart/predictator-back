from datetime import datetime
import random

from sqlalchemy.orm import Session

from models import Market, MarketSnapshot, Signal

SnapshotThresholds = dict[str, float]

DEFAULT_SNAPSHOT_THRESHOLDS: SnapshotThresholds = {
    "yes_price": 0.005,
    "no_price": 0.005,
    "spread": 0.003,
    "volume_24h": 500.0,
    "liquidity": 500.0,
    "best_bid": 0.005,
    "best_ask": 0.005,
}


def generate_next_snapshot(previous: MarketSnapshot) -> dict:
    """
    Genera un nuevo snapshot a partir del anterior con pequeñas variaciones.
    Esto es una simulación inicial para validar el pipeline.
    """

    yes_price = previous.yes_price if previous.yes_price is not None else 0.5
    no_price = previous.no_price if previous.no_price is not None else 0.5
    spread = previous.spread if previous.spread is not None else 0.05
    volume_24h = previous.volume_24h if previous.volume_24h is not None else 10000
    liquidity = previous.liquidity if previous.liquidity is not None else 10000
    best_bid = previous.best_bid if previous.best_bid is not None else 0.49
    best_ask = previous.best_ask if previous.best_ask is not None else 0.51

    # Variaciones pequeñas simuladas
    yes_price = max(0.01, min(0.99, yes_price + random.uniform(-0.03, 0.03)))
    no_price = max(0.01, min(0.99, 1 - yes_price))
    spread = max(0.005, min(0.08, spread + random.uniform(-0.005, 0.005)))
    volume_24h = max(1000, volume_24h + random.uniform(-5000, 5000))
    liquidity = max(1000, liquidity + random.uniform(-3000, 3000))
    best_bid = max(0.01, min(0.98, yes_price - spread / 2))
    best_ask = max(best_bid + 0.001, min(0.99, yes_price + spread / 2))

    return {
        "yes_price": round(yes_price, 4),
        "no_price": round(no_price, 4),
        "spread": round(spread, 4),
        "volume_24h": round(volume_24h, 2),
        "liquidity": round(liquidity, 2),
        "best_bid": round(best_bid, 4),
        "best_ask": round(best_ask, 4),
    }


def evaluate_signal(snapshot_data: dict) -> tuple[str, float, float, str]:
    """
    Regla V1:
    ENTER si:
    - spread <= 0.03
    - liquidity >= 10000
    - volume_24h >= 50000
    En caso contrario: SKIP
    """

    spread = snapshot_data["spread"]
    liquidity = snapshot_data["liquidity"]
    volume_24h = snapshot_data["volume_24h"]

    conditions_met = 0

    if spread <= 0.03:
        conditions_met += 1
    if liquidity >= 10000:
        conditions_met += 1
    if volume_24h >= 50000:
        conditions_met += 1

    confidence = round(conditions_met / 3, 2)

    edge_estimate = round(
        max(0.0, (0.03 - spread)) +
        max(0.0, (liquidity - 10000) / 100000) +
        max(0.0, (volume_24h - 50000) / 500000),
        4
    )

    if conditions_met == 3:
        signal_type = "ENTER"
        reason = (
            f"ENTER: spread={spread}, liquidity={liquidity}, volume_24h={volume_24h}. "
            "Todas las condiciones mínimas se cumplen."
        )
    else:
        signal_type = "SKIP"
        reason = (
            f"SKIP: spread={spread}, liquidity={liquidity}, volume_24h={volume_24h}. "
            "No se cumplen todas las condiciones mínimas."
        )

    return signal_type, confidence, edge_estimate, reason


def _is_snapshot_significant(
    last_snapshot: MarketSnapshot,
    new_snapshot_data: dict,
    thresholds: SnapshotThresholds,
) -> bool:
    """Return True when the new snapshot differs enough from the last one."""
    for field_name, threshold in thresholds.items():
        last_value = getattr(last_snapshot, field_name)
        new_value = new_snapshot_data[field_name]

        if last_value is None or new_value is None:
            if last_value != new_value:
                return True
            continue

        if abs(new_value - last_value) > threshold:
            return True

    return False


def _is_duplicate_signal(
    last_signal: Signal | None,
    signal_type: str,
    strategy_name: str,
    confidence: float,
    confidence_threshold: float,
) -> bool:
    """Return True when the new signal is equivalent to the latest one."""
    if last_signal is None:
        return False

    if last_signal.signal_type != signal_type:
        return False

    if last_signal.strategy_name != strategy_name:
        return False

    if last_signal.confidence is None:
        return False

    return abs(last_signal.confidence - confidence) <= confidence_threshold


def run_market_scanner(
    db: Session,
    snapshot_thresholds: SnapshotThresholds | None = None,
    signal_confidence_threshold: float = 0.0,
    strategy_name: str = "microstructure_v1",
):
    thresholds = snapshot_thresholds or DEFAULT_SNAPSHOT_THRESHOLDS
    markets = db.query(Market).filter(Market.status == "open").all()

    if not markets:
        print("No hay mercados abiertos para escanear.")
        return

    for market in markets:
        latest_snapshot = (
            db.query(MarketSnapshot)
            .filter(MarketSnapshot.market_id == market.id)
            .order_by(MarketSnapshot.captured_at.desc())
            .first()
        )

        if not latest_snapshot:
            print(f"Mercado {market.id} sin snapshot previo. Se omite.")
            continue

        new_snapshot_data = generate_next_snapshot(latest_snapshot)
        if not _is_snapshot_significant(latest_snapshot, new_snapshot_data, thresholds):
            print(
                f"[SKIP SNAPSHOT DUPLICATE] Market {market.id} - "
                "cambio mínimo, no se registra nuevo snapshot"
            )
            continue

        new_snapshot = MarketSnapshot(
            market_id=market.id,
            yes_price=new_snapshot_data["yes_price"],
            no_price=new_snapshot_data["no_price"],
            spread=new_snapshot_data["spread"],
            volume_24h=new_snapshot_data["volume_24h"],
            liquidity=new_snapshot_data["liquidity"],
            best_bid=new_snapshot_data["best_bid"],
            best_ask=new_snapshot_data["best_ask"],
            captured_at=datetime.utcnow(),
        )
        db.add(new_snapshot)
        db.flush()

        signal_type, confidence, edge_estimate, reason = evaluate_signal(new_snapshot_data)

        latest_signal = (
            db.query(Signal)
            .filter(Signal.market_id == market.id)
            .order_by(Signal.created_at.desc())
            .first()
        )

        if _is_duplicate_signal(
            latest_signal,
            signal_type,
            strategy_name,
            confidence,
            signal_confidence_threshold,
        ):
            print(
                f"[SKIP SIGNAL DUPLICATE] Market {market.id} - "
                f"{signal_type} / {strategy_name} / confidence={confidence}"
            )
            print(f"[Market {market.id}] Snapshot creado | Signal omitido | confidence={confidence}")
        else:
            new_signal = Signal(
                market_id=market.id,
                signal_type=signal_type,
                strategy_name="microstructure_v1",
                confidence=confidence,
                edge_estimate=edge_estimate,
                reason=reason,
                is_executed=False,
                created_at=datetime.utcnow(),
            )
            db.add(new_signal)
            print(
                f"[Market {market.id}] Snapshot creado | Signal={signal_type} | "
                f"confidence={confidence} | edge={edge_estimate}"
            )

    db.commit()
    print("Scanner completado.")