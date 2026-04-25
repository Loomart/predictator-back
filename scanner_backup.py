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
    spread = previous.spread if previous.spread is not None else 0.05
    volume_24h = previous.volume_24h if previous.volume_24h is not None else 10000
    liquidity = previous.liquidity if previous.liquidity is not None else 10000

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
    
    


def _normalize_positive_score(value: float | None, min_value: float, max_value: float) -> float:
    if value is None:
        return 0.0
    if value <= min_value:
        return 0.0
    if value >= max_value:
        return 1.0
    return (value - min_value) / (max_value - min_value)


def _normalize_inverse_score(value: float | None, min_value: float, max_value: float) -> float:
    if value is None:
        return 0.0
    if value <= min_value:
        return 1.0
    if value >= max_value:
        return 0.0
    return 1.0 - (value - min_value) / (max_value - min_value)


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def calculate_market_score(snapshot_data: dict) -> dict[str, float | None]:
    """
    Score compuesto de microestructura.
    Devuelve scores parciales + score final entre 0 y 1.
    """

    spread = snapshot_data.get("spread") or 1.0
    liquidity = snapshot_data.get("liquidity") or 0.0
    volume_24h = snapshot_data.get("volume_24h") or 0.0
    best_bid = snapshot_data.get("best_bid")
    best_ask = snapshot_data.get("best_ask")

    # 1) Spread score
    # spread <= 0.01 excelente, spread >= 0.08 malo
    spread_score = clamp(1 - ((spread - 0.01) / (0.08 - 0.01)))

    # 2) Liquidity score
    # 100k+ excelente, menos de 5k pobre
    liquidity_score = clamp((liquidity - 5_000) / (100_000 - 5_000))

    # 3) Volume score
    # 200k+ excelente, menos de 10k pobre
    volume_score = clamp((volume_24h - 10_000) / (200_000 - 10_000))

    # 4) Orderbook score
    if best_bid is None or best_ask is None or best_ask <= best_bid:
        orderbook_score = 0.0
        real_spread = None
    else:
        real_spread = best_ask - best_bid
        orderbook_score = clamp(1 - ((real_spread - 0.005) / (0.06 - 0.005)))

    weights = {
        "spread_score": 0.35,
        "liquidity_score": 0.25,
        "volume_score": 0.25,
        "orderbook_score": 0.15,
    }

    market_score = (
        spread_score * weights["spread_score"]
        + liquidity_score * weights["liquidity_score"]
        + volume_score * weights["volume_score"]
        + orderbook_score * weights["orderbook_score"]
    )

    return {
        "market_score": round(market_score, 4),
        "spread_score": round(spread_score, 4),
        "liquidity_score": round(liquidity_score, 4),
        "volume_score": round(volume_score, 4),
        "orderbook_score": round(orderbook_score, 4),
        "real_spread": round(real_spread, 4) if real_spread is not None else None,
    }


def estimate_edge(snapshot_data: dict, score_data: dict[str, float | None]) -> float:
    """
    Estimación simple de edge.
    No es alpha real todavía; es proxy operacional.
    """

    spread = snapshot_data.get("spread") or 1.0
    liquidity = snapshot_data.get("liquidity") or 0.0
    volume_24h = snapshot_data.get("volume_24h") or 0.0
    market_score = float(score_data.get("market_score") or 0.0)

    spread_component = max(0.0, 0.04 - spread)
    liquidity_component = min(liquidity / 500_000, 0.15)
    volume_component = min(volume_24h / 1_000_000, 0.15)

    raw_edge = (
        spread_component
        + liquidity_component
        + volume_component
    ) * market_score

    return round(raw_edge, 4)


def evaluate_signal(snapshot_data: dict) -> tuple[str, float, float, str]:
    """
    Devuelve:
    - signal_type: ENTER / WATCH / SKIP
    - confidence: market_score
    - edge_estimate
    - reason
    """

    score_data = calculate_market_score(snapshot_data)
    market_score = score_data["market_score"]
    edge_estimate = estimate_edge(snapshot_data, score_data)

    if market_score >= 0.75:
        signal_type = "ENTER"
    elif market_score >= 0.55:
        signal_type = "WATCH"
    else:
        signal_type = "SKIP"

    reason = (
        f"{signal_type}: market_score={market_score}, "
        f"spread_score={score_data['spread_score']}, "
        f"liquidity_score={score_data['liquidity_score']}, "
        f"volume_score={score_data['volume_score']}, "
        f"orderbook_score={score_data['orderbook_score']}, "
        f"edge_estimate={edge_estimate}. "
        f"Raw: spread={snapshot_data.get('spread')}, "
        f"liquidity={snapshot_data.get('liquidity')}, "
        f"volume_24h={snapshot_data.get('volume_24h')}, "
        f"best_bid={snapshot_data.get('best_bid')}, "
        f"best_ask={snapshot_data.get('best_ask')}."
    )

    return signal_type, market_score, edge_estimate, reason

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


def is_signal_meaningfully_different(
    last_signal: Signal | None,
    signal_type: str,
    strategy_name: str,
    confidence: float,
    edge_estimate: float,
    confidence_threshold: float,
) -> bool:
    """Return True if the new signal differs enough from the last signal to warrant insertion."""
    if last_signal is None:
        return True

    if last_signal.signal_type != signal_type:
        return True

    if last_signal.strategy_name != strategy_name:
        return True

    if last_signal.confidence is None:
        return True

    if abs(last_signal.confidence - confidence) > confidence_threshold:
        return True

    if last_signal.edge_estimate is None:
        return True

    if abs(last_signal.edge_estimate - edge_estimate) > confidence_threshold:
        return True

    return False


def run_market_scanner(
    db: Session,
    snapshot_thresholds: SnapshotThresholds | None = None,
    signal_confidence_threshold: float = 0.0,
    strategy_name: str = "microstructure_v1",
) -> dict[str, int]:
    thresholds = snapshot_thresholds or DEFAULT_SNAPSHOT_THRESHOLDS
    stats = {
        "markets_scanned": 0,
        "signals_inserted": 0,
        "signals_skipped_duplicate": 0,
        "snapshots_created": 0,
        "snapshots_skipped_duplicate": 0,
    }
    markets = db.query(Market).filter(Market.status == "open").all()

    stats["markets_scanned"] = len(markets)

    if not markets:
        print("No hay mercados abiertos para escanear.")
        return stats

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
            stats["snapshots_skipped_duplicate"] += 1
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
        stats["snapshots_created"] += 1

        signal_type, confidence, edge_estimate, reason = evaluate_signal(new_snapshot_data)

        latest_signal = (
            db.query(Signal)
            .filter(Signal.market_id == market.id)
            .order_by(Signal.created_at.desc())
            .first()
        )

        if not is_signal_meaningfully_different(
            latest_signal,
            signal_type,
            strategy_name,
            confidence,
            edge_estimate,
            signal_confidence_threshold,
        ):
            print(
                f"[SKIP SIGNAL DUPLICATE] Market {market.id} - "
                f"{signal_type} / {strategy_name} / confidence={confidence}"
            )
            print(f"[Market {market.id}] Snapshot creado | Signal omitido | confidence={confidence}")
            stats["signals_skipped_duplicate"] += 1
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
                f"confidence={confidence} | edge={edge_estimate} | reason={reason}"
            )
            stats["signals_inserted"] += 1

    db.commit()
    print(
        f"\n[SCAN COMPLETE] Scanned: {stats['markets_scanned']}, "
        f"Snapshots created: {stats['snapshots_created']}, "
        f"Snapshots skipped: {stats['snapshots_skipped_duplicate']}, "
        f"Signals inserted: {stats['signals_inserted']}, "
        f"Signals skipped: {stats['signals_skipped_duplicate']}\n"
    )
    return stats
