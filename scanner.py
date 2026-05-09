from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from statistics import mean, pstdev
from typing import Any

from sqlalchemy.orm import Session

from backend.market_filters import (
    category_matches,
    external_id_matches,
    normalize_set,
    parse_csv_values,
    title_matches,
)
from backend.runtime_switches import load_runtime_switches
from backend.models import Market, MarketSnapshot, Signal


DEFAULT_WINDOW_SIZE = 10
DEFAULT_MIN_HISTORY = 3
DEFAULT_STRATEGY_NAME = "alpha_scoring_v2"
DEFAULT_MARKET_LIMIT = 25


def clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def safe_float(value: Any, default: float | None = None) -> float | None:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_positive(value: float | None, min_value: float, max_value: float) -> float:
    if value is None:
        return 0.0
    if value <= min_value:
        return 0.0
    if value >= max_value:
        return 1.0
    return (value - min_value) / (max_value - min_value)


def normalize_inverse(value: float | None, min_value: float, max_value: float) -> float:
    if value is None:
        return 0.0
    if value <= min_value:
        return 1.0
    if value >= max_value:
        return 0.0
    return 1.0 - ((value - min_value) / (max_value - min_value))


def snapshot_to_dict(snapshot: MarketSnapshot) -> dict[str, Any]:
    return {
        "id": snapshot.id,
        "market_id": snapshot.market_id,
        "yes_price": safe_float(snapshot.yes_price),
        "no_price": safe_float(snapshot.no_price),
        "spread": safe_float(snapshot.spread),
        "volume_24h": safe_float(snapshot.volume_24h),
        "liquidity": safe_float(snapshot.liquidity),
        "best_bid": safe_float(snapshot.best_bid),
        "best_ask": safe_float(snapshot.best_ask),
        "captured_at": snapshot.captured_at.isoformat() if snapshot.captured_at else None,
    }


def get_recent_snapshots(db: Session, market_id: int, limit: int = DEFAULT_WINDOW_SIZE) -> list[MarketSnapshot]:
    snapshots = (
        db.query(MarketSnapshot)
        .filter(MarketSnapshot.market_id == market_id)
        .order_by(MarketSnapshot.captured_at.desc())
        .limit(limit)
        .all()
    )
    return list(reversed(snapshots))


def calculate_microstructure_score(latest: dict[str, Any]) -> dict[str, float | None]:
    spread = safe_float(latest.get("spread"))
    liquidity = safe_float(latest.get("liquidity"), 0.0) or 0.0
    volume_24h = safe_float(latest.get("volume_24h"), 0.0) or 0.0
    best_bid = safe_float(latest.get("best_bid"))
    best_ask = safe_float(latest.get("best_ask"))

    spread_score = normalize_inverse(spread, 0.01, 0.08)
    liquidity_score = normalize_positive(liquidity, 5_000.0, 100_000.0)
    volume_score = normalize_positive(volume_24h, 10_000.0, 200_000.0)

    real_spread = None
    orderbook_score = 0.0
    if best_bid is not None and best_ask is not None and best_ask > best_bid:
        real_spread = best_ask - best_bid
        orderbook_score = normalize_inverse(real_spread, 0.005, 0.06)

    score = (
        spread_score * 0.35
        + liquidity_score * 0.25
        + volume_score * 0.25
        + orderbook_score * 0.15
    )

    return {
        "score": round(clamp(score), 4),
        "spread_score": round(spread_score, 4),
        "liquidity_score": round(liquidity_score, 4),
        "volume_score": round(volume_score, 4),
        "orderbook_score": round(orderbook_score, 4),
        "real_spread": round(real_spread, 4) if real_spread is not None else None,
    }


def calculate_momentum_score(prices: list[float]) -> dict[str, float | str | None]:
    if len(prices) < 3:
        return {"score": 0.0, "direction": "unknown", "change": None, "slope": None}

    first = prices[0]
    last = prices[-1]
    change = last - first

    step_changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    avg_step = mean(step_changes)
    positive_steps = sum(1 for change_i in step_changes if change_i > 0)
    negative_steps = sum(1 for change_i in step_changes if change_i < 0)

    direction = "flat"
    consistency = 0.0
    if change > 0.005:
        direction = "up"
        consistency = positive_steps / len(step_changes)
    elif change < -0.005:
        direction = "down"
        consistency = negative_steps / len(step_changes)

    magnitude_score = normalize_positive(abs(change), 0.005, 0.08)
    consistency_score = clamp((consistency - 0.45) / 0.45)
    score = clamp((magnitude_score * 0.65) + (consistency_score * 0.35))

    return {
        "score": round(score, 4),
        "direction": direction,
        "change": round(change, 4),
        "slope": round(avg_step, 4),
        "consistency": round(consistency, 4),
    }


def calculate_stability_score(prices: list[float]) -> dict[str, float | int | None]:
    if len(prices) < 3:
        return {"score": 0.0, "volatility": None, "price_range": None, "jump_count": 0}

    volatility = pstdev(prices)
    price_range = max(prices) - min(prices)
    step_changes = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    jump_count = sum(1 for change in step_changes if change >= 0.06)

    volatility_score = normalize_inverse(volatility, 0.01, 0.08)
    range_score = normalize_inverse(price_range, 0.03, 0.18)
    jump_score = normalize_inverse(float(jump_count), 0.0, 3.0)

    score = clamp((volatility_score * 0.45) + (range_score * 0.35) + (jump_score * 0.20))

    return {
        "score": round(score, 4),
        "volatility": round(volatility, 4),
        "price_range": round(price_range, 4),
        "jump_count": jump_count,
    }


def calculate_liquidity_consistency_score(liquidities: list[float]) -> dict[str, float | None]:
    if len(liquidities) < 3:
        return {"score": 0.0, "avg_liquidity": None, "min_liquidity": None, "liquidity_cv": None}

    avg_liquidity = mean(liquidities)
    min_liquidity = min(liquidities)
    liquidity_std = pstdev(liquidities)
    liquidity_cv = liquidity_std / avg_liquidity if avg_liquidity > 0 else 999.0

    avg_score = normalize_positive(avg_liquidity, 5_000.0, 100_000.0)
    floor_score = normalize_positive(min_liquidity, 1_000.0, 50_000.0)
    consistency_score = normalize_inverse(liquidity_cv, 0.05, 0.80)

    score = clamp((avg_score * 0.40) + (floor_score * 0.35) + (consistency_score * 0.25))

    return {
        "score": round(score, 4),
        "avg_liquidity": round(avg_liquidity, 2),
        "min_liquidity": round(min_liquidity, 2),
        "liquidity_cv": round(liquidity_cv, 4),
    }


def calculate_noise_penalty(
    prices: list[float],
    spreads: list[float],
    volumes: list[float],
    liquidities: list[float],
) -> dict[str, float | int]:
    price_spikes = 0
    spread_spikes = 0
    volume_spikes = 0
    liquidity_drop_spikes = 0

    for i in range(1, len(prices)):
        if abs(prices[i] - prices[i - 1]) >= 0.08:
            price_spikes += 1

    for spread in spreads:
        if spread >= 0.08:
            spread_spikes += 1

    if len(volumes) >= 3:
        avg_volume = mean(volumes)
        if avg_volume > 0:
            volume_spikes = sum(1 for volume in volumes if volume > avg_volume * 3.0)

    if len(liquidities) >= 3:
        avg_liquidity = mean(liquidities)
        if avg_liquidity > 0:
            liquidity_drop_spikes = sum(1 for liquidity in liquidities if liquidity < avg_liquidity * 0.25)

    raw_penalty = (
        price_spikes * 0.10
        + spread_spikes * 0.06
        + volume_spikes * 0.04
        + liquidity_drop_spikes * 0.07
    )

    return {
        "penalty": round(clamp(raw_penalty, 0.0, 0.35), 4),
        "price_spikes": price_spikes,
        "spread_spikes": spread_spikes,
        "volume_spikes": volume_spikes,
        "liquidity_drop_spikes": liquidity_drop_spikes,
    }


def estimate_edge_v2(score: float, momentum: dict[str, Any], microstructure: dict[str, Any], noise: dict[str, Any]) -> float:
    direction_bonus = 1.0 if momentum.get("direction") in {"up", "down"} else 0.5
    spread_quality = safe_float(microstructure.get("spread_score"), 0.0) or 0.0
    noise_penalty = safe_float(noise.get("penalty"), 0.0) or 0.0

    edge = score * (0.03 + spread_quality * 0.03) * direction_bonus
    edge = max(0.0, edge - noise_penalty * 0.03)
    return round(edge, 4)


def calculate_market_score_v2(snapshots: list[MarketSnapshot]) -> dict[str, Any]:
    rows = [snapshot_to_dict(snapshot) for snapshot in snapshots]
    latest = rows[-1]

    prices = [row["yes_price"] for row in rows if row.get("yes_price") is not None]
    spreads = [row["spread"] for row in rows if row.get("spread") is not None]
    volumes = [row["volume_24h"] for row in rows if row.get("volume_24h") is not None]
    liquidities = [row["liquidity"] for row in rows if row.get("liquidity") is not None]

    microstructure = calculate_microstructure_score(latest)
    momentum = calculate_momentum_score(prices)
    stability = calculate_stability_score(prices)
    liquidity = calculate_liquidity_consistency_score(liquidities)
    noise = calculate_noise_penalty(prices, spreads, volumes, liquidities)

    volume_score = normalize_positive(latest.get("volume_24h"), 10_000.0, 200_000.0)
    momentum_score = safe_float(momentum.get("score"), 0.0) or 0.0
    liquidity_score = safe_float(liquidity.get("score"), 0.0) or 0.0
    stability_score = safe_float(stability.get("score"), 0.0) or 0.0
    microstructure_score = safe_float(microstructure.get("score"), 0.0) or 0.0
    noise_penalty = safe_float(noise.get("penalty"), 0.0) or 0.0

    raw_score = (
        momentum_score * 0.30
        + liquidity_score * 0.25
        + stability_score * 0.20
        + microstructure_score * 0.15
        + volume_score * 0.10
    )
    final_score = clamp(raw_score - noise_penalty)

    return {
        "score": round(final_score, 4),
        "raw_score": round(raw_score, 4),
        "components": {
            "momentum": momentum,
            "liquidity_consistency": liquidity,
            "stability": stability,
            "microstructure": microstructure,
            "volume": {"score": round(volume_score, 4)},
            "noise": noise,
        },
        "latest_snapshot": latest,
        "history_size": len(snapshots),
    }


def classify_signal(score_data: dict[str, Any], thresholds: dict[str, float] | None = None) -> tuple[str, str]:
    cfg = thresholds or {}
    score = score_data["score"]
    components = score_data["components"]

    momentum_score = components["momentum"]["score"]
    liquidity_score = components["liquidity_consistency"]["score"]
    stability_score = components["stability"]["score"]
    noise_penalty = components["noise"]["penalty"]
    direction = components["momentum"].get("direction")

    wait_liquidity_threshold = float(cfg.get("wait_liquidity_threshold", 0.20))
    wait_noise_threshold = float(cfg.get("wait_noise_threshold", 0.18))
    wait_stability_threshold = float(cfg.get("wait_stability_threshold", 0.25))
    strong_enter_score_threshold = float(cfg.get("strong_enter_score_threshold", 0.75))
    strong_enter_momentum_threshold = float(cfg.get("strong_enter_momentum_threshold", 0.55))
    strong_enter_change_threshold = float(cfg.get("strong_enter_change_threshold", 0.02))
    enter_score_threshold = float(cfg.get("enter_score_threshold", 0.60))
    enter_momentum_threshold = float(cfg.get("enter_momentum_threshold", 0.45))
    enter_change_threshold = float(cfg.get("enter_change_threshold", 0.015))
    watch_score_threshold = float(cfg.get("watch_score_threshold", 0.55))
    watch_momentum_threshold = float(cfg.get("watch_momentum_threshold", 0.30))
    avoid_score_threshold = float(cfg.get("avoid_score_threshold", 0.45))

    if liquidity_score < wait_liquidity_threshold:
        return "WAIT_LIQUIDITY", "liquidity_too_weak_or_inconsistent"

    if noise_penalty >= wait_noise_threshold or stability_score < wait_stability_threshold:
        return "WAIT_STABILITY", "market_too_noisy_or_unstable"

    if (
        score >= strong_enter_score_threshold
        and momentum_score >= strong_enter_momentum_threshold
        and direction in {"up", "down"}
        and abs(components["momentum"]["change"]) >= strong_enter_change_threshold
    ):
        return "STRONG_ENTER", "strong_directional_momentum_with_clean_market_conditions"

    if (
        score >= enter_score_threshold
        and momentum_score >= enter_momentum_threshold
        and abs(components["momentum"]["change"]) >= enter_change_threshold
    ):
        return "ENTER", "directional_momentum_with_acceptable_market_conditions"

    if score >= watch_score_threshold and momentum_score >= watch_momentum_threshold and direction in {"up", "down"}:
        return "WATCH", "partial_setup_needs_confirmation"

    if score >= avoid_score_threshold:
        return "AVOID", "high_quality_market_but_no_directional_setup"

    return "AVOID", "weak_setup"


def evaluate_signal_v2(
    snapshots: list[MarketSnapshot],
    *,
    min_history: int = DEFAULT_MIN_HISTORY,
    thresholds: dict[str, float] | None = None,
) -> tuple[str, float, float, str]:
    if len(snapshots) < min_history:
        latest = snapshots[-1] if snapshots else None
        reason_payload = {
            "strategy": DEFAULT_STRATEGY_NAME,
            "reason_code": "insufficient_history",
            "history_size": len(snapshots),
            "min_history": min_history,
            "latest_snapshot_id": latest.id if latest else None,
        }
        return "WATCH", 0.0, 0.0, json.dumps(reason_payload, default=str)

    score_data = calculate_market_score_v2(snapshots)
    signal_type, reason_code = classify_signal(score_data, thresholds)
    edge_estimate = estimate_edge_v2(
        score_data["score"],
        score_data["components"]["momentum"],
        score_data["components"]["microstructure"],
        score_data["components"]["noise"],
    )

    reason_payload = {
        "strategy": DEFAULT_STRATEGY_NAME,
        "reason_code": reason_code,
        "score": score_data["score"],
        "raw_score": score_data["raw_score"],
        "edge_estimate": edge_estimate,
        "history_size": score_data["history_size"],
        "components": score_data["components"],
        "latest_snapshot": score_data["latest_snapshot"],
    }

    return signal_type, score_data["score"], edge_estimate, json.dumps(reason_payload, default=str)


def is_signal_meaningfully_different(
    last_signal: Signal | None,
    signal_type: str,
    strategy_name: str,
    confidence: float,
    edge_estimate: float,
    confidence_threshold: float,
) -> bool:
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
    snapshot_thresholds: dict[str, float] | None = None,
    signal_confidence_threshold: float = 0.02,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    window_size: int = DEFAULT_WINDOW_SIZE,
    market_limit: int | None = DEFAULT_MARKET_LIMIT,
    confirmation_window_minutes: int = 10,
    category_allowlist: set[str] | None = None,
    title_terms: set[str] | None = None,
    external_id_allowlist: set[str] | None = None,
    min_history: int | None = None,
    classifier_thresholds_override: dict[str, float] | None = None,
) -> dict[str, int]:
    """
    Scanner V2.

    Importante:
    - No genera snapshots sintéticos.
    - Lee los últimos N snapshots reales creados por sync_market_data.
    - Inserta señales alpha_scoring_v2 solo cuando cambian de forma significativa.
    """
    stats = {
        "markets_scanned": 0,
        "markets_filtered_out": 0,
        "markets_without_snapshots": 0,
        "markets_with_insufficient_history": 0,
        "signals_inserted": 0,
        "signals_skipped_duplicate": 0,
        "snapshots_created": 0,
        "snapshots_skipped_duplicate": 0,
        "signals_skipped_avoid": 0,
    }

    runtime_switches = load_runtime_switches()
    scanner_switches = runtime_switches["scanner"]
    filter_switches = runtime_switches["filters"]

    effective_market_limit = market_limit
    if effective_market_limit is None:
        effective_market_limit = int(scanner_switches["market_limit"])

    effective_min_history = int(min_history) if min_history is not None else int(scanner_switches["min_history"])
    classifier_thresholds = {
        "wait_liquidity_threshold": float(scanner_switches["wait_liquidity_threshold"]),
        "wait_noise_threshold": float(scanner_switches["wait_noise_threshold"]),
        "wait_stability_threshold": float(scanner_switches["wait_stability_threshold"]),
        "strong_enter_score_threshold": float(scanner_switches["strong_enter_score_threshold"]),
        "strong_enter_momentum_threshold": float(scanner_switches["strong_enter_momentum_threshold"]),
        "strong_enter_change_threshold": float(scanner_switches["strong_enter_change_threshold"]),
        "enter_score_threshold": float(scanner_switches["enter_score_threshold"]),
        "enter_momentum_threshold": float(scanner_switches["enter_momentum_threshold"]),
        "enter_change_threshold": float(scanner_switches["enter_change_threshold"]),
        "watch_score_threshold": float(scanner_switches["watch_score_threshold"]),
        "watch_momentum_threshold": float(scanner_switches["watch_momentum_threshold"]),
        "avoid_score_threshold": float(scanner_switches["avoid_score_threshold"]),
    }
    if classifier_thresholds_override:
        classifier_thresholds.update(classifier_thresholds_override)

    category_filter = category_allowlist
    if category_filter is None:
        category_filter = set(filter_switches["market_category_allowlist"])
    title_filter = title_terms
    if title_filter is None:
        title_filter = set(filter_switches["market_title_include"])
    external_id_filter = external_id_allowlist
    if external_id_filter is None:
        external_id_filter = set(filter_switches["market_external_id_allowlist"])

    markets_query = (
        db.query(Market)
        .filter(Market.status == "open")
        .order_by(Market.id.asc())
    )
    all_open_markets = markets_query.all()
    markets: list[Market] = []
    for market in all_open_markets:
        if not category_matches(market.category, category_filter):
            stats["markets_filtered_out"] += 1
            continue
        if not title_matches(market.title, title_filter):
            stats["markets_filtered_out"] += 1
            continue
        if not external_id_matches(market.external_id, external_id_filter):
            stats["markets_filtered_out"] += 1
            continue
        markets.append(market)

    if effective_market_limit is not None:
        markets = markets[:effective_market_limit]

    stats["markets_scanned"] = len(markets)

    if not markets:
        print("No hay mercados abiertos para escanear.")
        return stats

    for market in markets:
        recent_snapshots = get_recent_snapshots(db, market.id, limit=window_size)

        if not recent_snapshots:
            stats["markets_without_snapshots"] += 1
            print(f"[SKIP] Market {market.id} sin snapshots reales.")
            continue

        if len(recent_snapshots) < effective_min_history:
            stats["markets_with_insufficient_history"] += 1
            print(
                f"[SKIP] Market {market.id} insufficient history "
                f"({len(recent_snapshots)}/{effective_min_history})"
            )
            continue

        try:
            signal_type, confidence, edge_estimate, reason = evaluate_signal_v2(
                recent_snapshots,
                min_history=effective_min_history,
                thresholds=classifier_thresholds,
            )
        except TypeError:
            # Backward-compatible path for tests/patches monkeypatching evaluate_signal_v2
            # with the legacy single-argument signature.
            signal_type, confidence, edge_estimate, reason = evaluate_signal_v2(recent_snapshots)
        
        if signal_type in {"AVOID", "WAIT_LIQUIDITY", "WAIT_STABILITY"}:
            edge_estimate = 0.0
        
        if signal_type == "AVOID":
            confidence = min(confidence, 0.25)
            edge_estimate = 0.0
            stats["signals_skipped_avoid"] += 1
            print(
                f"[SKIP AVOID] Market {market.id} | "
                f"confidence={confidence} | edge={edge_estimate}"
            )
            continue

        duplicate_cutoff = datetime.now(timezone.utc) - timedelta(seconds=30)

        recent_duplicate_signal = (
            db.query(Signal)
            .filter(Signal.market_id == market.id)
            .filter(Signal.strategy_name == strategy_name)
            .filter(Signal.signal_type == signal_type)
            .filter(Signal.created_at >= duplicate_cutoff)
            .order_by(Signal.created_at.desc())
            .first()
        )

        if recent_duplicate_signal is not None:
            stats["signals_skipped_duplicate"] += 1
            print(
                f"[SKIP SIGNAL DUPLICATE] Market {market.id} | "
                f"{signal_type} | confidence={confidence} | edge={edge_estimate}"
            )
            continue

        reason_data = json.loads(reason)
        components = reason_data.get("components", {})

        momentum = components.get("momentum", {}).get("score")
        liquidity = components.get("liquidity_consistency", {}).get("score")
        stability = components.get("stability", {}).get("score")
        microstructure = components.get("microstructure", {}).get("score")
        noise = components.get("noise", {}).get("penalty")
        direction = components.get("momentum", {}).get("direction")
        reason_code = reason_data.get("reason_code")

        alpha_direction = str(direction).lower() if direction is not None else ""
        signal_direction = "UP" if alpha_direction == "up" else "DOWN" if alpha_direction == "down" else None
        latest_snapshot = recent_snapshots[-1]
        created_at = datetime.now(timezone.utc)

        signal_status = None
        confirmation_deadline = None
        if signal_type == "WATCH":
            signal_status = "WATCH"
            confirmation_deadline = created_at + timedelta(minutes=confirmation_window_minutes)

        new_signal = Signal(
            market_id=market.id,
            signal_type=signal_type,
            strategy_name=strategy_name,
            confidence=confidence,
            edge_estimate=edge_estimate,
            status=signal_status,
            direction=signal_direction,
            reference_price=latest_snapshot.yes_price,
            reference_spread=latest_snapshot.spread,
            reference_liquidity=latest_snapshot.liquidity,
            confirmation_deadline=confirmation_deadline,
            reason=reason,
            is_executed=False,
            created_at=created_at,
        )
        db.add(new_signal)
        stats["signals_inserted"] += 1

        print(
            f"[SIGNAL] Market {market.id} | {signal_type} | "
            f"confidence={confidence} | edge={edge_estimate} | "
            f"mom={momentum} dir={direction} | "
            f"liq={liquidity} stab={stability} micro={microstructure} "
            f"noise={noise} | reason={reason_code}"
        )

    db.commit()
    
    print(
        f"\n[SCAN COMPLETE] Scanned: {stats['markets_scanned']}, "
        f"Filtered out: {stats['markets_filtered_out']}, "
        f"Signals inserted: {stats['signals_inserted']}, "
        f"Signals skipped duplicate: {stats['signals_skipped_duplicate']}, "
        f"Signals skipped avoid: {stats['signals_skipped_avoid']}, "
        f"Without snapshots: {stats['markets_without_snapshots']}, "
        f"Insufficient history: {stats['markets_with_insufficient_history']}\n"
    )
    return stats
