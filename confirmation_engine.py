from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


VALID_DIRECTIONS = {"UP", "DOWN"}
TERMINAL_STATUSES = {"CONFIRMED", "INVALIDATED", "EXPIRED"}


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _get_attr(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clamp(value: float, min_value: float = 0.0, max_value: float = 1.0) -> float:
    return max(min_value, min(max_value, value))


def _normalize_positive(value: float, floor: float, ceiling: float) -> float:
    if ceiling <= floor:
        return 0.0
    if value <= floor:
        return 0.0
    if value >= ceiling:
        return 1.0
    return (value - floor) / (ceiling - floor)


def compute_directional_delta(signal: Any, price: float) -> float:
    """
    Returns signed movement relative to signal.reference_price.
    Positive means movement is in favor of the signal direction.
    """
    direction = str(_get_attr(signal, "direction", "")).upper()
    reference_price = _as_float(_get_attr(signal, "reference_price", None), 0.0)
    current_price = _as_float(price, reference_price)

    move = current_price - reference_price
    if direction == "UP":
        return move
    if direction == "DOWN":
        return -move
    return 0.0


def compute_persistence(signal: Any, snapshots: list[Any]) -> float:
    """
    Measures directional persistence as the ratio of the latest
    consecutive favorable moves over all step moves.
    Returns value in [0, 1].
    """
    if len(snapshots) < 2:
        return 0.0

    direction = str(_get_attr(signal, "direction", "")).upper()
    if direction not in VALID_DIRECTIONS:
        return 0.0

    prices = [_as_float(_get_attr(s, "yes_price", None), float("nan")) for s in snapshots]
    prices = [p for p in prices if p == p]

    if len(prices) < 2:
        return 0.0

    steps = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    favorable = [step > 0 if direction == "UP" else step < 0 for step in steps]

    streak = 0
    for is_favorable in reversed(favorable):
        if is_favorable:
            streak += 1
        else:
            break

    return _clamp(streak / len(steps))


def compute_confirmation_score(signal: Any, snapshots: list[Any], config: dict[str, Any]) -> float:
    """
    Weighted score combining:
    - directional price move (normalized)
    - persistence
    - liquidity quality
    - spread penalty
    """
    if not snapshots:
        return 0.0

    latest = snapshots[-1]
    latest_price = _as_float(_get_attr(latest, "yes_price", None), _as_float(_get_attr(signal, "reference_price", None), 0.0))
    latest_liquidity = _as_float(_get_attr(latest, "liquidity", None), 0.0)
    latest_spread = _as_float(_get_attr(latest, "spread", None), 0.0)

    directional_delta = compute_directional_delta(signal, latest_price)
    persistence = compute_persistence(signal, snapshots)

    reference_liquidity = _as_float(_get_attr(signal, "reference_liquidity", None), 0.0)
    reference_spread = _as_float(_get_attr(signal, "reference_spread", None), 0.0)

    move_scale = _as_float(config.get("price_move_scale", 0.03), 0.03)
    min_liq_ratio = _as_float(config.get("min_liquidity_ratio", 0.5), 0.5)
    good_liq_ratio = _as_float(config.get("good_liquidity_ratio", 1.0), 1.0)
    max_spread_mult = _as_float(config.get("max_spread_multiplier", 2.0), 2.0)

    w_move = _as_float(config.get("w_price_move", 0.45), 0.45)
    w_persistence = _as_float(config.get("w_persistence", 0.30), 0.30)
    w_liquidity = _as_float(config.get("w_liquidity", 0.20), 0.20)
    w_spread_penalty = _as_float(config.get("w_spread_penalty", 0.15), 0.15)

    if move_scale <= 0:
        move_scale = 0.03

    move_score = _clamp((directional_delta / move_scale + 1.0) / 2.0)

    if reference_liquidity > 0:
        liq_ratio = latest_liquidity / reference_liquidity
        liquidity_score = _normalize_positive(liq_ratio, min_liq_ratio, good_liq_ratio)
    else:
        liquidity_score = _normalize_positive(latest_liquidity, 1_000.0, 50_000.0)

    if reference_spread > 0:
        spread_ratio = latest_spread / reference_spread if reference_spread > 0 else max_spread_mult
        spread_penalty = _normalize_positive(spread_ratio, 1.0, max_spread_mult)
    else:
        spread_penalty = _normalize_positive(latest_spread, 0.02, 0.12)

    score = (
        (w_move * move_score)
        + (w_persistence * persistence)
        + (w_liquidity * liquidity_score)
        - (w_spread_penalty * spread_penalty)
    )

    return float(round(score, 6))


def evaluate_confirmation(signal: Any, snapshots: list[Any], config: dict[str, Any]) -> tuple[str, float]:
    """
    Returns (new_status, score) based on confirmation lifecycle.

    Lifecycle intent:
    WATCH -> CONFIRMING -> CONFIRMED / INVALIDATED / EXPIRED
    """
    current_status = str(_get_attr(signal, "status", "WATCH") or "WATCH").upper()
    score = compute_confirmation_score(signal, snapshots, config)

    if current_status in TERMINAL_STATUSES:
        return current_status, score

    now = _now_utc()
    deadline = _to_aware_utc(_get_attr(signal, "confirmation_deadline", None))

    if deadline is not None and now > deadline:
        return "EXPIRED", score

    latest = snapshots[-1] if snapshots else None
    latest_liquidity = _as_float(_get_attr(latest, "liquidity", None), 0.0)
    latest_spread = _as_float(_get_attr(latest, "spread", None), 0.0)

    reference_liquidity = _as_float(_get_attr(signal, "reference_liquidity", None), 0.0)
    reference_spread = _as_float(_get_attr(signal, "reference_spread", None), 0.0)

    min_liquidity_ratio = _as_float(config.get("min_liquidity_ratio", 0.5), 0.5)
    max_spread_multiplier = _as_float(config.get("max_spread_multiplier", 2.0), 2.0)
    invalidation_score = _as_float(config.get("invalidation_score", -0.15), -0.15)
    confirmation_threshold = _as_float(config.get("confirmation_threshold", 0.65), 0.65)
    min_confirmation_snapshots = int(_as_float(config.get("min_confirmation_snapshots", 3), 3))
    min_persistence_for_confirmation = _as_float(config.get("min_persistence_for_confirmation", 0.5), 0.5)

    if reference_liquidity > 0 and latest_liquidity < (reference_liquidity * min_liquidity_ratio):
        return "INVALIDATED", score

    if reference_spread > 0 and latest_spread > (reference_spread * max_spread_multiplier):
        return "INVALIDATED", score

    if score <= invalidation_score:
        return "INVALIDATED", score

    if len(snapshots) >= max(2, min_confirmation_snapshots):
        persistence = compute_persistence(signal, snapshots)
    else:
        persistence = 0.0

    # Guardrail: avoid confirming from a single or too-short move sequence.
    if (
        score >= confirmation_threshold
        and len(snapshots) >= max(2, min_confirmation_snapshots)
        and persistence >= min_persistence_for_confirmation
    ):
        return "CONFIRMED", score

    return "CONFIRMING", score
