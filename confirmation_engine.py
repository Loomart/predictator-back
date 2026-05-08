from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, TypedDict

from confirmation_config import (
    CONFIRMATION_MIN_REQUIRED_SNAPSHOTS,
    CONFIRMATION_RECENT_WINDOW_SIZE,
    CONFIRMATION_THRESHOLD,
    CONTINUATION_SCORE_WEIGHT,
    DEFAULT_CONFIRMATION_CONFIG,
    DEFAULT_ZERO_VALUE,
    GOOD_LIQUIDITY_RATIO,
    INVALIDATION_SCORE,
    LIQUIDITY_ABS_CEILING,
    LIQUIDITY_ABS_FLOOR,
    LIQUIDITY_SCORE_WEIGHT,
    MAX_SLOPE_NORMALIZATION_VALUE,
    MAX_SPREAD_MULTIPLIER,
    MIN_CONFIRMATION_ELAPSED_SECONDS,
    MIN_CONFIRMATION_SNAPSHOTS,
    MIN_CONTINUATION_FOR_CONFIRMATION,
    MIN_LIQUIDITY_RATIO,
    MIN_PERSISTENCE_FOR_CONFIRMATION,
    MIN_SLOPE_VALUE,
    MIN_SNAPSHOTS_FOR_STEP_METRICS,
    PERSISTENCE_SCORE_WEIGHT,
    PRICE_MOVE_SCALE,
    PRICE_MOVE_SCORE_WEIGHT,
    REVERSAL_ALTERNATION_WEIGHT,
    REVERSAL_COUNTER_MOVE_WEIGHT,
    REVERSAL_COUNTER_NORMALIZATION_FACTOR,
    REVERSAL_MIN_MOVE_THRESHOLD,
    REVERSAL_NOISE_WEIGHT,
    REVERSAL_PENALTY_WEIGHT,
    SCORE_MAX_VALUE,
    SCORE_MIN_VALUE,
    SCORE_ROUND_DECIMALS,
    SLOPE_SCORE_WEIGHT,
    SLOPE_VOLATILITY_PENALTY_WEIGHT,
    SPREAD_ABS_CEILING,
    SPREAD_ABS_FLOOR,
    SPREAD_PENALTY_WEIGHT,
    TERMINAL_STATUSES,
    VALID_DIRECTIONS,
)


class ConfirmationScoreBreakdown(TypedDict):
    continuation_score: float
    persistence_score: float
    slope_score: float
    liquidity_score: float
    reversal_penalty: float
    spread_penalty: float
    final_score: float


def _as_float(value: Any, default: float = DEFAULT_ZERO_VALUE) -> float:
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


def _to_aware_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _elapsed_seconds(start: datetime | None, end: datetime | None) -> float:
    start_utc = _to_aware_utc(start)
    end_utc = _to_aware_utc(end)
    if start_utc is None or end_utc is None:
        return DEFAULT_ZERO_VALUE
    return max(DEFAULT_ZERO_VALUE, (end_utc - start_utc).total_seconds())


def _clamp(value: float, min_value: float = SCORE_MIN_VALUE, max_value: float = SCORE_MAX_VALUE) -> float:
    return max(min_value, min(max_value, value))


def _normalize_positive(value: float, floor: float, ceiling: float) -> float:
    if ceiling <= floor:
        return SCORE_MIN_VALUE
    if value <= floor:
        return SCORE_MIN_VALUE
    if value >= ceiling:
        return SCORE_MAX_VALUE
    return (value - floor) / (ceiling - floor)


def get_recent_confirmation_snapshots(all_snapshots: list[Any], window_size: int) -> list[Any]:
    if window_size <= 0:
        return []
    if len(all_snapshots) <= window_size:
        return list(all_snapshots)
    return list(all_snapshots[-window_size:])


def compute_directional_delta(signal: Any, price: float) -> float:
    direction = str(_get_attr(signal, "direction", "")).upper()
    reference_price = _as_float(_get_attr(signal, "reference_price", None), DEFAULT_ZERO_VALUE)
    current_price = _as_float(price, reference_price)

    move = current_price - reference_price
    if direction == "UP":
        return move
    if direction == "DOWN":
        return -move
    return DEFAULT_ZERO_VALUE


def compute_persistence_score(signal: Any, snapshots: list[Any]) -> float:
    """Return [0,1] persistence from latest favorable streak length over all recent steps."""
    if len(snapshots) < MIN_SNAPSHOTS_FOR_STEP_METRICS:
        return SCORE_MIN_VALUE

    direction = str(_get_attr(signal, "direction", "")).upper()
    if direction not in VALID_DIRECTIONS:
        return SCORE_MIN_VALUE

    prices = [_as_float(_get_attr(s, "yes_price", None), float("nan")) for s in snapshots]
    prices = [p for p in prices if p == p]
    if len(prices) < MIN_SNAPSHOTS_FOR_STEP_METRICS:
        return SCORE_MIN_VALUE

    steps = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    favorable = [step > 0 if direction == "UP" else step < 0 for step in steps]

    streak = 0
    for is_favorable in reversed(favorable):
        if is_favorable:
            streak += 1
        else:
            break

    return _clamp(streak / len(steps))


def compute_price_slope(signal: Any, snapshots: list[Any]) -> float:
    """Return directional slope where positive favors signal direction and negative opposes it."""
    if len(snapshots) < MIN_SNAPSHOTS_FOR_STEP_METRICS:
        return DEFAULT_ZERO_VALUE

    direction = str(_get_attr(signal, "direction", "")).upper()
    if direction not in VALID_DIRECTIONS:
        return DEFAULT_ZERO_VALUE

    prices = [_as_float(_get_attr(s, "yes_price", None), float("nan")) for s in snapshots]
    prices = [p for p in prices if p == p]
    if len(prices) < MIN_SNAPSHOTS_FOR_STEP_METRICS:
        return DEFAULT_ZERO_VALUE

    signed_steps: list[float] = []
    for i in range(1, len(prices)):
        raw_step = prices[i] - prices[i - 1]
        signed_steps.append(raw_step if direction == "UP" else -raw_step)

    avg_signed_step = sum(signed_steps) / len(signed_steps)
    mean_abs_step = sum(abs(step) for step in signed_steps) / len(signed_steps)
    instability = DEFAULT_ZERO_VALUE
    if mean_abs_step > DEFAULT_ZERO_VALUE:
        instability = sum(abs(step - avg_signed_step) for step in signed_steps) / (len(signed_steps) * mean_abs_step)

    smoothness = _clamp(SCORE_MAX_VALUE - (SLOPE_VOLATILITY_PENALTY_WEIGHT * instability))
    return avg_signed_step * smoothness


def compute_slope_score(signal: Any, snapshots: list[Any]) -> float:
    """Normalize directional slope into [0,1] contribution for confirmation quality."""
    return _normalize_positive(
        compute_price_slope(signal, snapshots),
        MIN_SLOPE_VALUE,
        MAX_SLOPE_NORMALIZATION_VALUE,
    )


def compute_continuation_score(signal: Any, snapshots: list[Any], price_move_scale: float) -> float:
    """Map directional slope into continuation quality; smooth trend continuation scores higher."""
    slope = compute_price_slope(signal, snapshots)
    if price_move_scale <= DEFAULT_ZERO_VALUE:
        return SCORE_MIN_VALUE
    return _clamp((slope / price_move_scale + SCORE_MAX_VALUE) / (SCORE_MAX_VALUE + SCORE_MAX_VALUE))


def compute_liquidity_score(signal: Any, latest_snapshot: Any, config: dict[str, Any]) -> float:
    """Score liquidity quality relative to reference baseline (or absolute fallback band)."""
    latest_liquidity = _as_float(_get_attr(latest_snapshot, "liquidity", None), DEFAULT_ZERO_VALUE)
    reference_liquidity = _as_float(_get_attr(signal, "reference_liquidity", None), DEFAULT_ZERO_VALUE)

    min_ratio = _as_float(config.get("min_liquidity_ratio", MIN_LIQUIDITY_RATIO), MIN_LIQUIDITY_RATIO)
    good_ratio = _as_float(config.get("good_liquidity_ratio", GOOD_LIQUIDITY_RATIO), GOOD_LIQUIDITY_RATIO)
    abs_floor = _as_float(config.get("liquidity_abs_floor", LIQUIDITY_ABS_FLOOR), LIQUIDITY_ABS_FLOOR)
    abs_ceiling = _as_float(config.get("liquidity_abs_ceiling", LIQUIDITY_ABS_CEILING), LIQUIDITY_ABS_CEILING)

    if reference_liquidity > DEFAULT_ZERO_VALUE:
        return _normalize_positive(latest_liquidity / reference_liquidity, min_ratio, good_ratio)
    return _normalize_positive(latest_liquidity, abs_floor, abs_ceiling)


def compute_spread_penalty(signal: Any, latest_snapshot: Any, config: dict[str, Any]) -> float:
    """Return [0,1] spread deterioration penalty; wider spreads increase execution uncertainty."""
    latest_spread = _as_float(_get_attr(latest_snapshot, "spread", None), DEFAULT_ZERO_VALUE)
    reference_spread = _as_float(_get_attr(signal, "reference_spread", None), DEFAULT_ZERO_VALUE)
    max_spread_multiplier = _as_float(config.get("max_spread_multiplier", MAX_SPREAD_MULTIPLIER), MAX_SPREAD_MULTIPLIER)
    abs_floor = _as_float(config.get("spread_abs_floor", SPREAD_ABS_FLOOR), SPREAD_ABS_FLOOR)
    abs_ceiling = _as_float(config.get("spread_abs_ceiling", SPREAD_ABS_CEILING), SPREAD_ABS_CEILING)

    if reference_spread > DEFAULT_ZERO_VALUE:
        spread_ratio = latest_spread / reference_spread
        return _normalize_positive(spread_ratio, SCORE_MAX_VALUE, max_spread_multiplier)
    return _normalize_positive(latest_spread, abs_floor, abs_ceiling)


def compute_reversal_penalty(signal: Any, snapshots: list[Any]) -> float:
    """Penalize whipsaw/anti-directional moves while ignoring micro-noise below threshold."""
    if len(snapshots) < CONFIRMATION_MIN_REQUIRED_SNAPSHOTS:
        return SCORE_MIN_VALUE

    direction = str(_get_attr(signal, "direction", "")).upper()
    if direction not in VALID_DIRECTIONS:
        return SCORE_MIN_VALUE

    prices = [_as_float(_get_attr(s, "yes_price", None), float("nan")) for s in snapshots]
    prices = [p for p in prices if p == p]
    if len(prices) < MIN_SNAPSHOTS_FOR_STEP_METRICS:
        return SCORE_MIN_VALUE

    significant_signed_steps: list[float] = []
    for i in range(1, len(prices)):
        raw_step = prices[i] - prices[i - 1]
        signed_step = raw_step if direction == "UP" else -raw_step
        if abs(signed_step) >= REVERSAL_MIN_MOVE_THRESHOLD:
            significant_signed_steps.append(signed_step)

    if len(significant_signed_steps) < MIN_SNAPSHOTS_FOR_STEP_METRICS:
        return SCORE_MIN_VALUE

    sign_flips = 0
    for i in range(1, len(significant_signed_steps)):
        prev_sign = SCORE_MAX_VALUE if significant_signed_steps[i - 1] > DEFAULT_ZERO_VALUE else -SCORE_MAX_VALUE
        curr_sign = SCORE_MAX_VALUE if significant_signed_steps[i] > DEFAULT_ZERO_VALUE else -SCORE_MAX_VALUE
        if prev_sign != curr_sign:
            sign_flips += 1
    alternation_penalty = sign_flips / (len(significant_signed_steps) - 1)

    counter_moves = [-step for step in significant_signed_steps if step < DEFAULT_ZERO_VALUE]
    counter_move_sum = sum(counter_moves)
    sharpest_counter = max(counter_moves) if counter_moves else DEFAULT_ZERO_VALUE
    counter_move_penalty = _clamp(
        (counter_move_sum + sharpest_counter)
        / (REVERSAL_COUNTER_NORMALIZATION_FACTOR * _as_float(DEFAULT_CONFIRMATION_CONFIG.get("price_move_scale", PRICE_MOVE_SCALE), PRICE_MOVE_SCALE))
    )

    against_count = sum(1 for step in significant_signed_steps if step < DEFAULT_ZERO_VALUE)
    noise_penalty = against_count / len(significant_signed_steps)

    return _clamp(
        (REVERSAL_ALTERNATION_WEIGHT * alternation_penalty)
        + (REVERSAL_COUNTER_MOVE_WEIGHT * counter_move_penalty)
        + (REVERSAL_NOISE_WEIGHT * noise_penalty)
    )


def compute_confirmation_score(signal: Any, snapshots: list[Any], config: dict[str, Any]) -> ConfirmationScoreBreakdown:
    """Assemble tunable confirmation score from transparent modular components."""
    if not snapshots:
        return {
            "continuation_score": SCORE_MIN_VALUE,
            "persistence_score": SCORE_MIN_VALUE,
            "slope_score": SCORE_MIN_VALUE,
            "liquidity_score": SCORE_MIN_VALUE,
            "reversal_penalty": SCORE_MIN_VALUE,
            "spread_penalty": SCORE_MIN_VALUE,
            "final_score": SCORE_MIN_VALUE,
        }

    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    window_size = int(_as_float(effective.get("confirmation_recent_window_size", CONFIRMATION_RECENT_WINDOW_SIZE), CONFIRMATION_RECENT_WINDOW_SIZE))
    recent_snapshots = get_recent_confirmation_snapshots(snapshots, window_size)
    min_required = int(_as_float(effective.get("confirmation_min_required_snapshots", CONFIRMATION_MIN_REQUIRED_SNAPSHOTS), CONFIRMATION_MIN_REQUIRED_SNAPSHOTS))

    if len(recent_snapshots) < max(MIN_SNAPSHOTS_FOR_STEP_METRICS, min_required):
        return {
            "continuation_score": SCORE_MIN_VALUE,
            "persistence_score": SCORE_MIN_VALUE,
            "slope_score": SCORE_MIN_VALUE,
            "liquidity_score": SCORE_MIN_VALUE,
            "reversal_penalty": SCORE_MIN_VALUE,
            "spread_penalty": SCORE_MIN_VALUE,
            "final_score": SCORE_MIN_VALUE,
        }

    latest = recent_snapshots[-1]
    latest_price = _as_float(_get_attr(latest, "yes_price", None), _as_float(_get_attr(signal, "reference_price", None), DEFAULT_ZERO_VALUE))
    price_move_scale = _as_float(effective.get("price_move_scale", PRICE_MOVE_SCALE), PRICE_MOVE_SCALE)
    if price_move_scale <= DEFAULT_ZERO_VALUE:
        price_move_scale = PRICE_MOVE_SCALE

    directional_delta = compute_directional_delta(signal, latest_price)
    move_score = _clamp((directional_delta / price_move_scale + SCORE_MAX_VALUE) / (SCORE_MAX_VALUE + SCORE_MAX_VALUE))
    continuation_score = compute_continuation_score(signal, recent_snapshots, price_move_scale)
    persistence_score = compute_persistence_score(signal, recent_snapshots)
    slope_score = compute_slope_score(signal, recent_snapshots)
    liquidity_score = compute_liquidity_score(signal, latest, effective)
    spread_penalty = compute_spread_penalty(signal, latest, effective)
    reversal_penalty = compute_reversal_penalty(signal, recent_snapshots)

    price_move_weight = _as_float(effective.get("price_move_score_weight", PRICE_MOVE_SCORE_WEIGHT), PRICE_MOVE_SCORE_WEIGHT)
    continuation_weight = _as_float(effective.get("w_continuation", CONTINUATION_SCORE_WEIGHT), CONTINUATION_SCORE_WEIGHT)
    persistence_weight = _as_float(effective.get("w_persistence", PERSISTENCE_SCORE_WEIGHT), PERSISTENCE_SCORE_WEIGHT)
    slope_weight = _as_float(effective.get("w_slope", SLOPE_SCORE_WEIGHT), SLOPE_SCORE_WEIGHT)
    liquidity_weight = _as_float(effective.get("w_liquidity", LIQUIDITY_SCORE_WEIGHT), LIQUIDITY_SCORE_WEIGHT)
    spread_weight = _as_float(effective.get("w_spread_penalty", SPREAD_PENALTY_WEIGHT), SPREAD_PENALTY_WEIGHT)
    reversal_weight = _as_float(effective.get("w_reversal_penalty", REVERSAL_PENALTY_WEIGHT), REVERSAL_PENALTY_WEIGHT)

    final_score = (
        (price_move_weight * move_score)
        + (continuation_weight * continuation_score)
        + (persistence_weight * persistence_score)
        + (slope_weight * slope_score)
        + (liquidity_weight * liquidity_score)
        - (spread_weight * spread_penalty)
        - (reversal_weight * reversal_penalty)
    )

    return {
        "continuation_score": round(continuation_score, SCORE_ROUND_DECIMALS),
        "persistence_score": round(persistence_score, SCORE_ROUND_DECIMALS),
        "slope_score": round(slope_score, SCORE_ROUND_DECIMALS),
        "liquidity_score": round(liquidity_score, SCORE_ROUND_DECIMALS),
        "reversal_penalty": round(reversal_penalty, SCORE_ROUND_DECIMALS),
        "spread_penalty": round(spread_penalty, SCORE_ROUND_DECIMALS),
        "final_score": round(float(final_score), SCORE_ROUND_DECIMALS),
    }


def has_minimum_snapshot_age(recent_snapshots: list[Any], config: dict[str, Any]) -> bool:
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    min_snapshot_age = int(_as_float(effective.get("min_confirmation_snapshots", MIN_CONFIRMATION_SNAPSHOTS), MIN_CONFIRMATION_SNAPSHOTS))
    return len(recent_snapshots) >= max(MIN_SNAPSHOTS_FOR_STEP_METRICS, min_snapshot_age)


def has_minimum_time_age(signal: Any, now_utc: datetime, config: dict[str, Any]) -> bool:
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    min_elapsed_seconds = _as_float(effective.get("min_confirmation_elapsed_seconds", MIN_CONFIRMATION_ELAPSED_SECONDS), MIN_CONFIRMATION_ELAPSED_SECONDS)
    created_at = _get_attr(signal, "created_at", None)
    return _elapsed_seconds(created_at, now_utc) >= min_elapsed_seconds


def evaluate_confirmation(signal: Any, snapshots: list[Any], config: dict[str, Any]) -> tuple[str, float]:
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    current_status = str(_get_attr(signal, "status", "WATCH") or "WATCH").upper()
    window_size = int(_as_float(effective.get("confirmation_recent_window_size", CONFIRMATION_RECENT_WINDOW_SIZE), CONFIRMATION_RECENT_WINDOW_SIZE))
    recent_snapshots = get_recent_confirmation_snapshots(snapshots, window_size)
    score_breakdown = compute_confirmation_score(signal, snapshots, effective)
    score = score_breakdown["final_score"]

    if current_status in TERMINAL_STATUSES:
        return current_status, score

    now = datetime.now(timezone.utc)
    deadline = _to_aware_utc(_get_attr(signal, "confirmation_deadline", None))
    if deadline is not None and now > deadline:
        return "EXPIRED", score

    latest = recent_snapshots[-1] if recent_snapshots else None
    latest_liquidity = _as_float(_get_attr(latest, "liquidity", None), DEFAULT_ZERO_VALUE)
    latest_spread = _as_float(_get_attr(latest, "spread", None), DEFAULT_ZERO_VALUE)
    reference_liquidity = _as_float(_get_attr(signal, "reference_liquidity", None), DEFAULT_ZERO_VALUE)
    reference_spread = _as_float(_get_attr(signal, "reference_spread", None), DEFAULT_ZERO_VALUE)

    min_liquidity_ratio = _as_float(effective.get("min_liquidity_ratio", MIN_LIQUIDITY_RATIO), MIN_LIQUIDITY_RATIO)
    max_spread_multiplier = _as_float(effective.get("max_spread_multiplier", MAX_SPREAD_MULTIPLIER), MAX_SPREAD_MULTIPLIER)
    invalidation_score = _as_float(effective.get("invalidation_score", INVALIDATION_SCORE), INVALIDATION_SCORE)
    confirmation_threshold = _as_float(effective.get("confirmation_threshold", CONFIRMATION_THRESHOLD), CONFIRMATION_THRESHOLD)

    if reference_liquidity > DEFAULT_ZERO_VALUE and latest_liquidity < (reference_liquidity * min_liquidity_ratio):
        return "INVALIDATED", score
    if reference_spread > DEFAULT_ZERO_VALUE and latest_spread > (reference_spread * max_spread_multiplier):
        return "INVALIDATED", score
    if score <= invalidation_score:
        return "INVALIDATED", score

    snapshot_readiness = has_minimum_snapshot_age(recent_snapshots, effective)
    time_readiness = has_minimum_time_age(signal, now, effective)

    if (
        score >= confirmation_threshold
        and snapshot_readiness
        and time_readiness
        and score_breakdown["persistence_score"] >= _as_float(effective.get("min_persistence_for_confirmation", MIN_PERSISTENCE_FOR_CONFIRMATION), MIN_PERSISTENCE_FOR_CONFIRMATION)
        and score_breakdown["continuation_score"] >= _as_float(effective.get("min_continuation_for_confirmation", MIN_CONTINUATION_FOR_CONFIRMATION), MIN_CONTINUATION_FOR_CONFIRMATION)
    ):
        return "CONFIRMED", score

    return "CONFIRMING", score
