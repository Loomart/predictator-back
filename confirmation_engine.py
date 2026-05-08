from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Any, TypedDict

from backend.confirmation_config import (
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
    REGIME_PERSISTENCE_RANGING_BOOST,
    REGIME_RANGING_THRESHOLD_DELTA,
    REGIME_REVERSAL_VOLATILE_BOOST,
    REGIME_TRENDING_THRESHOLD_DELTA,
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
from backend.market_regime_analyzer import MarketRegime, MarketRegimeAnalysisResult, analyze_market_regime


class SignalStatus(str, Enum):
    WATCH = "WATCH"
    CONFIRMING = "CONFIRMING"
    CONFIRMED = "CONFIRMED"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


class ConfirmationReason(str, Enum):
    CONFIRMATION_THRESHOLD_REACHED = "confirmation_threshold_reached"
    NOT_READY_MIN_SNAPSHOT_AGE = "not_ready_min_snapshot_age"
    NOT_READY_MIN_TIME_AGE = "not_ready_min_time_age"
    BELOW_CONFIRMATION_THRESHOLD = "below_confirmation_threshold"
    BELOW_PERSISTENCE_THRESHOLD = "below_persistence_threshold"
    BELOW_CONTINUATION_THRESHOLD = "below_continuation_threshold"
    TERMINAL_STATUS_NOOP = "terminal_status_noop"


class InvalidationReason(str, Enum):
    NONE = "none"
    LIQUIDITY_DROP = "liquidity_drop"
    SPREAD_EXPANSION = "spread_expansion"
    NEGATIVE_SCORE = "negative_score"
    DEADLINE_EXCEEDED = "deadline_exceeded"


class ConfirmationScoreBreakdown(TypedDict):
    continuation_score: float
    persistence_score: float
    slope_score: float
    liquidity_score: float
    reversal_penalty: float
    spread_penalty: float
    final_score: float
    directional_delta: float
    slope_value: float
    persistence_ratio: float


@dataclass(frozen=True)
class ConfirmationEvaluationResult:
    signal_id: int | None
    market_id: int | None
    status_before: SignalStatus
    status_after: SignalStatus
    continuation_score: float
    persistence_score: float
    slope_score: float
    liquidity_score: float
    spread_penalty: float
    reversal_penalty: float
    final_score: float
    directional_delta: float
    slope_value: float
    persistence_ratio: float
    evaluated_snapshot_count: int
    elapsed_seconds: float
    confirmation_reason: ConfirmationReason
    invalidation_reason: InvalidationReason
    regime_analysis: MarketRegimeAnalysisResult

    def is_confirmed(self) -> bool:
        return self.status_after == SignalStatus.CONFIRMED

    def is_invalidated(self) -> bool:
        return self.status_after == SignalStatus.INVALIDATED

    def is_expired(self) -> bool:
        return self.status_after == SignalStatus.EXPIRED

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status_before"] = self.status_before.value
        payload["status_after"] = self.status_after.value
        payload["confirmation_reason"] = self.confirmation_reason.value
        payload["invalidation_reason"] = self.invalidation_reason.value
        payload["regime_analysis"] = self.regime_analysis.to_dict()
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), default=str)


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


def _as_signal_status(value: Any) -> SignalStatus:
    try:
        normalized = str(value or SignalStatus.WATCH.value).upper()
        return SignalStatus(normalized)
    except ValueError:
        return SignalStatus.WATCH


def get_recent_confirmation_snapshots(all_snapshots: list[Any], window_size: int) -> list[Any]:
    """Return the trailing snapshot window used for confirmation decisions."""
    if window_size <= 0:
        return []
    if len(all_snapshots) <= window_size:
        return list(all_snapshots)
    return list(all_snapshots[-window_size:])


def compute_directional_delta(signal: Any, price: float) -> float:
    """Return signed price movement from reference: positive supports signal direction."""
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
    """Return persistence ratio [0,1] from the latest favorable step streak."""
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
    """Return directional slope value; positive supports signal, negative opposes it."""
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
    """Normalize slope value into [0,1] score contribution."""
    return _normalize_positive(compute_price_slope(signal, snapshots), MIN_SLOPE_VALUE, MAX_SLOPE_NORMALIZATION_VALUE)


def compute_continuation_score(signal: Any, snapshots: list[Any], price_move_scale: float) -> float:
    """Compute continuation quality from directional slope and movement scale."""
    slope = compute_price_slope(signal, snapshots)
    if price_move_scale <= DEFAULT_ZERO_VALUE:
        return SCORE_MIN_VALUE
    return _clamp((slope / price_move_scale + SCORE_MAX_VALUE) / (SCORE_MAX_VALUE + SCORE_MAX_VALUE))


def compute_liquidity_score(signal: Any, latest_snapshot: Any, config: dict[str, Any]) -> float:
    """Score liquidity quality against reference or fallback absolute band."""
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
    """Compute spread deterioration penalty [0,1] from relative or absolute spread expansion."""
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
    """Compute whipsaw/counter-move penalty [0,1], ignoring micro-noise below threshold."""
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


def _empty_breakdown() -> ConfirmationScoreBreakdown:
    return {
        "continuation_score": SCORE_MIN_VALUE,
        "persistence_score": SCORE_MIN_VALUE,
        "slope_score": SCORE_MIN_VALUE,
        "liquidity_score": SCORE_MIN_VALUE,
        "reversal_penalty": SCORE_MIN_VALUE,
        "spread_penalty": SCORE_MIN_VALUE,
        "final_score": SCORE_MIN_VALUE,
        "directional_delta": DEFAULT_ZERO_VALUE,
        "slope_value": DEFAULT_ZERO_VALUE,
        "persistence_ratio": SCORE_MIN_VALUE,
    }


def compute_confirmation_score(signal: Any, snapshots: list[Any], config: dict[str, Any]) -> ConfirmationScoreBreakdown:
    """Return detailed modular scoring breakdown used by confirmation evaluation."""
    if not snapshots:
        return _empty_breakdown()

    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    window_size = int(_as_float(effective.get("confirmation_recent_window_size", CONFIRMATION_RECENT_WINDOW_SIZE), CONFIRMATION_RECENT_WINDOW_SIZE))
    recent_snapshots = get_recent_confirmation_snapshots(snapshots, window_size)
    min_required = int(_as_float(effective.get("confirmation_min_required_snapshots", CONFIRMATION_MIN_REQUIRED_SNAPSHOTS), CONFIRMATION_MIN_REQUIRED_SNAPSHOTS))

    if len(recent_snapshots) < max(MIN_SNAPSHOTS_FOR_STEP_METRICS, min_required):
        return _empty_breakdown()

    latest = recent_snapshots[-1]
    latest_price = _as_float(_get_attr(latest, "yes_price", None), _as_float(_get_attr(signal, "reference_price", None), DEFAULT_ZERO_VALUE))
    price_move_scale = _as_float(effective.get("price_move_scale", PRICE_MOVE_SCALE), PRICE_MOVE_SCALE)
    if price_move_scale <= DEFAULT_ZERO_VALUE:
        price_move_scale = PRICE_MOVE_SCALE

    directional_delta = compute_directional_delta(signal, latest_price)
    slope_value = compute_price_slope(signal, recent_snapshots)
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
        "directional_delta": round(directional_delta, SCORE_ROUND_DECIMALS),
        "slope_value": round(slope_value, SCORE_ROUND_DECIMALS),
        "persistence_ratio": round(persistence_score, SCORE_ROUND_DECIMALS),
    }


def has_minimum_snapshot_age(recent_snapshots: list[Any], config: dict[str, Any]) -> bool:
    """Return whether the evaluation has enough snapshots for confirmation maturity."""
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    min_snapshot_age = int(_as_float(effective.get("min_confirmation_snapshots", MIN_CONFIRMATION_SNAPSHOTS), MIN_CONFIRMATION_SNAPSHOTS))
    return len(recent_snapshots) >= max(MIN_SNAPSHOTS_FOR_STEP_METRICS, min_snapshot_age)


def has_minimum_time_age(signal: Any, now_utc: datetime, config: dict[str, Any]) -> bool:
    """Return whether the signal age in seconds is mature enough for confirmation."""
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    min_elapsed_seconds = _as_float(effective.get("min_confirmation_elapsed_seconds", MIN_CONFIRMATION_ELAPSED_SECONDS), MIN_CONFIRMATION_ELAPSED_SECONDS)
    created_at = _get_attr(signal, "created_at", None)
    return _elapsed_seconds(created_at, now_utc) >= min_elapsed_seconds


def regime_adjusted_score_threshold(
    base_threshold: float,
    regime_analysis: MarketRegimeAnalysisResult,
    config: dict[str, Any],
) -> float:
    """Return a regime-adaptive score threshold for confirmation decisions."""
    trending_delta = _as_float(
        config.get("regime_trending_threshold_delta", REGIME_TRENDING_THRESHOLD_DELTA),
        REGIME_TRENDING_THRESHOLD_DELTA,
    )
    ranging_delta = _as_float(
        config.get("regime_ranging_threshold_delta", REGIME_RANGING_THRESHOLD_DELTA),
        REGIME_RANGING_THRESHOLD_DELTA,
    )

    if regime_analysis.regime == MarketRegime.TRENDING:
        return max(SCORE_MIN_VALUE, base_threshold - trending_delta)
    if regime_analysis.regime == MarketRegime.RANGING:
        return min(SCORE_MAX_VALUE, base_threshold + ranging_delta)
    return base_threshold


def evaluate_confirmation(signal: Any, snapshots: list[Any], config: dict[str, Any]) -> ConfirmationEvaluationResult:
    """Evaluate confirmation lifecycle and return full traceable evaluation result."""
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **config}
    status_before = _as_signal_status(_get_attr(signal, "status", SignalStatus.WATCH.value))
    window_size = int(_as_float(effective.get("confirmation_recent_window_size", CONFIRMATION_RECENT_WINDOW_SIZE), CONFIRMATION_RECENT_WINDOW_SIZE))
    recent_snapshots = get_recent_confirmation_snapshots(snapshots, window_size)
    score_breakdown = compute_confirmation_score(signal, snapshots, effective)
    regime_analysis = analyze_market_regime(recent_snapshots, effective)

    now = datetime.now(timezone.utc)
    elapsed_seconds = _elapsed_seconds(_get_attr(signal, "created_at", None), now)

    status_after = SignalStatus.CONFIRMING
    confirmation_reason = ConfirmationReason.BELOW_CONFIRMATION_THRESHOLD
    invalidation_reason = InvalidationReason.NONE

    if status_before.value in TERMINAL_STATUSES:
        status_after = status_before
        confirmation_reason = ConfirmationReason.TERMINAL_STATUS_NOOP
    else:
        deadline = _to_aware_utc(_get_attr(signal, "confirmation_deadline", None))
        if deadline is not None and now > deadline:
            status_after = SignalStatus.EXPIRED
            invalidation_reason = InvalidationReason.DEADLINE_EXCEEDED
        else:
            latest = recent_snapshots[-1] if recent_snapshots else None
            latest_liquidity = _as_float(_get_attr(latest, "liquidity", None), DEFAULT_ZERO_VALUE)
            latest_spread = _as_float(_get_attr(latest, "spread", None), DEFAULT_ZERO_VALUE)
            reference_liquidity = _as_float(_get_attr(signal, "reference_liquidity", None), DEFAULT_ZERO_VALUE)
            reference_spread = _as_float(_get_attr(signal, "reference_spread", None), DEFAULT_ZERO_VALUE)

            min_liquidity_ratio = _as_float(effective.get("min_liquidity_ratio", MIN_LIQUIDITY_RATIO), MIN_LIQUIDITY_RATIO)
            max_spread_multiplier = _as_float(effective.get("max_spread_multiplier", MAX_SPREAD_MULTIPLIER), MAX_SPREAD_MULTIPLIER)
            invalidation_score = _as_float(effective.get("invalidation_score", INVALIDATION_SCORE), INVALIDATION_SCORE)
            confirmation_threshold = _as_float(effective.get("confirmation_threshold", CONFIRMATION_THRESHOLD), CONFIRMATION_THRESHOLD)
            adjusted_confirmation_threshold = regime_adjusted_score_threshold(
                confirmation_threshold,
                regime_analysis,
                effective,
            )
            ranging_persistence_boost = _as_float(
                effective.get("regime_persistence_ranging_boost", REGIME_PERSISTENCE_RANGING_BOOST),
                REGIME_PERSISTENCE_RANGING_BOOST,
            )
            volatile_reversal_boost = _as_float(
                effective.get("regime_reversal_volatile_boost", REGIME_REVERSAL_VOLATILE_BOOST),
                REGIME_REVERSAL_VOLATILE_BOOST,
            )
            volatile_adjusted_score = (
                score_breakdown["final_score"]
                - (
                    volatile_reversal_boost
                    if regime_analysis.regime == MarketRegime.VOLATILE
                    else DEFAULT_ZERO_VALUE
                )
            )

            if reference_liquidity > DEFAULT_ZERO_VALUE and latest_liquidity < (reference_liquidity * min_liquidity_ratio):
                status_after = SignalStatus.INVALIDATED
                invalidation_reason = InvalidationReason.LIQUIDITY_DROP
            elif regime_analysis.regime == MarketRegime.ILLIQUID:
                status_after = SignalStatus.INVALIDATED
                invalidation_reason = InvalidationReason.LIQUIDITY_DROP
            elif reference_spread > DEFAULT_ZERO_VALUE and latest_spread > (reference_spread * max_spread_multiplier):
                status_after = SignalStatus.INVALIDATED
                invalidation_reason = InvalidationReason.SPREAD_EXPANSION
            elif volatile_adjusted_score <= invalidation_score:
                status_after = SignalStatus.INVALIDATED
                invalidation_reason = InvalidationReason.NEGATIVE_SCORE
            else:
                snapshot_ready = has_minimum_snapshot_age(recent_snapshots, effective)
                time_ready = has_minimum_time_age(signal, now, effective)

                if not snapshot_ready:
                    confirmation_reason = ConfirmationReason.NOT_READY_MIN_SNAPSHOT_AGE
                elif not time_ready:
                    confirmation_reason = ConfirmationReason.NOT_READY_MIN_TIME_AGE
                elif regime_analysis.regime == MarketRegime.UNSTABLE:
                    confirmation_reason = ConfirmationReason.BELOW_CONFIRMATION_THRESHOLD
                elif score_breakdown["final_score"] < adjusted_confirmation_threshold:
                    confirmation_reason = ConfirmationReason.BELOW_CONFIRMATION_THRESHOLD
                elif score_breakdown["persistence_score"] < (
                    _as_float(
                        effective.get("min_persistence_for_confirmation", MIN_PERSISTENCE_FOR_CONFIRMATION),
                        MIN_PERSISTENCE_FOR_CONFIRMATION,
                    )
                    + (
                        ranging_persistence_boost
                        if regime_analysis.regime == MarketRegime.RANGING
                        else DEFAULT_ZERO_VALUE
                    )
                ):
                    confirmation_reason = ConfirmationReason.BELOW_PERSISTENCE_THRESHOLD
                elif score_breakdown["continuation_score"] < _as_float(effective.get("min_continuation_for_confirmation", MIN_CONTINUATION_FOR_CONFIRMATION), MIN_CONTINUATION_FOR_CONFIRMATION):
                    confirmation_reason = ConfirmationReason.BELOW_CONTINUATION_THRESHOLD
                else:
                    status_after = SignalStatus.CONFIRMED
                    confirmation_reason = ConfirmationReason.CONFIRMATION_THRESHOLD_REACHED

    return ConfirmationEvaluationResult(
        signal_id=_get_attr(signal, "id", None),
        market_id=_get_attr(signal, "market_id", None),
        status_before=status_before,
        status_after=status_after,
        continuation_score=score_breakdown["continuation_score"],
        persistence_score=score_breakdown["persistence_score"],
        slope_score=score_breakdown["slope_score"],
        liquidity_score=score_breakdown["liquidity_score"],
        spread_penalty=score_breakdown["spread_penalty"],
        reversal_penalty=score_breakdown["reversal_penalty"],
        final_score=score_breakdown["final_score"],
        directional_delta=score_breakdown["directional_delta"],
        slope_value=score_breakdown["slope_value"],
        persistence_ratio=score_breakdown["persistence_ratio"],
        evaluated_snapshot_count=len(recent_snapshots),
        elapsed_seconds=round(elapsed_seconds, SCORE_ROUND_DECIMALS),
        confirmation_reason=confirmation_reason,
        invalidation_reason=invalidation_reason,
        regime_analysis=regime_analysis,
    )
