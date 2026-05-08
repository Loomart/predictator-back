from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any

from confirmation_config import (
    DEFAULT_CONFIRMATION_CONFIG,
    DEFAULT_ZERO_VALUE,
    SCORE_ROUND_DECIMALS,
    REGIME_DIRECTIONAL_CONSISTENCY_THRESHOLD,
    REGIME_INSTABILITY_THRESHOLD,
    REGIME_MAX_SPREAD_RATIO,
    REGIME_MIN_LIQUIDITY,
    REGIME_VOLATILITY_THRESHOLD,
    SCORE_MAX_VALUE,
    SCORE_MIN_VALUE,
)


class MarketRegime(str, Enum):
    TRENDING = "TRENDING"
    RANGING = "RANGING"
    VOLATILE = "VOLATILE"
    ILLIQUID = "ILLIQUID"
    UNSTABLE = "UNSTABLE"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class MarketRegimeAnalysisResult:
    regime: MarketRegime
    volatility_score: float
    liquidity_score: float
    spread_score: float
    directional_consistency: float
    instability_score: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["regime"] = self.regime.value
        return payload


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


def _clamp(value: float) -> float:
    return max(SCORE_MIN_VALUE, min(SCORE_MAX_VALUE, value))


def _normalize_positive(value: float, floor: float, ceiling: float) -> float:
    if ceiling <= floor:
        return SCORE_MIN_VALUE
    if value <= floor:
        return SCORE_MIN_VALUE
    if value >= ceiling:
        return SCORE_MAX_VALUE
    return (value - floor) / (ceiling - floor)


def _normalize_inverse(value: float, floor: float, ceiling: float) -> float:
    return SCORE_MAX_VALUE - _normalize_positive(value, floor, ceiling)


def _extract_prices(snapshots: list[Any]) -> list[float]:
    prices = [_as_float(_get_attr(snapshot, "yes_price", None), float("nan")) for snapshot in snapshots]
    return [price for price in prices if price == price]


def _extract_spreads(snapshots: list[Any]) -> list[float]:
    spreads = [_as_float(_get_attr(snapshot, "spread", None), float("nan")) for snapshot in snapshots]
    return [spread for spread in spreads if spread == spread]


def _extract_liquidities(snapshots: list[Any]) -> list[float]:
    liquidities = [_as_float(_get_attr(snapshot, "liquidity", None), float("nan")) for snapshot in snapshots]
    return [liquidity for liquidity in liquidities if liquidity == liquidity]


def _directional_consistency(prices: list[float]) -> float:
    if len(prices) < 2:
        return SCORE_MIN_VALUE
    steps = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    positive = sum(1 for step in steps if step > DEFAULT_ZERO_VALUE)
    negative = sum(1 for step in steps if step < DEFAULT_ZERO_VALUE)
    dominant = max(positive, negative)
    return _clamp(dominant / len(steps))


def _instability_score(prices: list[float]) -> float:
    if len(prices) < 3:
        return SCORE_MIN_VALUE

    steps = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    flips = 0
    for i in range(1, len(steps)):
        prev = steps[i - 1]
        curr = steps[i]
        if prev == DEFAULT_ZERO_VALUE or curr == DEFAULT_ZERO_VALUE:
            continue
        if (prev > DEFAULT_ZERO_VALUE and curr < DEFAULT_ZERO_VALUE) or (
            prev < DEFAULT_ZERO_VALUE and curr > DEFAULT_ZERO_VALUE
        ):
            flips += 1

    max_flips = max(1, len(steps) - 1)
    return _clamp(flips / max_flips)


def _volatility_score(prices: list[float], volatility_threshold: float) -> float:
    if len(prices) < 2:
        return SCORE_MIN_VALUE
    steps = [abs(prices[i] - prices[i - 1]) for i in range(1, len(prices))]
    mean_abs_move = sum(steps) / len(steps)
    return _normalize_positive(
        mean_abs_move,
        volatility_threshold,
        volatility_threshold * REGIME_MAX_SPREAD_RATIO,
    )


def analyze_market_regime(snapshots: list[Any], config: dict[str, Any] | None = None) -> MarketRegimeAnalysisResult:
    """
    Classify market regime from recent snapshots.

    Uses volatility, spread quality, liquidity quality, directional consistency,
    and instability to determine whether the market is trending, ranging,
    volatile, illiquid, unstable, or unknown.
    """
    effective = {**DEFAULT_CONFIRMATION_CONFIG, **(config or {})}

    volatility_threshold = _as_float(
        effective.get("regime_volatility_threshold", REGIME_VOLATILITY_THRESHOLD),
        REGIME_VOLATILITY_THRESHOLD,
    )
    min_liquidity = _as_float(
        effective.get("regime_min_liquidity", REGIME_MIN_LIQUIDITY),
        REGIME_MIN_LIQUIDITY,
    )
    max_spread_ratio = _as_float(
        effective.get("regime_max_spread_ratio", REGIME_MAX_SPREAD_RATIO),
        REGIME_MAX_SPREAD_RATIO,
    )
    consistency_threshold = _as_float(
        effective.get(
            "regime_directional_consistency_threshold",
            REGIME_DIRECTIONAL_CONSISTENCY_THRESHOLD,
        ),
        REGIME_DIRECTIONAL_CONSISTENCY_THRESHOLD,
    )
    instability_threshold = _as_float(
        effective.get("regime_instability_threshold", REGIME_INSTABILITY_THRESHOLD),
        REGIME_INSTABILITY_THRESHOLD,
    )

    prices = _extract_prices(snapshots)
    spreads = _extract_spreads(snapshots)
    liquidities = _extract_liquidities(snapshots)

    if len(prices) < 2:
        return MarketRegimeAnalysisResult(
            regime=MarketRegime.UNKNOWN,
            volatility_score=SCORE_MIN_VALUE,
            liquidity_score=SCORE_MIN_VALUE,
            spread_score=SCORE_MIN_VALUE,
            directional_consistency=SCORE_MIN_VALUE,
            instability_score=SCORE_MIN_VALUE,
        )

    latest_spread = spreads[-1] if spreads else DEFAULT_ZERO_VALUE
    baseline_spread = spreads[0] if spreads and spreads[0] > DEFAULT_ZERO_VALUE else latest_spread
    spread_ratio = latest_spread / baseline_spread if baseline_spread > DEFAULT_ZERO_VALUE else max_spread_ratio

    latest_liquidity = liquidities[-1] if liquidities else DEFAULT_ZERO_VALUE

    volatility_score = _volatility_score(prices, volatility_threshold)
    liquidity_score = _normalize_positive(latest_liquidity, min_liquidity, min_liquidity * SCORE_MAX_VALUE * SCORE_MAX_VALUE)
    spread_score = _normalize_inverse(spread_ratio, SCORE_MAX_VALUE, max_spread_ratio)
    directional_consistency = _directional_consistency(prices)
    instability_score = _instability_score(prices)

    regime = MarketRegime.UNKNOWN
    if latest_liquidity < min_liquidity:
        regime = MarketRegime.ILLIQUID
    elif instability_score >= instability_threshold:
        regime = MarketRegime.UNSTABLE
    elif volatility_score >= SCORE_MAX_VALUE:
        regime = MarketRegime.VOLATILE
    elif directional_consistency >= consistency_threshold and spread_ratio <= max_spread_ratio:
        regime = MarketRegime.TRENDING
    else:
        regime = MarketRegime.RANGING

    return MarketRegimeAnalysisResult(
        regime=regime,
        volatility_score=round(volatility_score, SCORE_ROUND_DECIMALS),
        liquidity_score=round(liquidity_score, SCORE_ROUND_DECIMALS),
        spread_score=round(spread_score, SCORE_ROUND_DECIMALS),
        directional_consistency=round(directional_consistency, SCORE_ROUND_DECIMALS),
        instability_score=round(instability_score, SCORE_ROUND_DECIMALS),
    )
