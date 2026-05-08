from __future__ import annotations

"""Centralized configuration for confirmation scoring and lifecycle decisions."""

# Lifecycle statuses used by the confirmation state machine.
TERMINAL_STATUSES = {"CONFIRMED", "INVALIDATED", "EXPIRED"}
# Allowed directional labels for signal movement logic.
VALID_DIRECTIONS = {"UP", "DOWN"}

# Number of recent snapshots used for confirmation scoring window.
CONFIRMATION_RECENT_WINDOW_SIZE = 8
# Minimum snapshots required in window before score is considered valid.
CONFIRMATION_MIN_REQUIRED_SNAPSHOTS = 3
# Hard guardrail for snapshots required before CONFIRMED is possible.
MIN_CONFIRMATION_SNAPSHOTS = 3
# Hard guardrail for elapsed seconds before CONFIRMED is possible.
MIN_CONFIRMATION_ELAPSED_SECONDS = 120.0
# Minimum points required to compute step-based metrics.
MIN_SNAPSHOTS_FOR_STEP_METRICS = 2

# Final score threshold to transition into CONFIRMED.
CONFIRMATION_THRESHOLD = 0.65
# Final score lower bound that forces INVALIDATED.
INVALIDATION_SCORE = -0.15
# Minimum persistence component required for CONFIRMED.
MIN_PERSISTENCE_FOR_CONFIRMATION = 0.5
# Minimum continuation component required for CONFIRMED.
MIN_CONTINUATION_FOR_CONFIRMATION = 0.5

# Scale used to normalize directional movement strength.
PRICE_MOVE_SCALE = 0.03
# Relative liquidity floor vs. reference liquidity.
MIN_LIQUIDITY_RATIO = 0.5
# Relative liquidity level considered healthy vs. reference.
GOOD_LIQUIDITY_RATIO = 1.0
# Relative spread multiplier above which spread is considered degraded.
MAX_SPREAD_MULTIPLIER = 2.0
# Absolute fallback liquidity floor when reference liquidity is unavailable.
LIQUIDITY_ABS_FLOOR = 1000.0
# Absolute fallback liquidity ceiling when reference liquidity is unavailable.
LIQUIDITY_ABS_CEILING = 50000.0
# Absolute fallback spread floor when reference spread is unavailable.
SPREAD_ABS_FLOOR = 0.02
# Absolute fallback spread ceiling when reference spread is unavailable.
SPREAD_ABS_CEILING = 0.12

# Weight for continuation component in final score.
CONTINUATION_SCORE_WEIGHT = 0.20
# Weight for persistence component in final score.
PERSISTENCE_SCORE_WEIGHT = 0.20
# Weight for slope component in final score.
SLOPE_SCORE_WEIGHT = 0.10
# Weight for liquidity component in final score.
LIQUIDITY_SCORE_WEIGHT = 0.15
# Weight for reversal penalty in final score.
REVERSAL_PENALTY_WEIGHT = 0.15
# Weight for spread penalty in final score.
SPREAD_PENALTY_WEIGHT = 0.10
# Weight for directional move component in final score.
PRICE_MOVE_SCORE_WEIGHT = 0.30

# Slope value mapped to minimum normalized slope score.
MIN_SLOPE_VALUE = -0.02
# Slope value mapped to maximum normalized slope score.
MAX_SLOPE_NORMALIZATION_VALUE = 0.02
# Penalty strength for slope instability (spiky/whipsaw steps).
SLOPE_VOLATILITY_PENALTY_WEIGHT = 0.50

# Minimum step magnitude to include in reversal analysis (filters micro-noise).
REVERSAL_MIN_MOVE_THRESHOLD = 0.002
# Weight of alternating sign flips in reversal penalty.
REVERSAL_ALTERNATION_WEIGHT = 0.35
# Weight of adverse counter-move magnitude in reversal penalty.
REVERSAL_COUNTER_MOVE_WEIGHT = 0.45
# Weight of adverse-step frequency in reversal penalty.
REVERSAL_NOISE_WEIGHT = 0.20
# Normalization divisor for counter-move magnitude aggregation.
REVERSAL_COUNTER_NORMALIZATION_FACTOR = 2.0

# Market regime classification thresholds and adaptation controls.
REGIME_VOLATILITY_THRESHOLD = 0.025
REGIME_MIN_LIQUIDITY = 5000.0
REGIME_MAX_SPREAD_RATIO = 1.8
REGIME_DIRECTIONAL_CONSISTENCY_THRESHOLD = 0.60
REGIME_INSTABILITY_THRESHOLD = 0.55
REGIME_TRENDING_THRESHOLD_DELTA = 0.05
REGIME_RANGING_THRESHOLD_DELTA = 0.05
REGIME_PERSISTENCE_RANGING_BOOST = 0.10
REGIME_REVERSAL_VOLATILE_BOOST = 0.10

# Global lower bound for normalized scores.
SCORE_MIN_VALUE = 0.0
# Global upper bound for normalized scores.
SCORE_MAX_VALUE = 1.0
# Shared zero value for float-safe defaults.
DEFAULT_ZERO_VALUE = 0.0
# Decimal precision used when returning score breakdown values.
SCORE_ROUND_DECIMALS = 6
# Enables verbose structured debug logs for every confirmation evaluation.
ENABLE_CONFIRMATION_DEBUG_LOGGING = False


DEFAULT_CONFIRMATION_CONFIG: dict[str, float | int | bool] = {
    "confirmation_threshold": CONFIRMATION_THRESHOLD,
    "invalidation_score": INVALIDATION_SCORE,
    "min_liquidity_ratio": MIN_LIQUIDITY_RATIO,
    "good_liquidity_ratio": GOOD_LIQUIDITY_RATIO,
    "max_spread_multiplier": MAX_SPREAD_MULTIPLIER,
    "price_move_scale": PRICE_MOVE_SCALE,
    "price_move_score_weight": PRICE_MOVE_SCORE_WEIGHT,
    "w_persistence": PERSISTENCE_SCORE_WEIGHT,
    "w_continuation": CONTINUATION_SCORE_WEIGHT,
    "w_slope": SLOPE_SCORE_WEIGHT,
    "w_liquidity": LIQUIDITY_SCORE_WEIGHT,
    "w_spread_penalty": SPREAD_PENALTY_WEIGHT,
    "w_reversal_penalty": REVERSAL_PENALTY_WEIGHT,
    "confirmation_recent_window_size": CONFIRMATION_RECENT_WINDOW_SIZE,
    "confirmation_min_required_snapshots": CONFIRMATION_MIN_REQUIRED_SNAPSHOTS,
    "min_confirmation_snapshots": MIN_CONFIRMATION_SNAPSHOTS,
    "min_confirmation_elapsed_seconds": MIN_CONFIRMATION_ELAPSED_SECONDS,
    "min_persistence_for_confirmation": MIN_PERSISTENCE_FOR_CONFIRMATION,
    "min_continuation_for_confirmation": MIN_CONTINUATION_FOR_CONFIRMATION,
    "liquidity_abs_floor": LIQUIDITY_ABS_FLOOR,
    "liquidity_abs_ceiling": LIQUIDITY_ABS_CEILING,
    "spread_abs_floor": SPREAD_ABS_FLOOR,
    "spread_abs_ceiling": SPREAD_ABS_CEILING,
    "enable_confirmation_debug_logging": ENABLE_CONFIRMATION_DEBUG_LOGGING,
    "regime_volatility_threshold": REGIME_VOLATILITY_THRESHOLD,
    "regime_min_liquidity": REGIME_MIN_LIQUIDITY,
    "regime_max_spread_ratio": REGIME_MAX_SPREAD_RATIO,
    "regime_directional_consistency_threshold": REGIME_DIRECTIONAL_CONSISTENCY_THRESHOLD,
    "regime_instability_threshold": REGIME_INSTABILITY_THRESHOLD,
    "regime_trending_threshold_delta": REGIME_TRENDING_THRESHOLD_DELTA,
    "regime_ranging_threshold_delta": REGIME_RANGING_THRESHOLD_DELTA,
    "regime_persistence_ranging_boost": REGIME_PERSISTENCE_RANGING_BOOST,
    "regime_reversal_volatile_boost": REGIME_REVERSAL_VOLATILE_BOOST,
}
