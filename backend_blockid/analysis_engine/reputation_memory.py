"""
Reputation memory layer: historical trust intelligence.

Trust is not a single snapshot. This module stores and computes:
- Rolling averages (7d, 30d) from trust score history
- Trend: improving | stable | degrading (deterministic rules)
- Volatility (population std dev over 30d)
- Trust decay over inactivity

All deterministic and explainable. No ML. Pure statistical model.
"""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

SECONDS_PER_DAY = 86400
TREND_IMPROVING = "improving"
TREND_STABLE = "stable"
TREND_DEGRADING = "degrading"

# Trend: score delta vs avg_30d to classify (points)
TREND_DELTA_THRESHOLD = 3.0
# Decay: after 90 days inactive, decay factor drops by 0.5
DECAY_DAYS = 90.0
DECAY_MAX = 0.5
# Minimum scores in window to compute rolling avg (else None)
MIN_SCORES_FOR_AVG = 1
# Minimum scores for volatility (else None)
MIN_SCORES_FOR_VOLATILITY = 2


@dataclass
class ReputationState:
    """
    Reputation state for a wallet (deterministic, explainable).

    current_score: Latest trust score.
    avg_7d: Rolling average over last 7 days; None if insufficient history.
    avg_30d: Rolling average over last 30 days; None if insufficient history.
    trend: improving | stable | degrading (vs avg_30d or prior period).
    volatility: Population std dev of scores in 30d window; None if < 2 scores.
    decay_factor: In [0, 1]; 1 = no decay; applied over inactivity (effective = current_score * decay_factor).
    """

    current_score: float
    avg_7d: float | None
    avg_30d: float | None
    trend: str
    volatility: float | None
    decay_factor: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "current_score": self.current_score,
            "avg_7d": self.avg_7d,
            "avg_30d": self.avg_30d,
            "trend": self.trend,
            "volatility": self.volatility,
            "decay_factor": self.decay_factor,
        }


def _rolling_scores(
    db: Any,
    wallet: str,
    now_ts: int,
    window_days: int,
    limit: int = 10_000,
) -> list[float]:
    """Return list of trust scores in [now_ts - window_days*86400, now_ts], newest first."""
    since_ts = now_ts - window_days * SECONDS_PER_DAY
    timeline = db.get_trust_score_timeline(
        wallet,
        since_timestamp=since_ts,
        until_timestamp=now_ts,
        limit=limit,
    )
    return [float(r.score) for r in timeline if r.score is not None]


def _last_computed_at(db: Any, wallet: str, now_ts: int, window_days: int = 365) -> int | None:
    """Return most recent computed_at for wallet in the last window_days, or None."""
    since_ts = now_ts - window_days * SECONDS_PER_DAY
    timeline = db.get_trust_score_timeline(
        wallet,
        since_timestamp=since_ts,
        until_timestamp=now_ts,
        limit=1,
    )
    if not timeline:
        return None
    return timeline[0].computed_at


def _decay_factor(last_computed_at: int | None, now_ts: int) -> float:
    """
    Return decay factor in [0, 1]. 1 = no decay; decreases with inactivity.
    Rule: linear over DECAY_DAYS up to DECAY_MAX.
    """
    if last_computed_at is None:
        return 1.0
    days_inactive = (now_ts - last_computed_at) / SECONDS_PER_DAY
    if days_inactive <= 0:
        return 1.0
    if days_inactive >= DECAY_DAYS:
        return max(0.0, 1.0 - DECAY_MAX)
    return 1.0 - (days_inactive / DECAY_DAYS) * DECAY_MAX


def _classify_trend(
    current_score: float,
    avg_30d: float | None,
    avg_7d: float | None,
) -> str:
    """
    Classify trend: improving | stable | degrading (deterministic).
    Uses current vs avg_30d; if avg_30d missing, uses avg_7d; else stable.
    """
    ref = avg_30d if avg_30d is not None else avg_7d
    if ref is None:
        return TREND_STABLE
    delta = current_score - ref
    if delta >= TREND_DELTA_THRESHOLD:
        return TREND_IMPROVING
    if delta <= -TREND_DELTA_THRESHOLD:
        return TREND_DEGRADING
    return TREND_STABLE


def update_reputation(
    db: Any,
    wallet_id: str,
    new_score: float,
    now_ts: int | None = None,
) -> ReputationState:
    """
    Update reputation state from trust score history; persist and return state.

    Assumes the caller has already appended new_score to the trust score timeline
    (e.g. via insert_trust_score). Computes rolling 7d/30d averages from timeline,
    trend (improving/stable/degrading), volatility (std dev 30d), and decay over
    inactivity. Persists to wallet_reputation_state. Deterministic; no ML.
    """
    now_ts = now_ts if now_ts is not None else int(time.time())
    current_score = round(new_score, 2)

    scores_7d = _rolling_scores(db, wallet_id, now_ts, 7)
    scores_30d = _rolling_scores(db, wallet_id, now_ts, 30)

    avg_7d = round(statistics.mean(scores_7d), 2) if len(scores_7d) >= MIN_SCORES_FOR_AVG else None
    avg_30d = round(statistics.mean(scores_30d), 2) if len(scores_30d) >= MIN_SCORES_FOR_AVG else None

    volatility = None
    if len(scores_30d) >= MIN_SCORES_FOR_VOLATILITY:
        volatility = round(statistics.pstdev(scores_30d), 2)

    trend = _classify_trend(current_score, avg_30d, avg_7d)

    last_computed = _last_computed_at(db, wallet_id, now_ts)
    decay_factor = round(_decay_factor(last_computed, now_ts), 4)

    state = ReputationState(
        current_score=current_score,
        avg_7d=avg_7d,
        avg_30d=avg_30d,
        trend=trend,
        volatility=volatility,
        decay_factor=decay_factor,
    )

    db.upsert_wallet_reputation_state(
        wallet_id,
        current_score=state.current_score,
        avg_7d=state.avg_7d,
        avg_30d=state.avg_30d,
        trend=state.trend,
        volatility=state.volatility,
        decay_factor=state.decay_factor,
    )

    logger.debug(
        "reputation_updated",
        wallet_id=wallet_id[:16] + "..." if len(wallet_id) > 16 else wallet_id,
        current_score=state.current_score,
        avg_7d=state.avg_7d,
        avg_30d=state.avg_30d,
        trend=state.trend,
        volatility=state.volatility,
        decay_factor=state.decay_factor,
    )
    return state
