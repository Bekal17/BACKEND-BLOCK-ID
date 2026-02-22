"""
Behavioral memory engine: compute rolling stats, baseline, trend, reputation decay.

Compares current cycle vs historical baseline using rule-based thresholds.
Persists rolling stats in DB. No ML; statistical only.
"""

from __future__ import annotations

import json
import statistics
import time
from typing import Any

from backend_blockid.behavioral_memory.models import (
    RollingStats,
    TrendResult,
    TrendType,
)
from backend_blockid.database.models import WalletProfile
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

SECONDS_PER_DAY = 86400
# Minimum number of historical snapshots to compute a baseline
MIN_BASELINE_PERIODS = 2
# Score delta (points) to classify trend_up / trend_down
TREND_SCORE_DELTA = 5.0
# Ratio thresholds for behavioral shift: current/baseline outside [1/ratio, ratio] -> shift
BEHAVIORAL_SHIFT_RATIO = 2.0
# Reputation decay: after 90 days inactive, decay factor drops by 0.5
REPUTATION_DECAY_DAYS = 90.0
REPUTATION_DECAY_MAX = 0.5


def _compute_current_rolling_stats(
    db: Any,
    wallet: str,
    now_ts: int,
    window_days: int,
) -> RollingStats:
    """
    Compute rolling stats for wallet over [now_ts - window_days*86400, now_ts].
    Uses transactions, trust_scores timeline, and alerts. Deterministic.
    """
    since_ts = now_ts - window_days * SECONDS_PER_DAY
    until_ts = now_ts

    history = db.get_transaction_history(
        wallet,
        since_timestamp=since_ts,
        until_timestamp=until_ts,
        limit=50_000,
    )
    volume_lamports = sum(r.amount_lamports for r in history)
    tx_count = len(history)

    timeline = db.get_trust_score_timeline(
        wallet,
        since_timestamp=since_ts,
        until_timestamp=until_ts,
        limit=10_000,
    )
    scores: list[float] = []
    anomaly_count = 0
    for r in timeline:
        if r.score is not None:
            scores.append(r.score)
        if r.metadata_json:
            try:
                meta = json.loads(r.metadata_json)
                if meta.get("is_anomalous") is True:
                    anomaly_count += 1
            except (json.JSONDecodeError, TypeError):
                pass
    avg_trust_score = float(statistics.mean(scores)) if scores else None

    alert_count = db.get_alert_count(wallet, since_created_at=since_ts, until_created_at=until_ts)

    return RollingStats(
        wallet=wallet,
        period_end_ts=now_ts,
        window_days=window_days,
        volume_lamports=volume_lamports,
        tx_count=tx_count,
        anomaly_count=anomaly_count,
        avg_trust_score=round(avg_trust_score, 2) if avg_trust_score is not None else None,
        alert_count=alert_count,
    )


def _get_baseline(
    db: Any,
    wallet: str,
    window_days: int,
    num_periods: int = 8,
) -> RollingStats | None:
    """
    Return baseline (median of last num_periods snapshots) for wallet and window_days.
    None if insufficient history.
    """
    rows = db.get_wallet_rolling_stats_history(wallet, window_days, limit=num_periods)
    if len(rows) < MIN_BASELINE_PERIODS:
        return None
    volumes = [r[1] for r in rows]
    tx_counts = [r[2] for r in rows]
    anomaly_counts = [r[3] for r in rows]
    avg_scores = [r[4] for r in rows if r[4] is not None]
    alert_counts = [r[5] for r in rows]
    return RollingStats(
        wallet=wallet,
        period_end_ts=0,
        window_days=window_days,
        volume_lamports=int(statistics.median(volumes)),
        tx_count=int(statistics.median(tx_counts)),
        anomaly_count=int(statistics.median(anomaly_counts)),
        avg_trust_score=round(statistics.median(avg_scores), 2) if avg_scores else None,
        alert_count=int(statistics.median(alert_counts)),
    )


def _reputation_decay(profile: WalletProfile | None, now_ts: int) -> float:
    """
    Return decay factor in [0, 1]. 1 = no decay; lower = decay over inactivity.
    Rule: linear decay over REPUTATION_DECAY_DAYS up to REPUTATION_DECAY_MAX.
    """
    if profile is None:
        return 1.0
    last_seen = profile.last_seen_at
    days_inactive = (now_ts - last_seen) / SECONDS_PER_DAY
    if days_inactive <= 0:
        return 1.0
    if days_inactive >= REPUTATION_DECAY_DAYS:
        return max(0.0, 1.0 - REPUTATION_DECAY_MAX)
    return 1.0 - (days_inactive / REPUTATION_DECAY_DAYS) * REPUTATION_DECAY_MAX


def _compare_and_classify(
    current: RollingStats,
    baseline: RollingStats | None,
    reasons: list[str],
) -> tuple[TrendType, bool]:
    """
    Compare current vs baseline; return (trend, behavioral_shift_detected).
    Fills reasons with explainable strings.
    """
    behavioral_shift = False
    if baseline is None:
        reasons.append("no_baseline_insufficient_history")
        return TrendType.STABLE, False

    # Score trend (trust score)
    cur_score = current.avg_trust_score
    base_score = baseline.avg_trust_score
    if cur_score is not None and base_score is not None:
        delta = cur_score - base_score
        if delta >= TREND_SCORE_DELTA:
            reasons.append(f"trust_score_up_delta={round(delta, 2)}")
        elif delta <= -TREND_SCORE_DELTA:
            reasons.append(f"trust_score_down_delta={round(delta, 2)}")
        else:
            reasons.append(f"trust_score_stable_delta={round(delta, 2)}")

    # Behavioral shift: volume, tx_count, anomaly_count, alert_count ratios
    def _ratio(cur_val: int | float, base_val: int | float) -> float | None:
        if base_val is None or base_val == 0:
            return None
        if isinstance(cur_val, (int, float)) and isinstance(base_val, (int, float)):
            return (cur_val or 0) / (base_val or 1)
        return None

    base_vol = baseline.volume_lamports or 0
    base_tx = baseline.tx_count or 0
    base_anom = baseline.anomaly_count or 0
    base_alert = baseline.alert_count or 0
    vol_ratio = _ratio(current.volume_lamports, base_vol) if base_vol > 0 else None
    tx_ratio = _ratio(current.tx_count, base_tx) if base_tx > 0 else None
    anom_ratio = _ratio(current.anomaly_count, base_anom) if base_anom > 0 else _ratio(current.anomaly_count, 1)
    alert_ratio = _ratio(current.alert_count, base_alert) if base_alert > 0 else _ratio(current.alert_count, 1)

    if vol_ratio is not None and (vol_ratio >= BEHAVIORAL_SHIFT_RATIO or vol_ratio <= 1.0 / BEHAVIORAL_SHIFT_RATIO):
        behavioral_shift = True
        reasons.append(f"volume_ratio={round(vol_ratio, 2)}")
    if tx_ratio is not None and (tx_ratio >= BEHAVIORAL_SHIFT_RATIO or tx_ratio <= 1.0 / BEHAVIORAL_SHIFT_RATIO):
        behavioral_shift = True
        reasons.append(f"tx_count_ratio={round(tx_ratio, 2)}")
    if anom_ratio is not None and anom_ratio >= BEHAVIORAL_SHIFT_RATIO:
        behavioral_shift = True
        reasons.append(f"anomaly_count_ratio={round(anom_ratio, 2)}")
    if alert_ratio is not None and alert_ratio >= BEHAVIORAL_SHIFT_RATIO:
        behavioral_shift = True
        reasons.append(f"alert_count_ratio={round(alert_ratio, 2)}")

    if behavioral_shift:
        return TrendType.BEHAVIORAL_SHIFT_DETECTED, True

    if cur_score is not None and base_score is not None:
        delta = cur_score - base_score
        if delta >= TREND_SCORE_DELTA:
            return TrendType.TREND_UP, False
        if delta <= -TREND_SCORE_DELTA:
            return TrendType.TREND_DOWN, False
    return TrendType.STABLE, False


def update_and_get_trend(
    db: Any,
    wallet: str,
    current_trust_score: float,
    current_is_anomalous: bool,
    now_ts: int | None = None,
    profile: WalletProfile | None = None,
) -> TrendResult:
    """
    Compute current 7d/30d rolling stats, persist them, compare to baseline, return TrendResult.

    Agent should call this after computing trust score for the wallet. No API changes;
    this is used by the analysis pipeline (runner/runtime). Modular: only needs db + wallet
    + current score and anomaly flag; profile optional for reputation_decay.
    """
    now_ts = now_ts if now_ts is not None else int(time.time())
    reasons: list[str] = []

    current_7d = _compute_current_rolling_stats(db, wallet, now_ts, 7)
    current_30d = _compute_current_rolling_stats(db, wallet, now_ts, 30)

    baseline_7d = _get_baseline(db, wallet, 7)
    baseline_30d = _get_baseline(db, wallet, 30)

    db.insert_wallet_rolling_stats(
        wallet,
        period_end_ts=now_ts,
        window_days=7,
        volume_lamports=current_7d.volume_lamports,
        tx_count=current_7d.tx_count,
        anomaly_count=current_7d.anomaly_count,
        avg_trust_score=current_7d.avg_trust_score,
        alert_count=current_7d.alert_count,
    )
    db.insert_wallet_rolling_stats(
        wallet,
        period_end_ts=now_ts,
        window_days=30,
        volume_lamports=current_30d.volume_lamports,
        tx_count=current_30d.tx_count,
        anomaly_count=current_30d.anomaly_count,
        avg_trust_score=current_30d.avg_trust_score,
        alert_count=current_30d.alert_count,
    )

    baseline = baseline_30d if baseline_30d is not None else baseline_7d
    current = current_30d if baseline_30d is not None else current_7d
    trend, behavioral_shift = _compare_and_classify(current, baseline, reasons)
    if not reasons:
        reasons.append("first_baseline_or_stable")

    reputation_decay = _reputation_decay(profile, now_ts)
    result = TrendResult(
        trend=trend,
        behavioral_shift_detected=behavioral_shift,
        reasons=reasons,
        baseline_7d=baseline_7d,
        baseline_30d=baseline_30d,
        current_7d=current_7d,
        current_30d=current_30d,
        reputation_decay=round(reputation_decay, 4),
    )
    logger.debug(
        "behavioral_memory_trend",
        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
        trend=trend.value,
        behavioral_shift_detected=behavioral_shift,
        reputation_decay=result.reputation_decay,
    )
    return result
