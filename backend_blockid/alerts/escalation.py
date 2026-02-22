"""
Alert escalation intelligence: per-wallet state machine.

Alerts accumulate per wallet. Rules (deterministic):
- Repeated anomaly increases severity (same type in recent window).
- Multiple anomaly types escalate risk (distinct types in current + recent).
- Time-clustered anomalies escalate faster (many alerts in short window).
- Long clean period reduces severity (no alerts for N hours → decay or reset).

Output: risk_stage = normal | warning | critical.
State persisted in DB (wallet_escalation_state).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.analysis_engine.anomaly import AnomalyResult
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# Risk stages (agent output)
RISK_STAGE_NORMAL = "normal"
RISK_STAGE_WARNING = "warning"
RISK_STAGE_CRITICAL = "critical"

# Score bands: 0–30 normal, 31–60 warning, 61+ critical
ESCALATION_SCORE_NORMAL_MAX = 30.0
ESCALATION_SCORE_WARNING_MAX = 60.0
# Max score cap
ESCALATION_SCORE_CAP = 100.0

# Time windows (seconds)
WINDOW_RECENT_ALERTS_SEC = 86400 * 2   # 2 days of alerts for accumulation
WINDOW_CLUSTER_SEC = 3600              # 1 hour: time-clustered if 3+ alerts in this window
WINDOW_CLEAN_REDUCE_SEC = 86400 * 1    # 24h clean: reduce score
WINDOW_CLEAN_RESET_SEC = 86400 * 2     # 48h clean + no current anomaly: reset to normal

# Points (additive)
POINTS_PER_CURRENT_FLAG = 6
POINTS_REPEATED_ANOMALY = 8            # same anomaly type seen again in recent
POINTS_MULTIPLE_TYPES = 12             # distinct anomaly types in current + recent
POINTS_TIME_CLUSTER = 15               # 3+ alerts in last 1h
DECAY_PER_24H_CLEAN = 12.0             # subtract when 24h clean
RESET_SCORE = 0.0

# Cluster threshold: alerts in WINDOW_CLUSTER_SEC to count as cluster
CLUSTER_ALERT_COUNT = 3


@dataclass
class EscalationConfig:
    """Configurable thresholds for escalation (all rule-based)."""

    window_recent_sec: int = WINDOW_RECENT_ALERTS_SEC
    window_cluster_sec: int = WINDOW_CLUSTER_SEC
    window_clean_reduce_sec: int = WINDOW_CLEAN_REDUCE_SEC
    window_clean_reset_sec: int = WINDOW_CLEAN_RESET_SEC
    score_normal_max: float = ESCALATION_SCORE_NORMAL_MAX
    score_warning_max: float = ESCALATION_SCORE_WARNING_MAX
    score_cap: float = ESCALATION_SCORE_CAP
    points_per_flag: float = POINTS_PER_CURRENT_FLAG
    points_repeated: float = POINTS_REPEATED_ANOMALY
    points_multiple_types: float = POINTS_MULTIPLE_TYPES
    points_cluster: float = POINTS_TIME_CLUSTER
    decay_per_clean: float = DECAY_PER_24H_CLEAN
    cluster_alert_count: int = CLUSTER_ALERT_COUNT


def _score_to_risk_stage(score: float, config: EscalationConfig) -> str:
    """Map escalation score to risk_stage."""
    if score <= config.score_normal_max:
        return RISK_STAGE_NORMAL
    if score <= config.score_warning_max:
        return RISK_STAGE_WARNING
    return RISK_STAGE_CRITICAL


def _extract_anomaly_type_from_reason(reason: str, severity: str) -> str:
    """
    Infer anomaly type from alert reason/severity for dedup.
    Returns a stable key (e.g. burst_transactions, risk_score).
    """
    r = (reason or "").lower()
    s = (severity or "").lower()
    if "burst" in r or "transaction" in r:
        return "burst_transactions"
    if "velocity" in r:
        return "suspicious_velocity"
    if "fresh" in r or "high value" in r:
        return "fresh_wallet_high_value"
    if "trust score" in r or s == "risk_score":
        return "risk_score"
    return f"other_{s}"


def update_escalation_and_get_risk_stage(
    db: Any,
    wallet: str,
    anomaly_result: AnomalyResult,
    now_ts: int | None = None,
    config: EscalationConfig | None = None,
) -> str:
    """
    Run escalation state machine: load state + recent alerts, apply rules, persist, return risk_stage.

    Agent should call this after evaluate_and_store_alerts. Returns one of: normal, warning, critical.
    """
    cfg = config or EscalationConfig()
    now_ts = now_ts if now_ts is not None else int(time.time())
    since = now_ts - cfg.window_recent_sec

    current_types = {f.type.value for f in anomaly_result.flags}
    current_flag_count = len(anomaly_result.flags)

    # Load current state
    state = db.get_escalation_state(wallet)
    if state is None:
        risk_stage = RISK_STAGE_NORMAL
        escalation_score = 0.0
        last_alert_ts = None
        last_clean_ts = now_ts
    else:
        risk_stage, escalation_score, last_alert_ts, last_clean_ts, _, _ = state

    # Recent alerts (accumulated per wallet)
    recent_alerts = db.get_alerts_for_wallet(wallet, since_created_at=since, until_created_at=now_ts, limit=200)
    recent_types = {_extract_anomaly_type_from_reason(r, s) for (_, s, r) in recent_alerts}
    repeated = current_types & recent_types
    all_types = current_types | recent_types
    cluster_since = now_ts - cfg.window_cluster_sec
    cluster_count = sum(1 for (ts, _, _) in recent_alerts if ts >= cluster_since)

    # Long clean period: reduce severity or reset
    if not anomaly_result.is_anomalous and not recent_alerts:
        # No current anomaly and no recent alerts
        if last_clean_ts is not None and (now_ts - last_clean_ts) >= cfg.window_clean_reset_sec:
            escalation_score = RESET_SCORE
            risk_stage = RISK_STAGE_NORMAL
            last_clean_ts = now_ts
            last_alert_ts = None
        else:
            last_clean_ts = now_ts
            if last_alert_ts is not None and (now_ts - last_alert_ts) >= cfg.window_clean_reduce_sec:
                escalation_score = max(0.0, escalation_score - cfg.decay_per_clean)
                risk_stage = _score_to_risk_stage(escalation_score, cfg)
    else:
        # Current cycle has anomalies or we have recent alerts
        if anomaly_result.is_anomalous:
            last_alert_ts = now_ts
        if last_clean_ts is not None and (now_ts - last_clean_ts) >= cfg.window_clean_reduce_sec and not anomaly_result.is_anomalous:
            escalation_score = max(0.0, escalation_score - cfg.decay_per_clean)

        # Add points: current flags
        escalation_score += current_flag_count * cfg.points_per_flag

        # Repeated anomaly: current types that appear in recent alerts
        if repeated:
            escalation_score += len(repeated) * cfg.points_repeated

        # Multiple anomaly types: distinct types in current + recent
        if len(all_types) >= 2:
            escalation_score += cfg.points_multiple_types

        # Time-clustered: 3+ alerts in last 1 hour
        if cluster_count >= cfg.cluster_alert_count:
            escalation_score += cfg.points_cluster

        escalation_score = min(escalation_score, cfg.score_cap)
        risk_stage = _score_to_risk_stage(escalation_score, cfg)

    state_json = json.dumps({
        "current_anomaly_types": list(current_types),
        "recent_alert_count": len(recent_alerts),
        "reasons": [
            "repeated" if repeated else None,
            "multiple_types" if len(all_types) >= 2 else None,
            "time_cluster" if cluster_count >= cfg.cluster_alert_count else None,
        ],
    })
    db.upsert_escalation_state(
        wallet,
        risk_stage,
        round(escalation_score, 2),
        last_alert_ts,
        last_clean_ts,
        state_json,
    )
    tier = risk_stage
    if risk_stage == "warning":
        tier = "watchlist"
    try:
        db.set_wallet_priority(wallet, tier)
    except Exception as e:
        logger.warning(
            "escalation_priority_persist_failed",
            wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
            error=str(e),
        )
    logger.info(
        "escalation_updated",
        wallet_id=wallet[:16] + "..." if len(wallet) > 16 else wallet,
        risk_stage=risk_stage,
        escalation_score=round(escalation_score, 2),
        current_flags=current_flag_count,
    )
    return risk_stage
