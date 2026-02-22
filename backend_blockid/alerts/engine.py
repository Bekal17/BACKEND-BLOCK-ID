"""
Alert engine: risk thresholds, anomaly-to-alert severity, store with dedup.

Defines when to trigger alerts (trust score below threshold, anomaly severity
above minimum). Converts anomaly flags into alert severity; stores in DB with
timestamp and cooldown-based deduplication to prevent duplicate alerts.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

from backend_blockid.analysis_engine.anomaly import AnomalyResult, AnomalySeverity
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# Alert severity = same as anomaly severity for 1:1 mapping; configurable if needed
ALERT_SEVERITY_FROM_ANOMALY = {
    AnomalySeverity.LOW: "low",
    AnomalySeverity.MEDIUM: "medium",
    AnomalySeverity.HIGH: "high",
    AnomalySeverity.CRITICAL: "critical",
}

# Minimum anomaly severity to emit an alert (below this we don't store)
DEFAULT_ANOMALY_SEVERITY_MIN = AnomalySeverity.MEDIUM
# Trust score below this triggers a "risk_score" alert
DEFAULT_TRUST_SCORE_ALERT_BELOW = 50.0
# Cooldown: don't store same (wallet, severity, reason) within this many seconds
DEFAULT_ALERT_COOLDOWN_SEC = 3600
# Max reason length stored (truncate for DB)
MAX_REASON_LENGTH = 500


@dataclass
class AlertConfig:
    """Configurable risk thresholds and dedup for the alert engine."""

    trust_score_alert_below: float = DEFAULT_TRUST_SCORE_ALERT_BELOW
    """Trigger a risk_score alert when trust score is below this."""
    anomaly_severity_min: AnomalySeverity = DEFAULT_ANOMALY_SEVERITY_MIN
    """Only emit alerts for anomaly flags with severity >= this."""
    cooldown_sec: int = DEFAULT_ALERT_COOLDOWN_SEC
    """Don't store duplicate (wallet, severity, reason) within this window."""


def _reason_truncate(reason: str) -> str:
    if len(reason) <= MAX_REASON_LENGTH:
        return reason
    return reason[: MAX_REASON_LENGTH - 3] + "..."


def _should_alert_for_anomaly(flag_severity: AnomalySeverity, config: AlertConfig) -> bool:
    order = (
        AnomalySeverity.LOW,
        AnomalySeverity.MEDIUM,
        AnomalySeverity.HIGH,
        AnomalySeverity.CRITICAL,
    )
    return order.index(flag_severity) >= order.index(config.anomaly_severity_min)


def evaluate_and_store_alerts(
    wallet: str,
    trust_score: float,
    anomaly_result: AnomalyResult,
    db: Any,
    config: AlertConfig | None = None,
) -> int:
    """
    Evaluate risk: if trust score below threshold or anomaly flags above minimum
    severity, store alerts in DB. Deduplicates by (wallet, severity, reason)
    within cooldown_sec. Returns number of new alerts stored.

    Agent should call this after computing trust score and anomaly result.
    """
    cfg = config or AlertConfig()
    now = int(time.time())
    since = now - cfg.cooldown_sec
    stored = 0

    # 1. Trust score below threshold
    if trust_score < cfg.trust_score_alert_below:
        severity = "risk_score"
        reason = _reason_truncate(
            f"Trust score below threshold: {trust_score:.1f} < {cfg.trust_score_alert_below}"
        )
        if not db.has_recent_alert(wallet, severity, reason, since):
            db.insert_alert(wallet, severity, reason, now)
            stored += 1
            logger.info(
                "alert_stored",
                wallet_id=wallet,
                severity=severity,
                reason=reason,
                trust_score=trust_score,
            )

    # 2. Anomaly flags above minimum severity
    for flag in anomaly_result.flags:
        if not _should_alert_for_anomaly(flag.severity, cfg):
            continue
        alert_severity = ALERT_SEVERITY_FROM_ANOMALY.get(flag.severity, flag.severity.value)
        reason = _reason_truncate(flag.message)
        if not db.has_recent_alert(wallet, alert_severity, reason, since):
            db.insert_alert(wallet, alert_severity, reason, now)
            stored += 1
            logger.info(
                "alert_stored",
                wallet_id=wallet,
                severity=alert_severity,
                reason=reason,
                anomaly_type=flag.type.value,
            )

    return stored
