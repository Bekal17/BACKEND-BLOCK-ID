"""
Trust score computation — rules and aggregation.

Responsibilities:
- Compute trust scores from transaction history and wallet features.
- Aggregate signals (volume, frequency, counterparties, known scams, etc.).
- Output a numeric score and optional breakdown for storage and API exposure.
"""

from __future__ import annotations

from backend_blockid.analysis_engine.anomaly import AnomalyResult, AnomalySeverity
from backend_blockid.analysis_engine.features import WalletFeatureVector


# Deductions per anomaly severity (explainable, rule-based)
SEVERITY_PENALTY = {
    AnomalySeverity.CRITICAL: 25,
    AnomalySeverity.HIGH: 15,
    AnomalySeverity.MEDIUM: 8,
    AnomalySeverity.LOW: 3,
}


def compute_trust_score(
    features: WalletFeatureVector,
    anomaly_result: AnomalyResult,
    *,
    base_score: float = 100.0,
    min_score: float = 0.0,
    max_score: float = 100.0,
) -> float:
    """
    Compute a trust score (0–100) from features and anomaly flags.

    Starts at base_score and subtracts a fixed penalty per anomaly by severity.
    No ML; fully explainable. Does not use feature values directly; anomalies
    drive the deduction. Caller can pass breakdown in metadata.

    Args:
        features: Behavioral feature vector (used for metadata/breakdown only here).
        anomaly_result: Result of detect_anomalies.
        base_score: Starting score before penalties.
        min_score: Floor for the returned score.
        max_score: Ceiling for the returned score.

    Returns:
        Score in [min_score, max_score].
    """
    score = base_score
    for flag in anomaly_result.flags:
        penalty = SEVERITY_PENALTY.get(flag.severity, 0)
        score -= penalty
    return max(min_score, min(max_score, score))
