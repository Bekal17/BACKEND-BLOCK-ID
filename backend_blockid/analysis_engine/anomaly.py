"""
Rule-based anomaly detection for wallet behavior.

Flags burst transactions, suspicious velocity, and fresh-wallet high value.
Fully explainable: each flag has a rule name, severity, and human-readable
reason. No ML; thresholds are configurable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from backend_blockid.analysis_engine.features import WalletFeatureVector
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)


class AnomalySeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AnomalyType(str, Enum):
    BURST_TRANSACTIONS = "burst_transactions"
    SUSPICIOUS_VELOCITY = "suspicious_velocity"
    FRESH_WALLET_HIGH_VALUE = "fresh_wallet_high_value"


@dataclass
class AnomalyFlag:
    """
    Single explainable anomaly flag.

    Every flag is tied to a rule and includes the exact reason
    (threshold vs actual) so downstream and users can interpret it.
    """

    type: AnomalyType
    severity: AnomalySeverity
    message: str
    """Human-readable explanation of why this was flagged."""
    rule_name: str
    """Rule identifier for logging and tuning."""
    details: dict[str, Any] = field(default_factory=dict)
    """Thresholds and actual values used; for auditing and explainability."""

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "type": self.type.value,
            "severity": self.severity.value,
            "message": self.message,
            "rule_name": self.rule_name,
            "details": self.details,
        }
        return out


@dataclass
class AnomalyResult:
    """
    Result of rule-based anomaly detection for one wallet.

    Contains zero or more flags and a summary; all flags are explainable.
    """

    wallet: str
    flags: list[AnomalyFlag]
    is_anomalous: bool
    """True if at least one flag was raised."""

    @property
    def max_severity(self) -> AnomalySeverity | None:
        """Highest severity among flags; None if no flags."""
        if not self.flags:
            return None
        order = (
            AnomalySeverity.LOW,
            AnomalySeverity.MEDIUM,
            AnomalySeverity.HIGH,
            AnomalySeverity.CRITICAL,
        )
        return max(self.flags, key=lambda f: order.index(f.severity)).severity

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "flags": [f.to_dict() for f in self.flags],
            "is_anomalous": self.is_anomalous,
            "max_severity": self.max_severity.value if self.max_severity else None,
        }


@dataclass
class AnomalyConfig:
    """
    Configurable thresholds for anomaly rules.

    Tune these per environment; all numeric thresholds use SOL or
    counts per day unless noted.
    """

    # Burst: tx_frequency (txs per day) above this triggers.
    burst_tx_frequency_per_day: float = 100.0
    burst_severity_medium_per_day: float = 50.0
    burst_severity_low_per_day: float = 20.0

    # Velocity: SOL per day above this triggers.
    suspicious_velocity_sol_per_day: float = 500.0
    velocity_severity_medium_sol_per_day: float = 200.0
    velocity_severity_low_sol_per_day: float = 50.0

    # Fresh wallet: max tx_count to consider "fresh".
    fresh_wallet_max_tx_count: int = 5
    # Min SOL (total_volume or avg per tx) to flag when fresh.
    fresh_wallet_min_sol: float = 10.0
    fresh_wallet_high_sol_critical: float = 100.0
    fresh_wallet_high_sol_high: float = 50.0


def _check_burst(features: WalletFeatureVector, config: AnomalyConfig) -> AnomalyFlag | None:
    """
    Flag burst transaction activity: unusually high tx frequency (txs per day).
    """
    freq = features.tx_frequency
    if freq is None or features.tx_count < 2:
        return None
    if freq >= config.burst_tx_frequency_per_day:
        return AnomalyFlag(
            type=AnomalyType.BURST_TRANSACTIONS,
            severity=AnomalySeverity.CRITICAL,
            message=(
                f"Burst activity: {freq:.1f} transactions per day "
                f"(threshold: {config.burst_tx_frequency_per_day})"
            ),
            rule_name="burst_tx_frequency_per_day",
            details={
                "tx_frequency_per_day": round(freq, 2),
                "threshold": config.burst_tx_frequency_per_day,
                "tx_count": features.tx_count,
                "time_span_days": features.time_span_days,
            },
        )
    if freq >= config.burst_severity_medium_per_day:
        return AnomalyFlag(
            type=AnomalyType.BURST_TRANSACTIONS,
            severity=AnomalySeverity.HIGH,
            message=(
                f"Elevated transaction frequency: {freq:.1f} txs/day "
                f"(threshold: {config.burst_severity_medium_per_day})"
            ),
            rule_name="burst_tx_frequency_per_day",
            details={
                "tx_frequency_per_day": round(freq, 2),
                "threshold": config.burst_severity_medium_per_day,
                "tx_count": features.tx_count,
                "time_span_days": features.time_span_days,
            },
        )
    if freq >= config.burst_severity_low_per_day:
        return AnomalyFlag(
            type=AnomalyType.BURST_TRANSACTIONS,
            severity=AnomalySeverity.MEDIUM,
            message=(
                f"Above-normal transaction frequency: {freq:.1f} txs/day "
                f"(threshold: {config.burst_severity_low_per_day})"
            ),
            rule_name="burst_tx_frequency_per_day",
            details={
                "tx_frequency_per_day": round(freq, 2),
                "threshold": config.burst_severity_low_per_day,
                "tx_count": features.tx_count,
                "time_span_days": features.time_span_days,
            },
        )
    return None


def _check_suspicious_velocity(
    features: WalletFeatureVector,
    config: AnomalyConfig,
) -> AnomalyFlag | None:
    """
    Flag suspicious velocity: unusually high volume per day (SOL/day).
    """
    vel = features.velocity_sol_per_day
    if vel is None:
        return None
    if vel >= config.suspicious_velocity_sol_per_day:
        return AnomalyFlag(
            type=AnomalyType.SUSPICIOUS_VELOCITY,
            severity=AnomalySeverity.CRITICAL,
            message=(
                f"Suspicious velocity: {vel:.2f} SOL/day "
                f"(threshold: {config.suspicious_velocity_sol_per_day} SOL/day)"
            ),
            rule_name="suspicious_velocity_sol_per_day",
            details={
                "velocity_sol_per_day": round(vel, 4),
                "threshold": config.suspicious_velocity_sol_per_day,
                "total_volume_sol": features.total_volume_sol,
                "time_span_days": features.time_span_days,
            },
        )
    if vel >= config.velocity_severity_medium_sol_per_day:
        return AnomalyFlag(
            type=AnomalyType.SUSPICIOUS_VELOCITY,
            severity=AnomalySeverity.HIGH,
            message=(
                f"Elevated velocity: {vel:.2f} SOL/day "
                f"(threshold: {config.velocity_severity_medium_sol_per_day} SOL/day)"
            ),
            rule_name="suspicious_velocity_sol_per_day",
            details={
                "velocity_sol_per_day": round(vel, 4),
                "threshold": config.velocity_severity_medium_sol_per_day,
                "total_volume_sol": features.total_volume_sol,
                "time_span_days": features.time_span_days,
            },
        )
    if vel >= config.velocity_severity_low_sol_per_day:
        return AnomalyFlag(
            type=AnomalyType.SUSPICIOUS_VELOCITY,
            severity=AnomalySeverity.MEDIUM,
            message=(
                f"Above-normal velocity: {vel:.2f} SOL/day "
                f"(threshold: {config.velocity_severity_low_sol_per_day} SOL/day)"
            ),
            rule_name="suspicious_velocity_sol_per_day",
            details={
                "velocity_sol_per_day": round(vel, 4),
                "threshold": config.velocity_severity_low_sol_per_day,
                "total_volume_sol": features.total_volume_sol,
                "time_span_days": features.time_span_days,
            },
        )
    return None


def _check_fresh_wallet_high_value(
    features: WalletFeatureVector,
    config: AnomalyConfig,
) -> AnomalyFlag | None:
    """
    Flag fresh wallet moving high value: low tx count + high volume or high avg tx.
    """
    if features.tx_count > config.fresh_wallet_max_tx_count:
        return None
    if features.tx_count == 0:
        return None
    total_sol = features.total_volume_sol
    avg_sol = features.avg_transaction_value_sol
    value_sol = max(total_sol, avg_sol)
    if value_sol < config.fresh_wallet_min_sol:
        return None
    if value_sol >= config.fresh_wallet_high_sol_critical:
        return AnomalyFlag(
            type=AnomalyType.FRESH_WALLET_HIGH_VALUE,
            severity=AnomalySeverity.CRITICAL,
            message=(
                f"Fresh wallet ({features.tx_count} txs) moving high value: "
                f"{value_sol:.2f} SOL (threshold: {config.fresh_wallet_high_sol_critical} SOL)"
            ),
            rule_name="fresh_wallet_high_value",
            details={
                "tx_count": features.tx_count,
                "total_volume_sol": round(total_sol, 4),
                "avg_transaction_value_sol": round(avg_sol, 4),
                "value_used_sol": round(value_sol, 4),
                "fresh_wallet_max_tx_count": config.fresh_wallet_max_tx_count,
                "threshold_critical": config.fresh_wallet_high_sol_critical,
            },
        )
    if value_sol >= config.fresh_wallet_high_sol_high:
        return AnomalyFlag(
            type=AnomalyType.FRESH_WALLET_HIGH_VALUE,
            severity=AnomalySeverity.HIGH,
            message=(
                f"Fresh wallet ({features.tx_count} txs) with elevated value: "
                f"{value_sol:.2f} SOL (threshold: {config.fresh_wallet_high_sol_high} SOL)"
            ),
            rule_name="fresh_wallet_high_value",
            details={
                "tx_count": features.tx_count,
                "total_volume_sol": round(total_sol, 4),
                "avg_transaction_value_sol": round(avg_sol, 4),
                "value_used_sol": round(value_sol, 4),
                "fresh_wallet_max_tx_count": config.fresh_wallet_max_tx_count,
                "threshold_high": config.fresh_wallet_high_sol_high,
            },
        )
    return AnomalyFlag(
        type=AnomalyType.FRESH_WALLET_HIGH_VALUE,
        severity=AnomalySeverity.MEDIUM,
        message=(
            f"Fresh wallet ({features.tx_count} txs) with notable value: "
            f"{value_sol:.2f} SOL (threshold: {config.fresh_wallet_min_sol} SOL)"
        ),
        rule_name="fresh_wallet_high_value",
        details={
            "tx_count": features.tx_count,
            "total_volume_sol": round(total_sol, 4),
            "avg_transaction_value_sol": round(avg_sol, 4),
            "value_used_sol": round(value_sol, 4),
            "fresh_wallet_max_tx_count": config.fresh_wallet_max_tx_count,
            "threshold_min": config.fresh_wallet_min_sol,
        },
    )


def detect_anomalies(
    features: WalletFeatureVector,
    config: AnomalyConfig | None = None,
) -> AnomalyResult:
    """
    Run all rule-based anomaly checks on a wallet feature vector.

    Each rule is independent and explainable: flags include message and
    details (threshold vs actual). No ML; thresholds come from config.

    Args:
        features: Behavioral feature vector (e.g. from extract_features).
        config: Thresholds for each rule; uses defaults if None.

    Returns:
        AnomalyResult with flags list and is_anomalous; all flags explainable.
    """
    cfg = config or AnomalyConfig()
    flags: list[AnomalyFlag] = []

    for check in (_check_burst, _check_suspicious_velocity, _check_fresh_wallet_high_value):
        try:
            flag = check(features, cfg)
            if flag is not None:
                flags.append(flag)
        except Exception as e:
            logger.warning(
                "anomaly_rule_failed",
                rule=check.__name__,
                error=str(e),
            )

    return AnomalyResult(
        wallet=features.wallet,
        flags=flags,
        is_anomalous=len(flags) > 0,
    )
