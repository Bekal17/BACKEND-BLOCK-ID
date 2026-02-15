"""
Data models for wallet behavioral memory.

Rolling stats (7d/30d), baseline, trend result, and reputation decay.
All deterministic; no ML.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TrendType(str, Enum):
    """Trend classification: current cycle vs historical baseline."""

    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    STABLE = "stable"
    BEHAVIORAL_SHIFT_DETECTED = "behavioral_shift_detected"


@dataclass
class RollingStats:
    """
    Rolling window stats for one wallet over a period (e.g. 7d or 30d).

    Used for current cycle and for historical baseline (median of past snapshots).
    """

    wallet: str
    period_end_ts: int
    window_days: int
    volume_lamports: int
    tx_count: int
    anomaly_count: int
    avg_trust_score: float | None
    alert_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "wallet": self.wallet,
            "period_end_ts": self.period_end_ts,
            "window_days": self.window_days,
            "volume_lamports": self.volume_lamports,
            "tx_count": self.tx_count,
            "anomaly_count": self.anomaly_count,
            "avg_trust_score": self.avg_trust_score,
            "alert_count": self.alert_count,
        }


@dataclass
class TrendResult:
    """
    Result of comparing current cycle vs historical baseline.

    trend: trend_up | trend_down | stable | behavioral_shift_detected.
    behavioral_shift_detected: True if volume/anomaly/alert ratio vs baseline exceeds thresholds.
    reasons: human-readable list of why this trend was chosen (explainable).
    reputation_decay: factor in [0, 1]; 1 = no decay, lower = decay over inactivity.
    """

    trend: TrendType
    behavioral_shift_detected: bool
    reasons: list[str]
    baseline_7d: RollingStats | None
    baseline_30d: RollingStats | None
    current_7d: RollingStats | None
    current_30d: RollingStats | None
    reputation_decay: float

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "trend": self.trend.value,
            "behavioral_shift_detected": self.behavioral_shift_detected,
            "reasons": self.reasons,
            "reputation_decay": self.reputation_decay,
        }
        if self.baseline_7d:
            out["baseline_7d"] = self.baseline_7d.to_dict()
        if self.baseline_30d:
            out["baseline_30d"] = self.baseline_30d.to_dict()
        if self.current_7d:
            out["current_7d"] = self.current_7d.to_dict()
        if self.current_30d:
            out["current_30d"] = self.current_30d.to_dict()
        return out
