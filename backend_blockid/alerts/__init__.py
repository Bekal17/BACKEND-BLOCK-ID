"""
Alert engine — risk thresholds, anomaly-to-alert severity, storage with dedup.

Converts anomaly flags and trust score into alerts; stores in DB with
cooldown-based deduplication. Agent triggers when risk crosses threshold.
"""

from backend_blockid.alerts.engine import (
    AlertConfig,
    evaluate_and_store_alerts,
)

__all__ = [
    "AlertConfig",
    "evaluate_and_store_alerts",
]
