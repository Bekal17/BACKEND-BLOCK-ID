"""
Alert engine — risk thresholds, anomaly-to-alert severity, storage with dedup.
Escalation intelligence — per-wallet state machine, risk_stage (normal/warning/critical).
"""

from backend_blockid.alerts.engine import (
    AlertConfig,
    evaluate_and_store_alerts,
)
from backend_blockid.alerts.escalation import (
    EscalationConfig,
    update_escalation_and_get_risk_stage,
)

__all__ = [
    "AlertConfig",
    "EscalationConfig",
    "evaluate_and_store_alerts",
    "update_escalation_and_get_risk_stage",
]
