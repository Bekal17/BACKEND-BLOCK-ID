"""
Analysis engine package — trust score and wallet behavior analysis.

Consumes normalized Solana transaction data, applies scoring rules and
optional ML models, and produces trust scores and risk signals for wallets.
"""

from backend_blockid.analysis_engine.features import (
    WalletFeatureVector,
    extract_features,
)
from backend_blockid.analysis_engine.anomaly import (
    AnomalyConfig,
    AnomalyFlag,
    AnomalyResult,
    AnomalySeverity,
    AnomalyType,
    detect_anomalies,
)
from backend_blockid.analysis_engine.reputation_memory import (
    ReputationState,
    update_reputation,
)
from backend_blockid.analysis_engine.graph import update_wallet_graph
from backend_blockid.analysis_engine.risk_propagation import (
    PropagationHit,
    propagate_risk,
)

__all__ = [
    "WalletFeatureVector",
    "extract_features",
    "AnomalyConfig",
    "AnomalyFlag",
    "AnomalyResult",
    "AnomalySeverity",
    "AnomalyType",
    "detect_anomalies",
    "ReputationState",
    "update_reputation",
    "update_wallet_graph",
    "PropagationHit",
    "propagate_risk",
]
