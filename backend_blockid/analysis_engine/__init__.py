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

__all__ = [
    "WalletFeatureVector",
    "extract_features",
    "AnomalyConfig",
    "AnomalyFlag",
    "AnomalyResult",
    "AnomalySeverity",
    "AnomalyType",
    "detect_anomalies",
]
