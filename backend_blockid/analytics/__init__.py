"""
BlockID Step 3 Analytics Engine.

Computes trust scores from on-chain wallet activity before publishing to the oracle.
Modules: wallet_scanner, risk_engine, trust_engine, analytics_pipeline.
"""

from backend_blockid.analytics.wallet_scanner import scan_wallet
from backend_blockid.analytics.risk_engine import calculate_risk
from backend_blockid.analytics.trust_engine import calculate_trust
from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis

__all__ = [
    "scan_wallet",
    "calculate_risk",
    "calculate_trust",
    "run_wallet_analysis",
]
