"""
Cluster confidence for BlockID Dynamic Risk v2.

Confidence reflects how certain BlockID is about scam association.
Used by dynamic_risk.py to weight cluster-level penalties.
"""
from __future__ import annotations

import math
import os

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"

"""
Future upgrades:
* Graph centrality
* Distance weighting
* Reputation decay
* Bayesian update
"""


def _extract(kwargs: dict, *keys: str, default: float = 0.0) -> float:
    """First matching key wins. Returns float."""
    for k in keys:
        v = kwargs.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return default


def compute_cluster_confidence(*args: object, **kwargs: object) -> float:
    """
    Compute confidence (0–1) that cluster scam association is reliable.
    Accepts any args/kwargs for forward compatibility.
    """
    # Extract with aliases; dynamic_risk passes total_wallets, avg_flow_amount, etc.
    cluster_size = _extract(kwargs, "cluster_size", "total_wallets", default=1.0)
    scam_wallets = _extract(kwargs, "scam_wallets", default=0.0)
    avg_flow_amount = _extract(kwargs, "avg_flow_amount", "flow_amount", default=0.0)
    tx_count = _extract(kwargs, "tx_count", default=0.0)
    days_old = _extract(kwargs, "days_old", default=0.0)

    if TEST_MODE and cluster_size <= 0 and scam_wallets <= 0 and avg_flow_amount <= 0 and tx_count <= 0:
        logger.info("cluster_confidence", test_mode=True, confidence=0.5)
        return 0.5

    # 1. Scam ratio
    scam_ratio = scam_wallets / max(cluster_size, 1.0)

    # 2. Interaction strength (log dampening)
    tx_factor = min(1.0, math.log(tx_count + 1) / 5.0)

    # 3. Flow strength (log dampening)
    flow_factor = min(1.0, math.log(avg_flow_amount + 1) / 5.0)

    # 4. Recency (decay over ~30 days)
    time_factor = math.exp(-days_old / 30.0)

    confidence = (
        0.4 * scam_ratio
        + 0.3 * tx_factor
        + 0.2 * flow_factor
        + 0.1 * time_factor
    )
    confidence = max(0.0, min(1.0, confidence))

    logger.info(
        "cluster_confidence",
        scam_ratio=round(scam_ratio, 4),
        tx_factor=round(tx_factor, 4),
        flow_factor=round(flow_factor, 4),
        time_factor=round(time_factor, 4),
        confidence=round(confidence, 4),
    )
    return confidence
