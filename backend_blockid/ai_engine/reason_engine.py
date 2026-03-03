"""
Reason engine — helpers for building reason dicts with time context (days_old).

Used wherever reason codes are created from tx or cluster data.
"""
from __future__ import annotations

from typing import Any

from backend_blockid.tools.time_utils import days_since


def extract_tx_time(tx: dict[str, Any] | None) -> int | None:
    """
    Extract tx timestamp from Helius or RPC.
    Returns unix seconds or None.
    """
    if not tx or not isinstance(tx, dict):
        return None
    ts = tx.get("timestamp")
    if ts is not None:
        try:
            return int(float(ts))
        except (TypeError, ValueError):
            pass
    ts = tx.get("blockTime")
    if ts is not None:
        try:
            return int(float(ts))
        except (TypeError, ValueError):
            pass
    return None


def days_old_from_tx(tx: dict[str, Any] | None) -> int:
    """
    Compute days_old from tx timestamp.
    Returns 0 if no timestamp (treat as recent for security).
    """
    ts = extract_tx_time(tx)
    return days_since(ts) if ts is not None else 0


def days_old_from_cluster(
    cluster: dict[str, Any] | None,
    wallet_meta: dict[str, Any] | None = None,
) -> int:
    """
    Compute days_old from cluster timestamp (e.g. first_seen_ts).
    Fallback to wallet_meta.get("first_tx_ts") if cluster has no timestamp.
    Returns 0 if no timestamp (treat as recent).
    """
    cluster_time = None
    if cluster and isinstance(cluster, dict):
        cluster_time = cluster.get("first_seen_ts")
    if cluster_time is None and wallet_meta and isinstance(wallet_meta, dict):
        cluster_time = wallet_meta.get("first_tx_ts")
    if cluster_time is None:
        return 0
    try:
        return days_since(int(float(cluster_time)))
    except (TypeError, ValueError):
        return 0


def build_scam_cluster_reason(
    cluster: dict[str, Any] | None = None,
    wallet_meta: dict[str, Any] | None = None,
    *,
    weight: int = -30,
    confidence: float = 0.9,
    tx_hash: str | None = None,
) -> dict[str, Any]:
    """
    Build SCAM_CLUSTER_MEMBER reason with correct days_old.
    Uses cluster.first_seen_ts, fallback to wallet_meta.first_tx_ts.
    """
    return {
        "code": "SCAM_CLUSTER_MEMBER",
        "weight": weight,
        "confidence": confidence,
        "tx_hash": tx_hash,
        "days_old": days_old_from_cluster(cluster, wallet_meta),
    }
