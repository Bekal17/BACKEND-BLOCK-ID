"""
Risk propagation: propagate partial risk from anomalous neighbors through the wallet graph.

If a wallet has anomaly flags (stored in latest trust score metadata), neighbors within
max_depth hops receive a penalty. Risk decays by distance. Final score:
adjusted_score = base_score - propagated_risk_penalty.

Deterministic, explainable. No API change; runtime-only intelligence layer.
"""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass
from typing import Any

from backend_blockid.logging import get_logger

logger = get_logger(__name__)

MAX_DEPTH = 2
"""Max hop distance for propagation (1 = direct neighbors, 2 = neighbors of neighbors)."""

DECAY_PER_HOP = 0.5
"""Risk multiplier per hop: penalty at distance d = base_penalty * (DECAY_PER_HOP ** d)."""

BASE_PENALTY_PER_ANOMALOUS_NEIGHBOR = 6.0
"""Base penalty for one anomalous neighbor at distance 1; at d=2 it's BASE * DECAY^2."""

MAX_PROPAGATED_PENALTY = 20.0
"""Cap on total propagated penalty so one wallet's score is not destroyed by many bad neighbors."""


@dataclass
class PropagationHit:
    """Single propagation event: one risky neighbor affecting the scored wallet."""

    source_wallet: str
    affected_wallet: str
    hop_distance: int
    decay_factor: float
    penalty_applied: float


def _neighbors_up_to_hops(db: Any, wallet: str, max_hops: int) -> dict[str, int]:
    """Return {neighbor_wallet: hop_distance} for all wallets within max_hops (BFS)."""
    if max_hops < 1:
        return {}
    result: dict[str, int] = {}
    queue: deque[tuple[str, int]] = deque([(wallet.strip(), 0)])
    seen = {wallet.strip()}
    while queue:
        w, d = queue.popleft()
        if d >= max_hops:
            continue
        adjacent = db.get_wallet_graph_adjacent(w)
        for other in adjacent:
            if other in seen:
                continue
            seen.add(other)
            hop = d + 1
            result[other] = hop
            queue.append((other, hop))
    return result


def _is_anomalous_from_metadata(metadata_json: str | None) -> bool:
    """Parse trust score metadata_json; return True if is_anomalous is True."""
    if not metadata_json:
        return False
    try:
        meta = json.loads(metadata_json)
        return meta.get("is_anomalous") is True
    except (json.JSONDecodeError, TypeError):
        return False


def propagate_risk(
    db: Any,
    wallet_id: str,
    base_score: float,
    *,
    max_depth: int = MAX_DEPTH,
    decay: float = DECAY_PER_HOP,
    base_penalty: float = BASE_PENALTY_PER_ANOMALOUS_NEIGHBOR,
    max_penalty: float = MAX_PROPAGATED_PENALTY,
) -> float:
    """
    Compute adjusted trust score after propagating risk from anomalous neighbors.

    Finds all wallets within max_depth hops of wallet_id. For each neighbor that has
    is_anomalous=True in its latest trust score metadata, applies a penalty that decays
    by distance: penalty = base_penalty * (decay ** hop_distance). Sum of penalties
    is capped at max_penalty. adjusted_score = base_score - min(total_penalty, max_penalty).

    Logs each propagation event: source_wallet, affected_wallet, decay_factor, penalty_applied.

    Args:
        db: Database with get_wallet_graph_adjacent and get_latest_trust_scores_for_wallets.
        wallet_id: Wallet being scored (affected wallet).
        base_score: Trust score before propagation penalty.
        max_depth: Max hop distance (default 2).
        decay: Decay factor per hop (default 0.5).
        base_penalty: Penalty for an anomalous neighbor at distance 1.
        max_penalty: Cap on total propagated penalty.

    Returns:
        adjusted_score in [0, 100] (clamped).
    """
    neighbor_hops = _neighbors_up_to_hops(db, wallet_id, max_depth)
    if not neighbor_hops:
        return max(0.0, min(100.0, base_score))

    neighbor_list = list(neighbor_hops.keys())
    latest_scores = db.get_latest_trust_scores_for_wallets(neighbor_list)
    total_penalty = 0.0
    hits: list[PropagationHit] = []

    for neighbor in neighbor_list:
        rec = latest_scores.get(neighbor)
        if rec is None:
            continue
        if not _is_anomalous_from_metadata(rec.metadata_json):
            continue
        hop = neighbor_hops[neighbor]
        decay_factor = decay ** hop
        penalty = base_penalty * decay_factor
        total_penalty += penalty
        hits.append(
            PropagationHit(
                source_wallet=neighbor,
                affected_wallet=wallet_id,
                hop_distance=hop,
                decay_factor=round(decay_factor, 4),
                penalty_applied=round(penalty, 2),
            )
        )

    total_penalty = min(total_penalty, max_penalty)
    adjusted = base_score - total_penalty
    adjusted = max(0.0, min(100.0, round(adjusted, 2)))

    for h in hits:
        logger.info(
            "risk_propagation",
            source_wallet=h.source_wallet[:16] + "..." if len(h.source_wallet) > 16 else h.source_wallet,
            affected_wallet=h.affected_wallet[:16] + "..." if len(h.affected_wallet) > 16 else h.affected_wallet,
            hop_distance=h.hop_distance,
            decay_factor=h.decay_factor,
            penalty_applied=h.penalty_applied,
        )

    return adjusted
