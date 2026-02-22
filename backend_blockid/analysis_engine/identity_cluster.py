"""
Wallet identity clustering: group wallets that likely belong to the same entity.

Heuristic rules (no ML): bidirectional transfers, shared funding source,
burst timing patterns, fan-in/fan-out structure, circular flow. Persist in
wallet_clusters and wallet_cluster_members. Cluster risk increases if any
member is risky. Final trust score = base - anomaly - graph - cluster penalty.
Runtime-only; no API change.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

REASON_BIDIRECTIONAL = "bidirectional"
REASON_SHARED_FUNDING = "shared_funding"
REASON_FAN_IN_OUT = "fan_in_out"
REASON_BURST_TIMING = "burst_timing"
REASON_CIRCULAR = "circular"

MIN_BIDIRECTIONAL_TX = 2
MIN_FAN_SIZE = 2
BURST_WINDOW_SEC = 86400 * 7
MIN_CONFIDENCE = 0.3
MAX_CLUSTER_PENALTY = 15.0
CLUSTER_RISK_FACTOR = 0.25
EDGES_LIMIT = 50000


@dataclass
class Cluster:
    """
    Identity cluster: wallets likely same entity.

    cluster_id: DB primary key (0 until persisted).
    wallet_ids: list of wallet addresses.
    confidence_score: 0.0–1.0.
    reason_tags: e.g. ["bidirectional", "shared_funding"].
    """

    cluster_id: int = 0
    wallet_ids: list[str] = field(default_factory=list)
    confidence_score: float = 0.0
    reason_tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "wallet_ids": self.wallet_ids,
            "confidence_score": self.confidence_score,
            "reason_tags": self.reason_tags,
        }


def _edges_to_lookup(
    edges: list[tuple[str, str, int, int, int]],
) -> tuple[dict[tuple[str, str], tuple[int, int, int]], set[str]]:
    """(sender, receiver, tx_count, total_volume, last_seen_ts) -> (edge -> (tx_count, vol, ts)), all_wallets."""
    lookup: dict[tuple[str, str], tuple[int, int, int]] = {}
    wallets: set[str] = set()
    for s, r, tc, vol, ts in edges:
        if not s or not r or s == r:
            continue
        key = (s.strip(), r.strip())
        lookup[key] = (tc, vol, ts)
        wallets.add(s.strip())
        wallets.add(r.strip())
    return lookup, wallets


def _find_bidirectional(
    edge_lookup: dict[tuple[str, str], tuple[int, int, int]],
) -> list[frozenset[str]]:
    """Pairs (a,b) with both (a,b) and (b,a) and tx_count >= MIN_BIDIRECTIONAL_TX each."""
    pairs: list[frozenset[str]] = []
    seen: set[frozenset[str]] = set()
    for (a, b), (tc, _, _) in edge_lookup.items():
        if a >= b:
            continue
        rev = (b, a)
        if rev not in edge_lookup:
            continue
        tc_rev = edge_lookup[rev][0]
        if tc >= MIN_BIDIRECTIONAL_TX and tc_rev >= MIN_BIDIRECTIONAL_TX:
            f = frozenset({a, b})
            if f not in seen:
                seen.add(f)
                pairs.append(f)
    return pairs


def _find_shared_funding(
    edge_lookup: dict[tuple[str, str], tuple[int, int, int]],
) -> list[set[str]]:
    """Receivers that share the same sender with tx_count >= 1 (fan-in from one source)."""
    sender_to_receivers: dict[str, set[str]] = defaultdict(set)
    for (s, r), (tc, _, _) in edge_lookup.items():
        if tc < 1:
            continue
        sender_to_receivers[s].add(r)
    clusters: list[set[str]] = []
    for receivers in sender_to_receivers.values():
        if len(receivers) >= MIN_FAN_SIZE:
            clusters.append(set(receivers))
    return clusters


def _find_fan_out(
    edge_lookup: dict[tuple[str, str], tuple[int, int, int]],
) -> list[set[str]]:
    """Senders that send to multiple receivers (fan-out); cluster = sender + receivers."""
    sender_to_receivers: dict[str, set[str]] = defaultdict(set)
    for (s, r), (tc, _, _) in edge_lookup.items():
        if tc < 1:
            continue
        sender_to_receivers[s].add(r)
    clusters: list[set[str]] = []
    for sender, receivers in sender_to_receivers.items():
        if len(receivers) >= MIN_FAN_SIZE:
            clusters.append({sender} | receivers)
    return clusters


def _find_burst_timing(
    edge_lookup: dict[tuple[str, str], tuple[int, int, int]],
    window_sec: int = BURST_WINDOW_SEC,
) -> list[set[str]]:
    """Group edges by last_seen in same time window; cluster wallets in that burst."""
    bucket_to_wallets: dict[int, set[str]] = defaultdict(set)
    for (s, r), (_, _, ts) in edge_lookup.items():
        if ts <= 0:
            continue
        bucket = ts // window_sec
        bucket_to_wallets[bucket].add(s)
        bucket_to_wallets[bucket].add(r)
    return [w for w in bucket_to_wallets.values() if len(w) >= MIN_FAN_SIZE][:50]


def _find_circular_2(
    edge_lookup: dict[tuple[str, str], tuple[int, int, int]],
) -> list[frozenset[str]]:
    """2-cycles: same as bidirectional."""
    return _find_bidirectional(edge_lookup)


def _merge_cluster_sets(
    pairs: list[frozenset[str]],
    shared: list[set[str]],
    fan: list[set[str]],
    burst: list[set[str]],
    circular: list[frozenset[str]],
) -> list[tuple[set[str], list[str]]]:
    """Merge overlapping sets; return list of (wallet_set, reason_tags)."""
    merged: list[tuple[set[str], list[str]]] = []
    all_sets: list[tuple[set[str], list[str]]] = []
    for f in pairs:
        all_sets.append((set(f), [REASON_BIDIRECTIONAL]))
    for s in shared:
        all_sets.append((s, [REASON_SHARED_FUNDING]))
    for s in fan:
        all_sets.append((s, [REASON_FAN_IN_OUT]))
    for s in burst:
        all_sets.append((s, [REASON_BURST_TIMING]))
    for f in circular:
        all_sets.append((set(f), [REASON_CIRCULAR]))

    while all_sets:
        current, tags = all_sets.pop(0)
        changed = True
        while changed:
            changed = False
            rest = []
            for s, t in all_sets:
                if current & s:
                    current |= s
                    tags = list(dict.fromkeys(tags + t))
                    changed = True
                else:
                    rest.append((s, t))
            all_sets = rest
        if len(current) >= 2 and current not in [m[0] for m in merged]:
            merged.append((current, tags))
    return merged


def _confidence_from_reasons(reason_tags: list[str], size: int) -> float:
    """Simple confidence: more reasons and size -> higher."""
    base = 0.4 + 0.1 * len(reason_tags) + 0.05 * min(size - 2, 4)
    return min(1.0, round(base, 2))


def run_clustering(
    db: Any, *, edges_limit: int = EDGES_LIMIT, replace: bool = True
) -> list[Cluster]:
    """
    Build clusters from graph edges using heuristics; persist to DB.
    If replace=True (default), clears existing clusters first for a full recompute.
    Returns new Cluster objects. Logs cluster_created, wallet_added_to_cluster.
    """
    if replace:
        db.delete_all_wallet_clusters()
    edges = db.get_wallet_graph_edges_all(limit=edges_limit)
    if not edges:
        return []
    edge_lookup, _ = _edges_to_lookup(edges)
    pairs = _find_bidirectional(edge_lookup)
    shared = _find_shared_funding(edge_lookup)
    fan = _find_fan_out(edge_lookup)
    burst = _find_burst_timing(edge_lookup)
    circular = _find_circular_2(edge_lookup)
    merged = _merge_cluster_sets(pairs, shared, fan, burst, circular)

    result: list[Cluster] = []
    for wallet_set, reason_tags in merged:
        if len(wallet_set) < 2:
            continue
        confidence = _confidence_from_reasons(reason_tags, len(wallet_set))
        if confidence < MIN_CONFIDENCE:
            continue
        reason_tags_json = json.dumps(reason_tags)
        cluster_id = db.insert_wallet_cluster(confidence, reason_tags_json)
        logger.info(
            "cluster_created",
            cluster_id=cluster_id,
            wallet_count=len(wallet_set),
            confidence_score=confidence,
            reason_tags=reason_tags,
        )
        for w in sorted(wallet_set):
            db.insert_wallet_cluster_member(cluster_id, w)
            logger.debug(
                "wallet_added_to_cluster",
                cluster_id=cluster_id,
                wallet_id=w[:16] + "..." if len(w) > 16 else w,
            )
        result.append(
            Cluster(
                cluster_id=cluster_id,
                wallet_ids=sorted(wallet_set),
                confidence_score=confidence,
                reason_tags=reason_tags,
            )
        )
    return result


def compute_cluster_risk(db: Any, cluster_id: int) -> float:
    """
    If any member is risky (low trust score or anomalous), return cluster risk penalty.
    Persists cluster_risk to DB and logs cluster_risk_updated.
    """
    members = db.get_cluster_members(cluster_id)
    if not members:
        return 0.0
    latest = db.get_latest_trust_scores_for_wallets(members)
    scores: list[float] = []
    risky_wallets: set[str] = set()
    for w in members:
        rec = latest.get(w)
        if rec is None:
            continue
        scores.append(rec.score)
        if rec.score < 70.0:
            risky_wallets.add(w)
        if rec.metadata_json:
            try:
                meta = json.loads(rec.metadata_json)
                if meta.get("is_anomalous") is True:
                    risky_wallets.add(w)
            except (json.JSONDecodeError, TypeError):
                pass
    risky_count = len(risky_wallets)
    if not scores and risky_count == 0:
        risk = 0.0
    else:
        min_score = min(scores) if scores else 100.0
        risk = (100.0 - min_score) * CLUSTER_RISK_FACTOR
        if risky_count > 0:
            risk = min(MAX_CLUSTER_PENALTY, risk + risky_count * 2.0)
        risk = min(MAX_CLUSTER_PENALTY, round(risk, 2))
    db.update_cluster_risk(cluster_id, risk)
    logger.info(
        "cluster_risk_updated",
        cluster_id=cluster_id,
        cluster_risk=risk,
        member_count=len(members),
        risky_count=risky_count,
    )
    try:
        from backend_blockid.analysis_engine.entity_reputation import update_entity_reputation_from_cluster
        update_entity_reputation_from_cluster(db, cluster_id)
    except Exception as e:
        logger.warning(
            "entity_reputation_update_failed",
            cluster_id=cluster_id,
            error=str(e),
        )
    return risk


def get_cluster_penalty_for_wallet(db: Any, wallet_id: str) -> float:
    """
    Return cluster risk penalty for wallet (0 if not in a cluster or cluster has no risk).
    Used in final trust score: final = base - anomaly - graph - cluster_penalty.
    """
    row = db.get_cluster_for_wallet(wallet_id)
    if row is None:
        return 0.0
    cluster_id, _, _, stored_risk = row
    if stored_risk is not None and stored_risk > 0:
        return min(MAX_CLUSTER_PENALTY, stored_risk)
    risk = compute_cluster_risk(db, cluster_id)
    return min(MAX_CLUSTER_PENALTY, risk)


def apply_cluster_penalty(db: Any, wallet_id: str, score_after_graph: float) -> float:
    """
    Final trust score = score_after_graph - cluster_penalty. Clamped to [0, 100].
    """
    penalty = get_cluster_penalty_for_wallet(db, wallet_id)
    final = score_after_graph - penalty
    return max(0.0, min(100.0, round(final, 2)))
