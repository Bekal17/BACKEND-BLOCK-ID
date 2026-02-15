"""
Entity reputation engine: long-term evolving reputation for wallet clusters.

Entity = cluster. Score evolves over time: recent behavior > old behavior.
Applies time decay, anomaly weight, alert severity multiplier, cluster spread penalty.
Persists entity_profiles and entity_reputation_history. Wallet trust score
inherits entity reputation modifier. Explainability: cluster_contamination,
repeated_anomalies, behavior_recovery. Runtime-only; no API change.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.logging import get_logger

logger = get_logger(__name__)

REASON_CLUSTER_CONTAMINATION = "cluster_contamination"
REASON_REPEATED_ANOMALIES = "repeated_anomalies"
REASON_BEHAVIOR_RECOVERY = "behavior_recovery"

SECONDS_PER_DAY = 86400
DECAY_DAYS_HALFLIFE = 90
ANOMALY_WEIGHT = 4.0
ALERT_SEVERITY_MULTIPLIER = {"critical": 6.0, "high": 4.0, "medium": 2.0, "low": 1.0}
CLUSTER_SPREAD_PENALTY_FACTOR = 0.5
ENTITY_MODIFIER_SCALE = 0.2
ENTITY_MODIFIER_CAP = 10.0
RECENT_WINDOW_DAYS = 7
HISTORY_SNAPSHOT_MAX = 500


@dataclass
class EntityProfile:
    """
    Long-term evolving reputation for a wallet cluster (entity).

    entity_id: Same as cluster_id (1:1).
    cluster_id: Links to wallet_clusters.
    reputation_score: 0–100.
    risk_history: List of (timestamp, risk_snapshot) for explainability.
    last_updated: Unix timestamp.
    decay_factor: 0–1 applied to prior score over inactivity.
    reason_tags: Explainability tags.
    """

    entity_id: int
    cluster_id: int
    reputation_score: float
    risk_history: list[dict[str, Any]] = field(default_factory=list)
    last_updated: int = 0
    decay_factor: float = 1.0
    reason_tags: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "entity_id": self.entity_id,
            "cluster_id": self.cluster_id,
            "reputation_score": self.reputation_score,
            "risk_history": self.risk_history[-20:],
            "last_updated": self.last_updated,
            "decay_factor": self.decay_factor,
            "reason_tags": self.reason_tags,
        }


def _time_decay(prev_score: float, days_since: float, neutral: float = 50.0) -> float:
    """Pull previous score toward neutral over time; recent behavior matters more."""
    if days_since <= 0:
        return prev_score
    half = DECAY_DAYS_HALFLIFE
    decay = 0.5 ** (days_since / half)
    return neutral + (prev_score - neutral) * decay


def _alert_penalty(alerts: list[tuple[int, str, str]], now_ts: int) -> float:
    """Sum severity-weighted penalties for alerts in recent window."""
    window_start = now_ts - RECENT_WINDOW_DAYS * SECONDS_PER_DAY
    total = 0.0
    for created_at, severity, _ in alerts:
        if created_at < window_start:
            continue
        mult = ALERT_SEVERITY_MULTIPLIER.get((severity or "").strip().lower(), 1.0)
        total += mult
    return total


def _anomaly_penalty(anomaly_count: int) -> float:
    """Penalty from anomaly count (recent window)."""
    return anomaly_count * ANOMALY_WEIGHT


def _cluster_spread_penalty(risky_count: int, member_count: int) -> float:
    """Extra penalty when risk is spread across many members."""
    if member_count < 2:
        return 0.0
    ratio = risky_count / member_count
    return ratio * member_count * CLUSTER_SPREAD_PENALTY_FACTOR


def update_entity_reputation(
    db: Any,
    entity_id: int,
    anomalies: list[dict[str, Any]],
    alerts: list[tuple[int, str, str]],
    *,
    cluster_id: int | None = None,
    member_count: int = 0,
    now_ts: int | None = None,
) -> EntityProfile:
    """
    Update entity (cluster) reputation from anomalies and alerts.
    Recent behavior weighted more; applies time decay, anomaly weight,
    alert severity multiplier, cluster spread penalty. Persists profile
    and appends to entity_reputation_history. Returns EntityProfile with reason_tags.
    """
    now_ts = now_ts or int(time.time())
    if cluster_id is None:
        cluster_id = entity_id

    existing = db.get_entity_profile(entity_id)
    if existing is None:
        prev_score = 50.0
        prev_updated = now_ts - 365 * SECONDS_PER_DAY
        risk_history_json: str | None = None
        decay_factor = 1.0
    else:
        _, prev_score, risk_history_json, prev_updated, decay_factor, _ = existing
        prev_updated = int(prev_updated)

    days_since = (now_ts - prev_updated) / SECONDS_PER_DAY
    decayed = _time_decay(prev_score, days_since)

    anomaly_count = len(anomalies)
    anomaly_penalty = _anomaly_penalty(anomaly_count)
    alert_penalty = _alert_penalty(alerts, now_ts)
    risky_count = sum(1 for a in anomalies if a.get("is_anomalous") is True)
    if member_count < 1:
        member_count = max(1, risky_count)
    spread_penalty = _cluster_spread_penalty(risky_count, member_count)

    raw_score = decayed - anomaly_penalty - alert_penalty - spread_penalty
    reputation_score = max(0.0, min(100.0, round(raw_score, 2)))

    reason_tags: list[str] = []
    if risky_count > 0 and member_count > 0:
        reason_tags.append(REASON_CLUSTER_CONTAMINATION)
    if anomaly_count >= 2:
        reason_tags.append(REASON_REPEATED_ANOMALIES)
    if anomaly_count == 0 and alert_penalty == 0 and prev_score < 70 and reputation_score >= 70:
        reason_tags.append(REASON_BEHAVIOR_RECOVERY)

    new_decay = 1.0 - (days_since / DECAY_DAYS_HALFLIFE) * 0.1
    new_decay = max(0.5, min(1.0, new_decay))

    risk_snapshot = {
        "at": now_ts,
        "reputation_score": reputation_score,
        "anomaly_count": anomaly_count,
        "alert_penalty": round(alert_penalty, 2),
        "spread_penalty": round(spread_penalty, 2),
        "reason_tags": reason_tags,
    }
    risk_history: list[dict[str, Any]] = []
    if risk_history_json:
        try:
            risk_history = json.loads(risk_history_json)
        except (json.JSONDecodeError, TypeError):
            pass
    risk_history.append(risk_snapshot)
    if len(risk_history) > 100:
        risk_history = risk_history[-100:]
    risk_history_json_out = json.dumps(risk_history)
    reason_tags_json = json.dumps(reason_tags)

    db.upsert_entity_profile(
        entity_id=entity_id,
        cluster_id=cluster_id,
        reputation_score=reputation_score,
        risk_history_json=risk_history_json_out,
        last_updated=now_ts,
        decay_factor=new_decay,
        reason_tags_json=reason_tags_json,
    )
    db.insert_entity_reputation_history(
        entity_id=entity_id,
        reputation_score=reputation_score,
        reason_tags_json=reason_tags_json,
        snapshot_at=now_ts,
    )

    logger.info(
        "entity_reputation_updated",
        entity_id=entity_id,
        cluster_id=cluster_id,
        reputation_score=reputation_score,
        reason_tags=reason_tags,
    )

    return EntityProfile(
        entity_id=entity_id,
        cluster_id=cluster_id,
        reputation_score=reputation_score,
        risk_history=risk_history,
        last_updated=now_ts,
        decay_factor=new_decay,
        reason_tags=reason_tags,
    )


def update_entity_reputation_from_cluster(
    db: Any, cluster_id: int, now_ts: int | None = None
) -> EntityProfile | None:
    """
    Gather anomalies and alerts for cluster members from DB, then update entity reputation.
    Call after cluster risk update. entity_id = cluster_id.
    """
    members = db.get_cluster_members(cluster_id)
    if not members:
        return None
    now_ts = now_ts or int(time.time())
    anomalies: list[dict[str, Any]] = []
    all_alerts: list[tuple[int, str, str]] = []
    since_ts = now_ts - RECENT_WINDOW_DAYS * SECONDS_PER_DAY
    latest_scores = db.get_latest_trust_scores_for_wallets(members)
    for w in members:
        rec = latest_scores.get(w)
        if rec and rec.metadata_json:
            try:
                meta = json.loads(rec.metadata_json)
                anomalies.append(
                    {
                        "wallet": w,
                        "is_anomalous": meta.get("is_anomalous") is True,
                        "flags": meta.get("anomaly_flags") or [],
                    }
                )
            except (json.JSONDecodeError, TypeError):
                pass
        alerts = db.get_alerts_for_wallet(w, since_created_at=since_ts, limit=50)
        all_alerts.extend(alerts)
    risky_count = sum(1 for a in anomalies if a.get("is_anomalous") is True)
    return update_entity_reputation(
        db,
        entity_id=cluster_id,
        anomalies=anomalies,
        alerts=all_alerts,
        cluster_id=cluster_id,
        member_count=len(members),
        now_ts=now_ts,
    )


def get_entity_reputation_modifier(db: Any, wallet_id: str) -> float:
    """
    Return modifier for wallet trust score from its entity reputation.
    modifier = (entity_reputation_score - 50) * scale, clamped to [-cap, +cap].
    Wallet inherits entity reputation: good entity lifts score, bad entity lowers it.
    """
    row = db.get_cluster_for_wallet(wallet_id)
    if row is None:
        return 0.0
    cluster_id, _, _, _ = row
    profile = db.get_entity_profile_by_cluster(cluster_id)
    if profile is None:
        return 0.0
    _, reputation_score, _, _, _, _ = profile
    delta = (reputation_score - 50.0) * ENTITY_MODIFIER_SCALE
    return max(-ENTITY_MODIFIER_CAP, min(ENTITY_MODIFIER_CAP, round(delta, 2)))


def apply_entity_modifier(db: Any, wallet_id: str, score_after_cluster: float) -> float:
    """
    Final trust score = score_after_cluster + entity_reputation_modifier. Clamped to [0, 100].
    """
    modifier = get_entity_reputation_modifier(db, wallet_id)
    final = score_after_cluster + modifier
    return max(0.0, min(100.0, round(final, 2)))
