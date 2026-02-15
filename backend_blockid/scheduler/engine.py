"""
Wallet scheduling engine: priority queue by tier + risk, anomaly, recency.

Returns the next batch of wallets to analyze. Deterministic, rule-based only (no ML).
Priority order: wallet tier (critical > watchlist > normal), then escalation → high risk
→ recent anomaly → new → normal. Critical wallets scheduled more frequently (top of queue).
Tiebreaker: older last_computed_at first (stale wallets get re-scanned sooner).
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.database.models import TrustScoreRecord, WalletProfile
from backend_blockid.logging import get_logger

logger = get_logger(__name__)

# Wallet priority tiers (scheduling frequency: critical > watchlist > normal)
WALLET_TIER_CRITICAL = "critical"
WALLET_TIER_WATCHLIST = "watchlist"
WALLET_TIER_NORMAL = "normal"
# Rank for sort: higher = analyze first (critical more frequently)
WALLET_TIER_RANK: dict[str, int] = {
    WALLET_TIER_CRITICAL: 3,
    WALLET_TIER_WATCHLIST: 2,
    WALLET_TIER_NORMAL: 1,
}

# Priority tiers (higher = analyze first). Deterministic; explainable.
TIER_ESCALATION = 1000  # severe anomaly (critical/high) in last run → immediate re-scan
TIER_HIGH_RISK = 800    # trust score below threshold
TIER_RECENT_ANOMALY = 600  # any anomaly in last run (medium/low)
TIER_NEW = 500          # no score or never analyzed
TIER_NORMAL = 200       # base; lower trust score = higher sub-score

# Thresholds (pure rules)
TRUST_SCORE_HIGH_RISK_BELOW = 40.0
SEVERE_SEVERITIES = frozenset({"critical", "high"})
NEW_WALLET_MAX_AGE_SEC = 86400 * 7  # 7 days since first seen = still "new" for priority


@dataclass
class SchedulerConfig:
    """Config for the scheduler; all thresholds are rule-based."""

    trust_score_high_risk_below: float = TRUST_SCORE_HIGH_RISK_BELOW
    severe_severities: frozenset[str] = field(default_factory=lambda: SEVERE_SEVERITIES)
    new_wallet_max_age_sec: int = NEW_WALLET_MAX_AGE_SEC
    max_candidates: int = 10_000  # max wallets to consider per get_next_batch


def _parse_metadata(metadata_json: str | None) -> dict[str, Any]:
    """Parse trust score metadata; return empty dict on failure."""
    if not metadata_json:
        return {}
    try:
        return json.loads(metadata_json)
    except (json.JSONDecodeError, TypeError):
        return {}


def _max_anomaly_severity(metadata: dict[str, Any]) -> str | None:
    """
    Return the highest anomaly severity in metadata (critical > high > medium > low).
    None if no anomaly_flags or empty.
    """
    order = ("low", "medium", "high", "critical")
    flags = metadata.get("anomaly_flags") or []
    if not flags:
        return None
    severities = [f.get("severity") for f in flags if isinstance(f, dict) and f.get("severity")]
    if not severities:
        return None
    return max(severities, key=lambda s: order.index(s) if s in order else -1)


def _compute_priority_score(
    wallet: str,
    latest_score: TrustScoreRecord | None,
    profile: WalletProfile | None,
    now_ts: int,
    config: SchedulerConfig,
) -> tuple[float, str]:
    """
    Compute deterministic priority score (higher = analyze first) and reason.
    Returns (score, reason) for explainability.
    """
    score_val: float | None = None
    computed_at: int | None = None
    metadata: dict[str, Any] = {}
    if latest_score:
        score_val = latest_score.score
        computed_at = latest_score.computed_at
        metadata = _parse_metadata(latest_score.metadata_json)
    first_seen = profile.first_seen_at if profile else None
    last_seen = profile.last_seen_at if profile else None

    max_severity = _max_anomaly_severity(metadata)
    is_anomalous = metadata.get("is_anomalous") is True

    # 1. Escalation: severe anomaly in last run → immediate re-scan
    if max_severity and max_severity in config.severe_severities:
        return (TIER_ESCALATION + (100 - (score_val or 0)), "escalation_severe_anomaly")

    # 2. High risk: trust score below threshold
    if score_val is not None and score_val < config.trust_score_high_risk_below:
        return (TIER_HIGH_RISK + (config.trust_score_high_risk_below - score_val), "high_risk_low_score")

    # 3. Recent anomaly (medium/low)
    if is_anomalous and max_severity:
        return (TIER_RECENT_ANOMALY, "recent_anomaly")

    # 4. New wallet: no score or first seen within window
    if latest_score is None:
        return (TIER_NEW, "new_no_score")
    if first_seen is not None and (now_ts - first_seen) <= config.new_wallet_max_age_sec:
        return (TIER_NEW - 1, "new_recent_first_seen")

    # 5. Normal: base + (100 - trust_score) so lower score = higher priority
    sub = 100 - min(100, score_val if score_val is not None else 100)
    return (TIER_NORMAL + sub, "normal")


def _tiebreaker_computed_at(
    wallet: str,
    latest_score: TrustScoreRecord | None,
) -> int:
    """Older last_computed_at = higher priority (analyze stale first). None → treat as 0 (oldest)."""
    if not latest_score or latest_score.computed_at is None:
        return 0
    return latest_score.computed_at


def get_next_batch(
    db: Any,
    limit: int,
    *,
    now_ts: int | None = None,
    config: SchedulerConfig | None = None,
) -> list[str]:
    """
    Return the next batch of wallets to analyze, ordered by priority (high first).

    Uses: trust scores (latest per wallet), wallet profiles, and rule-based
    priority (escalation → high risk → recent anomaly → new → normal).
    Tiebreaker: older last_computed_at first.

    db: Database instance (get_latest_trust_scores_for_wallets, get_wallet_profiles_for_wallets,
        get_tracked_wallet_addresses).
    limit: Max number of wallets to return.
    now_ts: Current time (Unix sec); defaults to time.time().
    config: Scheduler config; defaults to SchedulerConfig().
    """
    cfg = config or SchedulerConfig()
    now_ts = now_ts if now_ts is not None else int(time.time())

    candidates = db.get_tracked_wallet_addresses(limit=cfg.max_candidates)
    if not candidates:
        return []

    priorities_map = db.get_wallet_priorities_for_wallets(candidates)
    latest_scores = db.get_latest_trust_scores_for_wallets(candidates)
    profiles = db.get_wallet_profiles_for_wallets(candidates)

    scored: list[tuple[int, float, int, str, str, str]] = []
    for w in candidates:
        tier = (priorities_map.get(w) or WALLET_TIER_NORMAL).lower()
        tier_rank = WALLET_TIER_RANK.get(tier, 1)
        priority, reason = _compute_priority_score(
            w,
            latest_scores.get(w),
            profiles.get(w) if profiles else None,
            now_ts,
            cfg,
        )
        tb = _tiebreaker_computed_at(w, latest_scores.get(w))
        scored.append((tier_rank, priority, -tb, w, reason, tier))

    scored.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
    batch = [w for (_, _, _, w, _, _) in scored[:limit]]
    if batch:
        logger.debug(
            "scheduler_next_batch",
            batch_size=len(batch),
            limit=limit,
            candidates=len(candidates),
            top_tiers=[t for (_, _, _, _, _, t) in scored[: min(5, len(scored))]],
            top_reasons=[r for (_, _, _, _, r, _) in scored[: min(5, len(scored))]],
        )
    return batch
