"""
Trust Oracle Service: expose trust intelligence safely to external systems.

Read-only: uses existing DB and reputation system. No heavy analysis.
Cache with configurable TTL. Rate limiting per client (IP or wallet).
Explainability: anomaly summary, cluster contamination, historical trend.
New module; no change to existing API endpoints.
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

DEFAULT_CACHE_TTL_SEC = 60
DEFAULT_RATE_LIMIT_REQUESTS = 100
DEFAULT_RATE_LIMIT_WINDOW_SEC = 60

RISK_LEVEL_CRITICAL = "critical"
RISK_LEVEL_HIGH = "high"
RISK_LEVEL_MEDIUM = "medium"
RISK_LEVEL_LOW = "low"


def _score_to_risk_level(score: float) -> str:
    if score < 30:
        return RISK_LEVEL_CRITICAL
    if score < 50:
        return RISK_LEVEL_HIGH
    if score < 70:
        return RISK_LEVEL_MEDIUM
    return RISK_LEVEL_LOW


@dataclass
class OracleResult:
    """Structured oracle response: trust_score, risk_level, entity_reputation, cluster_risk, reason_tags, last_updated, explanation."""

    trust_score: float | None
    risk_level: str
    entity_reputation: float | None
    cluster_risk: float | None
    reason_tags: list[str]
    last_updated: int
    explanation: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trust_score": self.trust_score,
            "risk_level": self.risk_level,
            "entity_reputation": self.entity_reputation,
            "cluster_risk": self.cluster_risk,
            "reason_tags": self.reason_tags,
            "last_updated": self.last_updated,
            "explanation": self.explanation,
        }


class _TTLCache:
    """Thread-safe cache with TTL. Key -> (value, expiry_ts)."""

    def __init__(self, ttl_sec: float) -> None:
        self._ttl = ttl_sec
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> Any | None:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._store[key]
                return None
            return value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._store[key] = (value, time.monotonic() + self._ttl)


class _RateLimiter:
    """Sliding-window rate limiter per key (e.g. IP or wallet)."""

    def __init__(self, max_requests: int, window_sec: float) -> None:
        self._max = max_requests
        self._window = window_sec
        self._key_to_times: dict[str, deque[float]] = {}
        self._lock = threading.Lock()

    def allow(self, key: str) -> bool:
        now = time.monotonic()
        cutoff = now - self._window
        with self._lock:
            times = self._key_to_times.setdefault(key, deque())
            while times and times[0] < cutoff:
                times.popleft()
            if len(times) >= self._max:
                return False
            times.append(now)
            return True


@dataclass
class OracleConfig:
    """Config for Trust Oracle: cache TTL and rate limits."""

    cache_ttl_sec: float = DEFAULT_CACHE_TTL_SEC
    rate_limit_requests: int = DEFAULT_RATE_LIMIT_REQUESTS
    rate_limit_window_sec: float = DEFAULT_RATE_LIMIT_WINDOW_SEC


class TrustOracle:
    """
    Trust Oracle: read-only access to trust intelligence with caching and rate limiting.
    """

    def __init__(self, db: Any, config: OracleConfig | None = None) -> None:
        self._db = db
        self._config = config or OracleConfig()
        self._cache = _TTLCache(self._config.cache_ttl_sec)
        self._rate_limiter = _RateLimiter(
            self._config.rate_limit_requests,
            self._config.rate_limit_window_sec,
        )

    def _rate_limit_key(self, client_id: str | None, fallback: str) -> str:
        return (client_id or fallback).strip() or "anonymous"

    def _check_rate_limit(self, key: str) -> bool:
        return self._rate_limiter.allow(key)

    def get_wallet_trust(
        self,
        wallet_id: str,
        *,
        client_id: str | None = None,
    ) -> OracleResult | None:
        """
        Return structured trust data for a wallet. Read-only from DB.
        Cached by TTL. Rate limited by client_id or wallet_id.
        """
        wallet_id = wallet_id.strip()
        if not wallet_id:
            return None
        rl_key = self._rate_limit_key(client_id, wallet_id)
        if not self._check_rate_limit(rl_key):
            logger.warning("oracle_rate_limited", key=rl_key[:32])
            return None

        cache_key = f"wallet:{wallet_id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        timeline = self._db.get_trust_score_timeline(wallet_id, limit=1)
        if not timeline:
            return None
        latest = timeline[0]
        trust_score = round(float(latest.score), 2)
        last_updated = latest.computed_at

        reputation = self._db.get_wallet_reputation_state(wallet_id)
        trend = None
        avg_7d = None
        avg_30d = None
        if reputation is not None:
            _, avg_7d, avg_30d, trend, _, _, _ = reputation

        cluster_row = self._db.get_cluster_for_wallet(wallet_id)
        cluster_risk = None
        entity_reputation = None
        reason_tags: list[str] = []
        cluster_contamination = None
        if cluster_row is not None:
            cid, _, c_reason_json, cluster_risk = cluster_row
            if c_reason_json:
                try:
                    reason_tags = json.loads(c_reason_json) or []
                except (json.JSONDecodeError, TypeError):
                    pass
            if cluster_risk is not None and cluster_risk > 0:
                cluster_contamination = f"Cluster risk {cluster_risk:.1f}; reason_tags={reason_tags}"
            entity_profile = self._db.get_entity_profile_by_cluster(cid)
            if entity_profile is not None:
                _, entity_reputation, _, _, _, e_reason = entity_profile
                if e_reason:
                    try:
                        reason_tags = list(dict.fromkeys(reason_tags + (json.loads(e_reason) or [])))
                    except (json.JSONDecodeError, TypeError):
                        pass

        anomaly_summary = None
        if latest.metadata_json:
            try:
                meta = json.loads(latest.metadata_json)
                flags = meta.get("anomaly_flags") or []
                if flags:
                    anomaly_summary = f"{len(flags)} anomaly flag(s): " + ", ".join(
                        (f.get("rule_name") or f.get("type") or "unknown") for f in flags[:5]
                    )
            except (json.JSONDecodeError, TypeError):
                pass

        historical_trend = None
        if trend:
            historical_trend = f"Trend: {trend}"
            if avg_7d is not None or avg_30d is not None:
                historical_trend += f"; avg_7d={avg_7d}; avg_30d={avg_30d}"

        risk_level = _score_to_risk_level(trust_score)
        result = OracleResult(
            trust_score=trust_score,
            risk_level=risk_level,
            entity_reputation=round(entity_reputation, 2) if entity_reputation is not None else None,
            cluster_risk=round(cluster_risk, 2) if cluster_risk is not None else None,
            reason_tags=reason_tags,
            last_updated=last_updated,
            explanation={
                "anomaly_summary": anomaly_summary,
                "cluster_contamination": cluster_contamination,
                "historical_trend": historical_trend,
            },
        )
        self._cache.set(cache_key, result)
        return result

    def get_entity_reputation(
        self,
        entity_id: int,
        *,
        client_id: str | None = None,
    ) -> OracleResult | None:
        """
        Return structured reputation data for an entity (cluster). Read-only from DB.
        """
        rl_key = self._rate_limit_key(client_id, f"entity:{entity_id}")
        if not self._check_rate_limit(rl_key):
            logger.warning("oracle_rate_limited", key=rl_key[:32])
            return None

        cache_key = f"entity:{entity_id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        profile = self._db.get_entity_profile(entity_id)
        if profile is None:
            return None
        _, reputation_score, risk_history_json, last_updated, _, reason_tags_json = profile
        reputation_score = float(reputation_score)
        last_updated = int(last_updated)
        reason_tags: list[str] = []
        if reason_tags_json:
            try:
                reason_tags = json.loads(reason_tags_json) or []
            except (json.JSONDecodeError, TypeError):
                pass

        history = self._db.get_entity_reputation_history(entity_id, limit=10)
        historical_trend = None
        if len(history) >= 2:
            recent, older = history[0][0], history[-1][0]
            direction = "improving" if recent > older else "degrading" if recent < older else "stable"
            historical_trend = f"Entity trend: {direction} (recent={recent:.1f}, older={older:.1f})"

        cluster_row = self._db.get_cluster_by_id(entity_id)
        cluster_risk = None
        if cluster_row is not None:
            _, _, cluster_risk, _ = cluster_row

        risk_level = _score_to_risk_level(100.0 - (cluster_risk or 0))
        result = OracleResult(
            trust_score=reputation_score,
            risk_level=risk_level,
            entity_reputation=reputation_score,
            cluster_risk=round(cluster_risk, 2) if cluster_risk is not None else None,
            reason_tags=reason_tags,
            last_updated=last_updated,
            explanation={
                "anomaly_summary": None,
                "cluster_contamination": f"Entity in cluster; cluster_risk={cluster_risk}" if cluster_risk else None,
                "historical_trend": historical_trend,
            },
        )
        self._cache.set(cache_key, result)
        return result

    def get_cluster_risk(
        self,
        cluster_id: int,
        *,
        client_id: str | None = None,
    ) -> OracleResult | None:
        """
        Return structured risk data for a cluster. Read-only from DB.
        """
        rl_key = self._rate_limit_key(client_id, f"cluster:{cluster_id}")
        if not self._check_rate_limit(rl_key):
            logger.warning("oracle_rate_limited", key=rl_key[:32])
            return None

        cache_key = f"cluster:{cluster_id}"
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        cluster_row = self._db.get_cluster_by_id(cluster_id)
        if cluster_row is None:
            return None
        confidence_score, reason_tags_json, cluster_risk, risk_updated_at = cluster_row
        reason_tags: list[str] = []
        if reason_tags_json:
            try:
                reason_tags = json.loads(reason_tags_json) or []
            except (json.JSONDecodeError, TypeError):
                pass
        last_updated = risk_updated_at or 0

        entity_profile = self._db.get_entity_profile_by_cluster(cluster_id)
        entity_reputation = None
        if entity_profile is not None:
            _, entity_reputation, _, last_updated_entity, _, _ = entity_profile
            if last_updated_entity > last_updated:
                last_updated = last_updated_entity

        members = self._db.get_cluster_members(cluster_id)
        cluster_contamination = f"Cluster has {len(members)} member(s); cluster_risk={cluster_risk}" if cluster_risk else None

        trust_score = 100.0 - (cluster_risk or 0)
        risk_level = _score_to_risk_level(trust_score)
        result = OracleResult(
            trust_score=round(trust_score, 2),
            risk_level=risk_level,
            entity_reputation=round(entity_reputation, 2) if entity_reputation is not None else None,
            cluster_risk=round(cluster_risk, 2) if cluster_risk is not None else None,
            reason_tags=reason_tags,
            last_updated=last_updated,
            explanation={
                "anomaly_summary": None,
                "cluster_contamination": cluster_contamination,
                "historical_trend": f"confidence={confidence_score}; members={len(members)}",
            },
        )
        self._cache.set(cache_key, result)
        return result


def get_wallet_trust(
    db: Any,
    wallet_id: str,
    *,
    client_id: str | None = None,
    config: OracleConfig | None = None,
) -> OracleResult | None:
    """Convenience: get wallet trust using a transient oracle (no shared cache)."""
    oracle = TrustOracle(db, config=config)
    return oracle.get_wallet_trust(wallet_id, client_id=client_id)


def get_entity_reputation(
    db: Any,
    entity_id: int,
    *,
    client_id: str | None = None,
    config: OracleConfig | None = None,
) -> OracleResult | None:
    """Convenience: get entity reputation using a transient oracle."""
    oracle = TrustOracle(db, config=config)
    return oracle.get_entity_reputation(entity_id, client_id=client_id)


def get_cluster_risk(
    db: Any,
    cluster_id: int,
    *,
    client_id: str | None = None,
    config: OracleConfig | None = None,
) -> OracleResult | None:
    """Convenience: get cluster risk using a transient oracle."""
    oracle = TrustOracle(db, config=config)
    return oracle.get_cluster_risk(cluster_id, client_id=client_id)
