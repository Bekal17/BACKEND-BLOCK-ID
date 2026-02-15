"""
Database abstraction layer for wallet profiles, transaction history, and trust score timeline.

MVP uses SQLite; designed so the backend can be swapped to PostgreSQL via a
different Backend implementation. All access goes through the abstract interface;
SQL and placeholders are backend-specific (? for SQLite, %s for PostgreSQL).
"""

from __future__ import annotations

import json
import sqlite3
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from backend_blockid.database.models import (
    TransactionRecord,
    TrustScoreRecord,
    WalletProfile,
)
from backend_blockid.logging import get_logger

logger = get_logger(__name__)

# -----------------------------------------------------------------------------
# Schema (SQLite). For PostgreSQL: use SERIAL/BIGSERIAL, TIMESTAMPTZ, and %s.
# -----------------------------------------------------------------------------

SCHEMA_WALLET_PROFILES = """
CREATE TABLE IF NOT EXISTS wallet_profiles (
    wallet TEXT PRIMARY KEY,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    profile_json TEXT,
    created_at INTEGER,
    updated_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_wallet_profiles_last_seen ON wallet_profiles(last_seen_at);
"""

SCHEMA_TRANSACTIONS = """
CREATE TABLE IF NOT EXISTS transactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    signature TEXT NOT NULL,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount_lamports INTEGER NOT NULL,
    timestamp INTEGER,
    slot INTEGER,
    created_at INTEGER,
    UNIQUE(wallet, signature)
);
CREATE INDEX IF NOT EXISTS ix_transactions_wallet ON transactions(wallet);
CREATE INDEX IF NOT EXISTS ix_transactions_signature ON transactions(signature);
CREATE INDEX IF NOT EXISTS ix_transactions_timestamp ON transactions(timestamp);
CREATE INDEX IF NOT EXISTS ix_transactions_wallet_timestamp ON transactions(wallet, timestamp);
"""

SCHEMA_TRUST_SCORES = """
CREATE TABLE IF NOT EXISTS trust_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    score REAL NOT NULL,
    computed_at INTEGER NOT NULL,
    metadata_json TEXT,
    created_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_trust_scores_wallet ON trust_scores(wallet);
CREATE INDEX IF NOT EXISTS ix_trust_scores_wallet_computed ON trust_scores(wallet, computed_at);
"""

SCHEMA_TRACKED_WALLETS = """
CREATE TABLE IF NOT EXISTS tracked_wallets (
    wallet TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    priority TEXT NOT NULL DEFAULT 'normal',
    last_analyzed_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_tracked_wallets_created_at ON tracked_wallets(created_at);
CREATE INDEX IF NOT EXISTS ix_tracked_wallets_priority ON tracked_wallets(priority);
"""

SCHEMA_ALERTS = """
CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    severity TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_alerts_wallet ON alerts(wallet);
CREATE INDEX IF NOT EXISTS ix_alerts_wallet_severity_reason_created ON alerts(wallet, severity, reason, created_at);
CREATE INDEX IF NOT EXISTS ix_alerts_created_at ON alerts(created_at);
"""

SCHEMA_WALLET_ROLLING_STATS = """
CREATE TABLE IF NOT EXISTS wallet_rolling_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    wallet TEXT NOT NULL,
    period_end_ts INTEGER NOT NULL,
    window_days INTEGER NOT NULL,
    volume_lamports INTEGER NOT NULL,
    tx_count INTEGER NOT NULL,
    anomaly_count INTEGER NOT NULL,
    avg_trust_score REAL,
    alert_count INTEGER NOT NULL,
    created_at INTEGER
);
CREATE INDEX IF NOT EXISTS ix_wallet_rolling_stats_wallet_window ON wallet_rolling_stats(wallet, window_days);
CREATE INDEX IF NOT EXISTS ix_wallet_rolling_stats_period ON wallet_rolling_stats(wallet, window_days, period_end_ts DESC);
"""

SCHEMA_WALLET_ESCALATION_STATE = """
CREATE TABLE IF NOT EXISTS wallet_escalation_state (
    wallet TEXT PRIMARY KEY,
    risk_stage TEXT NOT NULL,
    escalation_score REAL NOT NULL,
    last_alert_ts INTEGER,
    last_clean_ts INTEGER,
    state_json TEXT,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_wallet_escalation_risk_stage ON wallet_escalation_state(risk_stage);
"""

SCHEMA_WALLET_PRIORITY = """
CREATE TABLE IF NOT EXISTS wallet_priority (
    wallet TEXT PRIMARY KEY,
    tier TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_wallet_priority_tier ON wallet_priority(tier);
"""

SCHEMA_WALLET_REPUTATION_STATE = """
CREATE TABLE IF NOT EXISTS wallet_reputation_state (
    wallet TEXT PRIMARY KEY,
    current_score REAL NOT NULL,
    avg_7d REAL,
    avg_30d REAL,
    trend TEXT NOT NULL,
    volatility REAL,
    decay_factor REAL NOT NULL DEFAULT 1.0,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_wallet_reputation_trend ON wallet_reputation_state(trend);
"""

SCHEMA_WALLET_GRAPH_EDGES = """
CREATE TABLE IF NOT EXISTS wallet_graph_edges (
    sender_wallet TEXT NOT NULL,
    receiver_wallet TEXT NOT NULL,
    tx_count INTEGER NOT NULL DEFAULT 0,
    total_volume INTEGER NOT NULL DEFAULT 0,
    last_seen_timestamp INTEGER NOT NULL,
    PRIMARY KEY (sender_wallet, receiver_wallet)
);
CREATE INDEX IF NOT EXISTS ix_wallet_graph_sender ON wallet_graph_edges(sender_wallet);
CREATE INDEX IF NOT EXISTS ix_wallet_graph_receiver ON wallet_graph_edges(receiver_wallet);
"""

SCHEMA_WALLET_CLUSTERS = """
CREATE TABLE IF NOT EXISTS wallet_clusters (
    cluster_id INTEGER PRIMARY KEY AUTOINCREMENT,
    confidence_score REAL NOT NULL DEFAULT 0.0,
    reason_tags TEXT,
    cluster_risk REAL,
    risk_updated_at INTEGER,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_wallet_clusters_confidence ON wallet_clusters(confidence_score);
"""

SCHEMA_WALLET_CLUSTER_MEMBERS = """
CREATE TABLE IF NOT EXISTS wallet_cluster_members (
    cluster_id INTEGER NOT NULL,
    wallet TEXT NOT NULL,
    added_at INTEGER NOT NULL,
    PRIMARY KEY (cluster_id, wallet),
    FOREIGN KEY (cluster_id) REFERENCES wallet_clusters(cluster_id)
);
CREATE INDEX IF NOT EXISTS ix_wallet_cluster_members_wallet ON wallet_cluster_members(wallet);
"""

SCHEMA_ENTITY_PROFILES = """
CREATE TABLE IF NOT EXISTS entity_profiles (
    entity_id INTEGER PRIMARY KEY,
    cluster_id INTEGER NOT NULL,
    reputation_score REAL NOT NULL DEFAULT 50.0,
    risk_history TEXT,
    last_updated INTEGER NOT NULL,
    decay_factor REAL NOT NULL DEFAULT 1.0,
    reason_tags TEXT,
    FOREIGN KEY (cluster_id) REFERENCES wallet_clusters(cluster_id)
);
CREATE INDEX IF NOT EXISTS ix_entity_profiles_cluster ON entity_profiles(cluster_id);
"""

SCHEMA_ENTITY_REPUTATION_HISTORY = """
CREATE TABLE IF NOT EXISTS entity_reputation_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id INTEGER NOT NULL,
    reputation_score REAL NOT NULL,
    reason_tags TEXT,
    snapshot_at INTEGER NOT NULL,
    FOREIGN KEY (entity_id) REFERENCES entity_profiles(entity_id)
);
CREATE INDEX IF NOT EXISTS ix_entity_reputation_history_entity ON entity_reputation_history(entity_id);
CREATE INDEX IF NOT EXISTS ix_entity_reputation_history_snapshot ON entity_reputation_history(entity_id, snapshot_at DESC);
"""


# -----------------------------------------------------------------------------
# Abstract backend: swap implementation for PostgreSQL later.
# -----------------------------------------------------------------------------


class DatabaseBackend(ABC):
    """Abstract interface for persistence; implement for SQLite or PostgreSQL."""

    @abstractmethod
    def ensure_schema(self) -> None:
        """Create tables and indexes if they do not exist."""
        ...

    @abstractmethod
    def upsert_wallet_profile(self, profile: WalletProfile) -> None:
        """Insert or update a wallet profile by wallet key."""
        ...

    @abstractmethod
    def get_wallet_profile(self, wallet: str) -> WalletProfile | None:
        """Return the wallet profile for the given address, or None."""
        ...

    @abstractmethod
    def insert_transactions(
        self,
        wallet: str,
        records: list[tuple[str, str, str, int, int | None, int | None]],
    ) -> int:
        """
        Insert transaction rows for a wallet. Each record is
        (signature, sender, receiver, amount_lamports, timestamp, slot).
        Ignores duplicates (wallet+signature). Returns number inserted.
        """
        ...

    @abstractmethod
    def get_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 500,
        since_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> list[TransactionRecord]:
        """Return transaction history for a wallet, newest first."""
        ...

    @abstractmethod
    def insert_trust_score(
        self,
        wallet: str,
        score: float,
        computed_at: int,
        metadata_json: str | None = None,
    ) -> int:
        """Append a trust score to the timeline. Returns row id."""
        ...

    @abstractmethod
    def get_trust_score_timeline(
        self,
        wallet: str,
        *,
        limit: int = 100,
        since_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> list[TrustScoreRecord]:
        """Return trust score timeline for a wallet, newest first."""
        ...

    @abstractmethod
    def get_tracked_wallets(self, *, limit: int = 5000) -> list[str]:
        """Return wallet addresses from wallet_profiles, most recently seen first."""
        ...

    @abstractmethod
    def add_tracked_wallet(self, wallet: str) -> bool:
        """Register a wallet for monitoring. Returns True if inserted, False if already present."""
        ...

    @abstractmethod
    def get_tracked_wallet_created_at(self, wallet: str) -> int | None:
        """Return created_at for a tracked wallet, or None if not found."""
        ...

    @abstractmethod
    def get_tracked_wallet_addresses(self, *, limit: int = 10000) -> list[str]:
        """Return wallet addresses from tracked_wallets registry, oldest first (FIFO)."""
        ...

    @abstractmethod
    def get_tracked_wallets_with_priority_and_analyzed(
        self, *, limit: int = 50000
    ) -> list[tuple[str, str, int | None]]:
        """Return (wallet, priority, last_analyzed_at) for all tracked wallets, oldest first."""
        ...

    @abstractmethod
    def update_tracked_wallet_priority(self, wallet: str, priority: str) -> None:
        """Set priority (critical | watchlist | normal) for a tracked wallet."""
        ...

    @abstractmethod
    def update_tracked_wallet_last_analyzed(self, wallet: str, last_analyzed_at: int) -> None:
        """Set last_analyzed_at (Unix timestamp) for a tracked wallet."""
        ...

    @abstractmethod
    def insert_alert(self, wallet: str, severity: str, reason: str, created_at: int) -> int:
        """Insert an alert. Returns row id."""
        ...

    @abstractmethod
    def has_recent_alert(
        self,
        wallet: str,
        severity: str,
        reason: str,
        since_created_at: int,
    ) -> bool:
        """True if an alert (wallet, severity, reason) exists with created_at >= since_created_at."""
        ...

    @abstractmethod
    def get_alert_count(
        self,
        wallet: str,
        since_created_at: int,
        until_created_at: int | None = None,
    ) -> int:
        """Count alerts for wallet with created_at in [since_created_at, until_created_at] (until optional)."""
        ...

    @abstractmethod
    def insert_wallet_rolling_stats(
        self,
        wallet: str,
        period_end_ts: int,
        window_days: int,
        volume_lamports: int,
        tx_count: int,
        anomaly_count: int,
        avg_trust_score: float | None,
        alert_count: int,
    ) -> int:
        """Append one rolling stats snapshot. Returns row id."""
        ...

    @abstractmethod
    def get_wallet_rolling_stats_history(
        self,
        wallet: str,
        window_days: int,
        *,
        limit: int = 32,
    ) -> list[tuple[int, int, int, int, float | None, int]]:
        """Return (period_end_ts, volume_lamports, tx_count, anomaly_count, avg_trust_score, alert_count) newest first."""
        ...

    @abstractmethod
    def get_alerts_for_wallet(
        self,
        wallet: str,
        since_created_at: int,
        until_created_at: int | None = None,
        limit: int = 200,
    ) -> list[tuple[int, str, str]]:
        """Return (created_at, severity, reason) for wallet, newest first."""
        ...

    @abstractmethod
    def get_escalation_state(
        self,
        wallet: str,
    ) -> tuple[str, float, int | None, int | None, str | None, int] | None:
        """Return (risk_stage, escalation_score, last_alert_ts, last_clean_ts, state_json, updated_at) or None."""
        ...

    @abstractmethod
    def upsert_escalation_state(
        self,
        wallet: str,
        risk_stage: str,
        escalation_score: float,
        last_alert_ts: int | None,
        last_clean_ts: int | None,
        state_json: str | None,
    ) -> None:
        """Insert or update escalation state for wallet."""
        ...

    @abstractmethod
    def get_wallet_priority(self, wallet: str) -> str | None:
        """Return tier (critical | watchlist | normal) for wallet, or None if not set (default normal)."""
        ...

    @abstractmethod
    def set_wallet_priority(self, wallet: str, tier: str) -> None:
        """Set wallet priority tier (critical | watchlist | normal)."""
        ...

    @abstractmethod
    def get_wallet_priorities_for_wallets(self, wallets: list[str]) -> dict[str, str]:
        """Return dict wallet -> tier for given wallets; missing wallets default to normal in scheduler."""
        ...

    @abstractmethod
    def get_wallet_reputation_state(
        self,
        wallet: str,
    ) -> tuple[float, float | None, float | None, str, float | None, float, int] | None:
        """Return (current_score, avg_7d, avg_30d, trend, volatility, decay_factor, updated_at) or None."""
        ...

    @abstractmethod
    def upsert_wallet_reputation_state(
        self,
        wallet: str,
        current_score: float,
        avg_7d: float | None,
        avg_30d: float | None,
        trend: str,
        volatility: float | None,
        decay_factor: float,
    ) -> None:
        """Insert or update reputation state for wallet."""
        ...

    @abstractmethod
    def upsert_wallet_graph_edge(
        self,
        sender_wallet: str,
        receiver_wallet: str,
        amount_lamports: int,
        timestamp: int,
    ) -> None:
        """Increment edge (sender -> receiver): tx_count += 1, total_volume += amount, last_seen = max(last_seen, timestamp)."""
        ...

    @abstractmethod
    def get_wallet_graph_adjacent(self, wallet: str) -> list[str]:
        """Return distinct wallet addresses that share an edge with wallet (as sender or receiver)."""
        ...

    @abstractmethod
    def get_wallet_graph_edges_all(
        self, limit: int = 50000
    ) -> list[tuple[str, str, int, int, int]]:
        """Return (sender_wallet, receiver_wallet, tx_count, total_volume, last_seen_timestamp) for clustering."""
        ...

    @abstractmethod
    def insert_wallet_cluster(
        self, confidence_score: float, reason_tags_json: str | None
    ) -> int:
        """Insert a cluster; return cluster_id."""
        ...

    @abstractmethod
    def insert_wallet_cluster_member(self, cluster_id: int, wallet: str) -> None:
        """Add wallet to cluster. Idempotent."""
        ...

    @abstractmethod
    def get_cluster_members(self, cluster_id: int) -> list[str]:
        """Return wallet addresses in the cluster."""
        ...

    @abstractmethod
    def get_cluster_for_wallet(
        self, wallet: str
    ) -> tuple[int, float, str | None, float | None] | None:
        """Return (cluster_id, confidence_score, reason_tags_json, cluster_risk) or None."""
        ...

    @abstractmethod
    def get_all_clusters(
        self,
    ) -> list[tuple[int, float, str | None, float | None, int | None]]:
        """Return (cluster_id, confidence_score, reason_tags_json, cluster_risk, risk_updated_at)."""
        ...

    @abstractmethod
    def update_cluster_confidence(
        self, cluster_id: int, confidence_score: float, reason_tags_json: str | None
    ) -> None:
        """Update cluster confidence and reason tags."""
        ...

    @abstractmethod
    def update_cluster_risk(self, cluster_id: int, cluster_risk: float) -> None:
        """Update stored cluster risk."""
        ...

    @abstractmethod
    def delete_all_wallet_clusters(self) -> None:
        """Remove all clusters and members; used before full recompute."""
        ...

    @abstractmethod
    def upsert_entity_profile(
        self,
        entity_id: int,
        cluster_id: int,
        reputation_score: float,
        risk_history_json: str | None,
        last_updated: int,
        decay_factor: float,
        reason_tags_json: str | None,
    ) -> None:
        """Insert or update entity profile (entity_id = cluster_id for 1:1)."""
        ...

    @abstractmethod
    def get_entity_profile(
        self, entity_id: int
    ) -> tuple[int, float, str | None, int, float, str | None] | None:
        """Return (cluster_id, reputation_score, risk_history_json, last_updated, decay_factor, reason_tags_json) or None."""
        ...

    @abstractmethod
    def get_entity_profile_by_cluster(
        self, cluster_id: int
    ) -> tuple[int, float, str | None, int, float, str | None] | None:
        """Return (entity_id, reputation_score, risk_history_json, last_updated, decay_factor, reason_tags_json) or None."""
        ...

    @abstractmethod
    def insert_entity_reputation_history(
        self,
        entity_id: int,
        reputation_score: float,
        reason_tags_json: str | None,
        snapshot_at: int,
    ) -> int:
        """Append historical snapshot. Returns row id."""
        ...

    @abstractmethod
    def get_entity_reputation_history(
        self,
        entity_id: int,
        *,
        limit: int = 100,
        since_ts: int | None = None,
    ) -> list[tuple[float, str | None, int]]:
        """Return (reputation_score, reason_tags_json, snapshot_at) newest first."""
        ...


# -----------------------------------------------------------------------------
# SQLite backend
# -----------------------------------------------------------------------------


class SQLiteBackend(DatabaseBackend):
    """SQLite implementation; single file, one connection per operation for MVP."""

    def __init__(self, path: str | Path, *, timeout_sec: float = 5.0) -> None:
        self._path = Path(path)
        self._timeout_sec = timeout_sec

    def _connect(self) -> sqlite3.Connection:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._path), timeout=self._timeout_sec)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        return conn

    @contextmanager
    def _cursor(self):
        conn = self._connect()
        try:
            cur = conn.cursor()
            yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def ensure_schema(self) -> None:
        with self._cursor() as cur:
            for stmt in (
                SCHEMA_WALLET_PROFILES,
                SCHEMA_TRANSACTIONS,
                SCHEMA_TRUST_SCORES,
                SCHEMA_TRACKED_WALLETS,
                SCHEMA_ALERTS,
                SCHEMA_WALLET_ROLLING_STATS,
                SCHEMA_WALLET_ESCALATION_STATE,
                SCHEMA_WALLET_PRIORITY,
                SCHEMA_WALLET_REPUTATION_STATE,
                SCHEMA_WALLET_GRAPH_EDGES,
                SCHEMA_WALLET_CLUSTERS,
                SCHEMA_WALLET_CLUSTER_MEMBERS,
                SCHEMA_ENTITY_PROFILES,
                SCHEMA_ENTITY_REPUTATION_HISTORY,
            ):
                cur.executescript(stmt)
            cur.execute("PRAGMA table_info(tracked_wallets)")
            columns = [row[1] for row in cur.fetchall()]
            if "priority" not in columns:
                cur.execute("ALTER TABLE tracked_wallets ADD COLUMN priority TEXT DEFAULT 'normal'")
                cur.execute("UPDATE tracked_wallets SET priority = 'normal' WHERE priority IS NULL")
            if "last_analyzed_at" not in columns:
                cur.execute("ALTER TABLE tracked_wallets ADD COLUMN last_analyzed_at INTEGER")

    def upsert_wallet_profile(self, profile: WalletProfile) -> None:
        now = int(time.time())
        created = now
        updated = now
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_profiles (wallet, first_seen_at, last_seen_at, profile_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    last_seen_at = excluded.last_seen_at,
                    profile_json = COALESCE(excluded.profile_json, profile_json),
                    updated_at = excluded.updated_at
                """,
                (
                    profile.wallet,
                    profile.first_seen_at,
                    profile.last_seen_at,
                    profile.profile_json,
                    created,
                    updated,
                ),
            )

    def get_wallet_profile(self, wallet: str) -> WalletProfile | None:
        with self._cursor() as cur:
            cur.execute(
                "SELECT wallet, first_seen_at, last_seen_at, profile_json, created_at, updated_at FROM wallet_profiles WHERE wallet = ?",
                (wallet,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return WalletProfile(
            wallet=row["wallet"],
            first_seen_at=row["first_seen_at"],
            last_seen_at=row["last_seen_at"],
            profile_json=row["profile_json"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
        )

    def insert_transactions(
        self,
        wallet: str,
        records: list[tuple[str, str, str, int, int | None, int | None]],
    ) -> int:
        if not records:
            return 0
        now = int(time.time())
        inserted = 0
        with self._cursor() as cur:
            for sig, sender, receiver, amount_lamports, timestamp, slot in records:
                try:
                    cur.execute(
                        """
                        INSERT INTO transactions (wallet, signature, sender, receiver, amount_lamports, timestamp, slot, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (wallet, sig, sender, receiver, amount_lamports, timestamp, slot, now),
                    )
                    inserted += cur.rowcount
                except sqlite3.IntegrityError:
                    # UNIQUE(wallet, signature) duplicate
                    pass
        return inserted

    def get_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 500,
        since_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> list[TransactionRecord]:
        sql = """
            SELECT id, wallet, signature, sender, receiver, amount_lamports, timestamp, slot, created_at
            FROM transactions WHERE wallet = ?
        """
        params: list[Any] = [wallet]
        if since_timestamp is not None:
            sql += " AND timestamp >= ?"
            params.append(since_timestamp)
        if until_timestamp is not None:
            sql += " AND timestamp <= ?"
            params.append(until_timestamp)
        sql += " ORDER BY COALESCE(timestamp, 0) DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            TransactionRecord(
                id=row["id"],
                wallet=row["wallet"],
                signature=row["signature"],
                sender=row["sender"],
                receiver=row["receiver"],
                amount_lamports=row["amount_lamports"],
                timestamp=row["timestamp"],
                slot=row["slot"],
                created_at=row["created_at"],
            )
            for row in rows
        ]

    def insert_trust_score(
        self,
        wallet: str,
        score: float,
        computed_at: int,
        metadata_json: str | None = None,
    ) -> int:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO trust_scores (wallet, score, computed_at, metadata_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (wallet, score, computed_at, metadata_json, now),
            )
            return cur.lastrowid or 0

    def get_trust_score_timeline(
        self,
        wallet: str,
        *,
        limit: int = 100,
        since_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> list[TrustScoreRecord]:
        sql = """
            SELECT id, wallet, score, computed_at, metadata_json
            FROM trust_scores WHERE wallet = ?
        """
        params: list[Any] = [wallet]
        if since_timestamp is not None:
            sql += " AND computed_at >= ?"
            params.append(since_timestamp)
        if until_timestamp is not None:
            sql += " AND computed_at <= ?"
            params.append(until_timestamp)
        sql += " ORDER BY computed_at DESC LIMIT ?"
        params.append(limit)
        with self._cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            TrustScoreRecord(
                id=row["id"],
                wallet=row["wallet"],
                score=row["score"],
                computed_at=row["computed_at"],
                metadata_json=row["metadata_json"],
            )
            for row in rows
        ]

    def get_tracked_wallets(self, *, limit: int = 5000) -> list[str]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT wallet FROM wallet_profiles ORDER BY last_seen_at DESC LIMIT ?",
                (limit,),
            )
            return [row["wallet"] for row in cur.fetchall()]

    def add_tracked_wallet(self, wallet: str, priority: str = "normal") -> bool:
        now = int(time.time())
        with self._cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO tracked_wallets (wallet, created_at, priority, last_analyzed_at)
                    VALUES (?, ?, ?, NULL)
                    """,
                    (wallet.strip(), now, (priority or "normal").strip().lower()),
                )
                return cur.rowcount > 0
            except sqlite3.OperationalError:
                cur.execute(
                    "INSERT INTO tracked_wallets (wallet, created_at) VALUES (?, ?)",
                    (wallet.strip(), now),
                )
                return cur.rowcount > 0
            except sqlite3.IntegrityError:
                return False

    def get_tracked_wallet_created_at(self, wallet: str) -> int | None:
        with self._cursor() as cur:
            cur.execute(
                "SELECT created_at FROM tracked_wallets WHERE wallet = ?",
                (wallet.strip(),),
            )
            row = cur.fetchone()
        return int(row["created_at"]) if row is not None else None

    def get_tracked_wallets_with_priority_and_analyzed(
        self, *, limit: int = 50000
    ) -> list[tuple[str, str, int | None]]:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT wallet, COALESCE(priority, 'normal') AS priority, last_analyzed_at
                FROM tracked_wallets
                ORDER BY created_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [
            (
                row["wallet"],
                (row["priority"] or "normal").lower(),
                int(row["last_analyzed_at"]) if row["last_analyzed_at"] is not None else None,
            )
            for row in rows
        ]

    def update_tracked_wallet_priority(self, wallet: str, priority: str) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE tracked_wallets SET priority = ? WHERE wallet = ?",
                ((priority or "normal").strip().lower(), wallet.strip()),
            )

    def update_tracked_wallet_last_analyzed(self, wallet: str, last_analyzed_at: int) -> None:
        with self._cursor() as cur:
            cur.execute(
                "UPDATE tracked_wallets SET last_analyzed_at = ? WHERE wallet = ?",
                (last_analyzed_at, wallet.strip()),
            )

    def get_tracked_wallet_addresses(self, *, limit: int = 10000) -> list[str]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT wallet FROM tracked_wallets ORDER BY created_at ASC LIMIT ?",
                (limit,),
            )
            return [row["wallet"] for row in cur.fetchall()]

    def insert_alert(self, wallet: str, severity: str, reason: str, created_at: int) -> int:
        with self._cursor() as cur:
            cur.execute(
                "INSERT INTO alerts (wallet, severity, reason, created_at) VALUES (?, ?, ?, ?)",
                (wallet.strip(), severity.strip(), reason.strip(), created_at),
            )
            return cur.lastrowid or 0

    def has_recent_alert(
        self,
        wallet: str,
        severity: str,
        reason: str,
        since_created_at: int,
    ) -> bool:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM alerts
                WHERE wallet = ? AND severity = ? AND reason = ? AND created_at >= ?
                LIMIT 1
                """,
                (wallet.strip(), severity.strip(), reason.strip(), since_created_at),
            )
            return cur.fetchone() is not None

    def get_alert_count(
        self,
        wallet: str,
        since_created_at: int,
        until_created_at: int | None = None,
    ) -> int:
        sql = "SELECT COUNT(*) FROM alerts WHERE wallet = ? AND created_at >= ?"
        params: list[Any] = [wallet.strip(), since_created_at]
        if until_created_at is not None:
            sql += " AND created_at <= ?"
            params.append(until_created_at)
        with self._cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        return int(row[0]) if row else 0

    def insert_wallet_rolling_stats(
        self,
        wallet: str,
        period_end_ts: int,
        window_days: int,
        volume_lamports: int,
        tx_count: int,
        anomaly_count: int,
        avg_trust_score: float | None,
        alert_count: int,
    ) -> int:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_rolling_stats
                (wallet, period_end_ts, window_days, volume_lamports, tx_count, anomaly_count, avg_trust_score, alert_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    wallet.strip(),
                    period_end_ts,
                    window_days,
                    volume_lamports,
                    tx_count,
                    anomaly_count,
                    avg_trust_score,
                    alert_count,
                    now,
                ),
            )
            return cur.lastrowid or 0

    def get_wallet_rolling_stats_history(
        self,
        wallet: str,
        window_days: int,
        *,
        limit: int = 32,
    ) -> list[tuple[int, int, int, int, float | None, int]]:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT period_end_ts, volume_lamports, tx_count, anomaly_count, avg_trust_score, alert_count
                FROM wallet_rolling_stats
                WHERE wallet = ? AND window_days = ?
                ORDER BY period_end_ts DESC
                LIMIT ?
                """,
                (wallet.strip(), window_days, limit),
            )
            rows = cur.fetchall()
        return [
            (
                int(row["period_end_ts"]),
                int(row["volume_lamports"]),
                int(row["tx_count"]),
                int(row["anomaly_count"]),
                float(row["avg_trust_score"]) if row["avg_trust_score"] is not None else None,
                int(row["alert_count"]),
            )
            for row in rows
        ]

    def get_alerts_for_wallet(
        self,
        wallet: str,
        since_created_at: int,
        until_created_at: int | None = None,
        limit: int = 200,
    ) -> list[tuple[int, str, str]]:
        sql = """
            SELECT created_at, severity, reason FROM alerts
            WHERE wallet = ? AND created_at >= ?
        """
        params: list[Any] = [wallet.strip(), since_created_at]
        if until_created_at is not None:
            sql += " AND created_at <= ?"
            params.append(until_created_at)
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [(int(row["created_at"]), row["severity"], row["reason"]) for row in rows]

    def get_escalation_state(
        self,
        wallet: str,
    ) -> tuple[str, float, int | None, int | None, str | None, int] | None:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT risk_stage, escalation_score, last_alert_ts, last_clean_ts, state_json, updated_at
                FROM wallet_escalation_state WHERE wallet = ?
                """,
                (wallet.strip(),),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return (
            row["risk_stage"],
            float(row["escalation_score"]),
            int(row["last_alert_ts"]) if row["last_alert_ts"] is not None else None,
            int(row["last_clean_ts"]) if row["last_clean_ts"] is not None else None,
            row["state_json"],
            int(row["updated_at"]),
        )

    def upsert_escalation_state(
        self,
        wallet: str,
        risk_stage: str,
        escalation_score: float,
        last_alert_ts: int | None,
        last_clean_ts: int | None,
        state_json: str | None,
    ) -> None:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_escalation_state
                (wallet, risk_stage, escalation_score, last_alert_ts, last_clean_ts, state_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    risk_stage = excluded.risk_stage,
                    escalation_score = excluded.escalation_score,
                    last_alert_ts = excluded.last_alert_ts,
                    last_clean_ts = excluded.last_clean_ts,
                    state_json = excluded.state_json,
                    updated_at = excluded.updated_at
                """,
                (
                    wallet.strip(),
                    risk_stage,
                    escalation_score,
                    last_alert_ts,
                    last_clean_ts,
                    state_json,
                    now,
                ),
            )

    def get_wallet_priority(self, wallet: str) -> str | None:
        with self._cursor() as cur:
            cur.execute(
                "SELECT tier FROM wallet_priority WHERE wallet = ?",
                (wallet.strip(),),
            )
            row = cur.fetchone()
        return row["tier"] if row is not None else None

    def set_wallet_priority(self, wallet: str, tier: str) -> None:
        now = int(time.time())
        tier_lower = (tier or "normal").strip().lower()
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_priority (wallet, tier, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET tier = excluded.tier, updated_at = excluded.updated_at
                """,
                (wallet.strip(), tier_lower, now),
            )
            cur.execute(
                "UPDATE tracked_wallets SET priority = ? WHERE wallet = ?",
                (tier_lower, wallet.strip()),
            )

    def get_wallet_priorities_for_wallets(self, wallets: list[str]) -> dict[str, str]:
        if not wallets:
            return {}
        placeholders = ",".join("?" for _ in wallets)
        params = [w.strip() for w in wallets]
        with self._cursor() as cur:
            cur.execute(
                f"SELECT wallet, tier FROM wallet_priority WHERE wallet IN ({placeholders})",
                params,
            )
            rows = cur.fetchall()
        return {row["wallet"]: row["tier"] for row in rows}

    def get_wallet_reputation_state(
        self,
        wallet: str,
    ) -> tuple[float, float | None, float | None, str, float | None, float, int] | None:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT current_score, avg_7d, avg_30d, trend, volatility, decay_factor, updated_at
                FROM wallet_reputation_state WHERE wallet = ?
                """,
                (wallet.strip(),),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return (
            float(row["current_score"]),
            float(row["avg_7d"]) if row["avg_7d"] is not None else None,
            float(row["avg_30d"]) if row["avg_30d"] is not None else None,
            row["trend"],
            float(row["volatility"]) if row["volatility"] is not None else None,
            float(row["decay_factor"]),
            int(row["updated_at"]),
        )

    def upsert_wallet_reputation_state(
        self,
        wallet: str,
        current_score: float,
        avg_7d: float | None,
        avg_30d: float | None,
        trend: str,
        volatility: float | None,
        decay_factor: float,
    ) -> None:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_reputation_state
                (wallet, current_score, avg_7d, avg_30d, trend, volatility, decay_factor, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    current_score = excluded.current_score,
                    avg_7d = excluded.avg_7d,
                    avg_30d = excluded.avg_30d,
                    trend = excluded.trend,
                    volatility = excluded.volatility,
                    decay_factor = excluded.decay_factor,
                    updated_at = excluded.updated_at
                """,
                (
                    wallet.strip(),
                    current_score,
                    avg_7d,
                    avg_30d,
                    trend.strip().lower(),
                    volatility,
                    decay_factor,
                    now,
                ),
            )

    def upsert_wallet_graph_edge(
        self,
        sender_wallet: str,
        receiver_wallet: str,
        amount_lamports: int,
        timestamp: int,
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_graph_edges
                (sender_wallet, receiver_wallet, tx_count, total_volume, last_seen_timestamp)
                VALUES (?, ?, 1, ?, ?)
                ON CONFLICT(sender_wallet, receiver_wallet) DO UPDATE SET
                    tx_count = tx_count + 1,
                    total_volume = total_volume + ?,
                    last_seen_timestamp = CASE
                        WHEN last_seen_timestamp >= ? THEN last_seen_timestamp
                        ELSE ?
                    END
                """,
                (
                    sender_wallet.strip(),
                    receiver_wallet.strip(),
                    amount_lamports,
                    timestamp,
                    amount_lamports,
                    timestamp,
                    timestamp,
                ),
            )

    def get_wallet_graph_adjacent(self, wallet: str) -> list[str]:
        w = wallet.strip()
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT receiver_wallet AS other FROM wallet_graph_edges WHERE sender_wallet = ?
                UNION
                SELECT sender_wallet AS other FROM wallet_graph_edges WHERE receiver_wallet = ?
                """,
                (w, w),
            )
            rows = cur.fetchall()
        return [row["other"] for row in rows]

    def get_wallet_graph_edges_all(
        self, limit: int = 50000
    ) -> list[tuple[str, str, int, int, int]]:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT sender_wallet, receiver_wallet, tx_count, total_volume, last_seen_timestamp
                FROM wallet_graph_edges
                ORDER BY last_seen_timestamp DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = cur.fetchall()
        return [
            (row["sender_wallet"], row["receiver_wallet"], row["tx_count"], row["total_volume"], row["last_seen_timestamp"])
            for row in rows
        ]

    def insert_wallet_cluster(
        self, confidence_score: float, reason_tags_json: str | None
    ) -> int:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO wallet_clusters (confidence_score, reason_tags, updated_at)
                VALUES (?, ?, ?)
                """,
                (confidence_score, reason_tags_json, now),
            )
            return cur.lastrowid or 0

    def insert_wallet_cluster_member(self, cluster_id: int, wallet: str) -> None:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT OR IGNORE INTO wallet_cluster_members (cluster_id, wallet, added_at)
                VALUES (?, ?, ?)
                """,
                (cluster_id, wallet.strip(), now),
            )

    def get_cluster_members(self, cluster_id: int) -> list[str]:
        with self._cursor() as cur:
            cur.execute(
                "SELECT wallet FROM wallet_cluster_members WHERE cluster_id = ? ORDER BY added_at",
                (cluster_id,),
            )
            return [row["wallet"] for row in cur.fetchall()]

    def get_cluster_for_wallet(
        self, wallet: str
    ) -> tuple[int, float, str | None, float | None] | None:
        w = wallet.strip()
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT c.cluster_id, c.confidence_score, c.reason_tags, c.cluster_risk
                FROM wallet_clusters c
                JOIN wallet_cluster_members m ON c.cluster_id = m.cluster_id
                WHERE m.wallet = ?
                """,
                (w,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return (
            row["cluster_id"],
            float(row["confidence_score"]),
            row["reason_tags"],
            float(row["cluster_risk"]) if row["cluster_risk"] is not None else None,
        )

    def get_all_clusters(
        self,
    ) -> list[tuple[int, float, str | None, float | None, int | None]]:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT cluster_id, confidence_score, reason_tags, cluster_risk, risk_updated_at
                FROM wallet_clusters
                ORDER BY cluster_id
                """
            )
            rows = cur.fetchall()
        return [
            (
                row["cluster_id"],
                float(row["confidence_score"]),
                row["reason_tags"],
                float(row["cluster_risk"]) if row["cluster_risk"] is not None else None,
                row["risk_updated_at"],
            )
            for row in rows
        ]

    def update_cluster_confidence(
        self, cluster_id: int, confidence_score: float, reason_tags_json: str | None
    ) -> None:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE wallet_clusters
                SET confidence_score = ?, reason_tags = ?, updated_at = ?
                WHERE cluster_id = ?
                """,
                (confidence_score, reason_tags_json, now, cluster_id),
            )

    def update_cluster_risk(self, cluster_id: int, cluster_risk: float) -> None:
        now = int(time.time())
        with self._cursor() as cur:
            cur.execute(
                """
                UPDATE wallet_clusters
                SET cluster_risk = ?, risk_updated_at = ?
                WHERE cluster_id = ?
                """,
                (cluster_risk, now, cluster_id),
            )

    def delete_all_wallet_clusters(self) -> None:
        with self._cursor() as cur:
            cur.execute("DELETE FROM wallet_cluster_members")
            cur.execute("DELETE FROM wallet_clusters")

    def upsert_entity_profile(
        self,
        entity_id: int,
        cluster_id: int,
        reputation_score: float,
        risk_history_json: str | None,
        last_updated: int,
        decay_factor: float,
        reason_tags_json: str | None,
    ) -> None:
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO entity_profiles
                (entity_id, cluster_id, reputation_score, risk_history, last_updated, decay_factor, reason_tags)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entity_id) DO UPDATE SET
                    cluster_id = excluded.cluster_id,
                    reputation_score = excluded.reputation_score,
                    risk_history = excluded.risk_history,
                    last_updated = excluded.last_updated,
                    decay_factor = excluded.decay_factor,
                    reason_tags = excluded.reason_tags
                """,
                (
                    entity_id,
                    cluster_id,
                    reputation_score,
                    risk_history_json,
                    last_updated,
                    decay_factor,
                    reason_tags_json,
                ),
            )

    def get_entity_profile(
        self, entity_id: int
    ) -> tuple[int, float, str | None, int, float, str | None] | None:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT cluster_id, reputation_score, risk_history, last_updated, decay_factor, reason_tags
                FROM entity_profiles WHERE entity_id = ?
                """,
                (entity_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return (
            row["cluster_id"],
            float(row["reputation_score"]),
            row["risk_history"],
            int(row["last_updated"]),
            float(row["decay_factor"]),
            row["reason_tags"],
        )

    def get_entity_profile_by_cluster(
        self, cluster_id: int
    ) -> tuple[int, float, str | None, int, float, str | None] | None:
        with self._cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, reputation_score, risk_history, last_updated, decay_factor, reason_tags
                FROM entity_profiles WHERE cluster_id = ?
                """,
                (cluster_id,),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return (
            row["entity_id"],
            float(row["reputation_score"]),
            row["risk_history"],
            int(row["last_updated"]),
            float(row["decay_factor"]),
            row["reason_tags"],
        )

    def insert_entity_reputation_history(
        self,
        entity_id: int,
        reputation_score: float,
        reason_tags_json: str | None,
        snapshot_at: int,
    ) -> int:
        with self._cursor() as cur:
            cur.execute(
                """
                INSERT INTO entity_reputation_history (entity_id, reputation_score, reason_tags, snapshot_at)
                VALUES (?, ?, ?, ?)
                """,
                (entity_id, reputation_score, reason_tags_json, snapshot_at),
            )
            return cur.lastrowid or 0

    def get_entity_reputation_history(
        self,
        entity_id: int,
        *,
        limit: int = 100,
        since_ts: int | None = None,
    ) -> list[tuple[float, str | None, int]]:
        sql = """
            SELECT reputation_score, reason_tags, snapshot_at
            FROM entity_reputation_history
            WHERE entity_id = ?
        """
        params: list[Any] = [entity_id]
        if since_ts is not None:
            sql += " AND snapshot_at >= ?"
            params.append(since_ts)
        sql += " ORDER BY snapshot_at DESC LIMIT ?"
        params.append(limit)
        with self._cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            (float(r["reputation_score"]), r["reason_tags"], int(r["snapshot_at"]))
            for r in rows
        ]


# -----------------------------------------------------------------------------
# Database facade: single entrypoint; backend is swappable.
# -----------------------------------------------------------------------------


class Database:
    """
    Database abstraction: wallet profiles, transaction history, trust score timeline.

    Uses a Backend (SQLite for MVP); replace with PostgreSQLBackend when upgrading.
    """

    def __init__(self, backend: DatabaseBackend) -> None:
        self._backend = backend

    def ensure_schema(self) -> None:
        """Create tables and indexes if they do not exist."""
        self._backend.ensure_schema()

    # --- Wallet profiles ---

    def upsert_wallet_profile(self, profile: WalletProfile) -> None:
        self._backend.upsert_wallet_profile(profile)

    def get_wallet_profile(self, wallet: str) -> WalletProfile | None:
        return self._backend.get_wallet_profile(wallet)

    # --- Transaction history ---

    def insert_transactions(
        self,
        wallet: str,
        records: list[tuple[str, str, str, int, int | None, int | None]],
    ) -> int:
        """Insert rows (signature, sender, receiver, amount_lamports, timestamp, slot). Returns count inserted."""
        return self._backend.insert_transactions(wallet, records)

    def insert_parsed_transactions(self, wallet: str, txs: list[Any]) -> int:
        """
        Insert from a list of ParsedTransaction-like objects (signature, sender, receiver, amount, timestamp, slot).
        Returns count inserted. Duplicates (wallet+signature) are skipped.
        """
        from backend_blockid.solana_listener.parser import ParsedTransaction

        rows = []
        for tx in txs:
            if isinstance(tx, ParsedTransaction):
                sig = (tx.signature or "").strip()
                if not sig:
                    continue
                rows.append(
                    (sig, tx.sender, tx.receiver, tx.amount, tx.timestamp, tx.slot)
                )
            elif isinstance(tx, (list, tuple)) and len(tx) >= 6:
                sig = (str(tx[0]) or "").strip()
                if sig:
                    rows.append((sig, str(tx[1]), str(tx[2]), int(tx[3]), tx[4] if len(tx) > 4 else None, tx[5] if len(tx) > 5 else None))
            elif hasattr(tx, "signature") and hasattr(tx, "sender"):
                sig = (getattr(tx, "signature", None) or "").strip()
                if not sig:
                    continue
                rows.append(
                    (
                        sig,
                        getattr(tx, "sender", ""),
                        getattr(tx, "receiver", ""),
                        getattr(tx, "amount", 0),
                        getattr(tx, "timestamp", None),
                        getattr(tx, "slot", None),
                    )
                )
        return self._backend.insert_transactions(wallet, rows) if rows else 0

    def get_transaction_history(
        self,
        wallet: str,
        *,
        limit: int = 500,
        since_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> list[TransactionRecord]:
        return self._backend.get_transaction_history(
            wallet, limit=limit, since_timestamp=since_timestamp, until_timestamp=until_timestamp
        )

    # --- Trust score timeline ---

    def insert_trust_score(
        self,
        wallet: str,
        score: float,
        computed_at: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        """Append a trust score. computed_at defaults to now; metadata serialized to JSON. Returns row id."""
        now = int(time.time())
        computed_at = computed_at if computed_at is not None else now
        metadata_json = json.dumps(metadata) if metadata else None
        return self._backend.insert_trust_score(wallet, score, computed_at, metadata_json)

    def get_trust_score_timeline(
        self,
        wallet: str,
        *,
        limit: int = 100,
        since_timestamp: int | None = None,
        until_timestamp: int | None = None,
    ) -> list[TrustScoreRecord]:
        return self._backend.get_trust_score_timeline(
            wallet, limit=limit, since_timestamp=since_timestamp, until_timestamp=until_timestamp
        )

    def get_tracked_wallets(self, *, limit: int = 5000) -> list[str]:
        """Return wallet addresses from wallet_profiles, most recently seen first."""
        return self._backend.get_tracked_wallets(limit=limit)

    def add_tracked_wallet(self, wallet: str) -> bool:
        """Register a wallet for monitoring. Returns True if inserted, False if already present."""
        return self._backend.add_tracked_wallet(wallet)

    def get_tracked_wallet_created_at(self, wallet: str) -> int | None:
        """Return created_at for a tracked wallet, or None if not found."""
        return self._backend.get_tracked_wallet_created_at(wallet)

    def get_tracked_wallet_addresses(self, *, limit: int = 10000) -> list[str]:
        """Return wallet addresses from tracked_wallets registry."""
        return self._backend.get_tracked_wallet_addresses(limit=limit)

    def get_tracked_wallets_with_priority_and_analyzed(
        self, *, limit: int = 50000
    ) -> list[tuple[str, str, int | None]]:
        """Return (wallet, priority, last_analyzed_at) for all tracked wallets, oldest first."""
        return self._backend.get_tracked_wallets_with_priority_and_analyzed(limit=limit)

    def update_tracked_wallet_priority(self, wallet: str, priority: str) -> None:
        """Set priority (critical | watchlist | normal) for a tracked wallet."""
        self._backend.update_tracked_wallet_priority(wallet, priority)

    def update_tracked_wallet_last_analyzed(self, wallet: str, last_analyzed_at: int) -> None:
        """Set last_analyzed_at (Unix timestamp) for a tracked wallet."""
        self._backend.update_tracked_wallet_last_analyzed(wallet, last_analyzed_at)

    def get_latest_trust_scores_for_wallets(
        self, wallets: list[str]
    ) -> dict[str, TrustScoreRecord | None]:
        """
        Return latest trust score per wallet. Keys are input wallets;
        value is latest TrustScoreRecord or None if no score exists.
        """
        out: dict[str, TrustScoreRecord | None] = {}
        for w in wallets:
            timeline = self.get_trust_score_timeline(w, limit=1)
            out[w] = timeline[0] if timeline else None
        return out

    def get_wallet_profiles_for_wallets(
        self, wallets: list[str]
    ) -> dict[str, WalletProfile | None]:
        """
        Return wallet profile per wallet. Keys are input wallets;
        value is WalletProfile or None if no profile exists.
        """
        out: dict[str, WalletProfile | None] = {}
        for w in wallets:
            out[w] = self.get_wallet_profile(w)
        return out

    def insert_alert(self, wallet: str, severity: str, reason: str, created_at: int | None = None) -> int:
        """Insert an alert. created_at defaults to now. Returns row id."""
        now = int(time.time())
        created_at = created_at if created_at is not None else now
        return self._backend.insert_alert(wallet, severity, reason, created_at)

    def has_recent_alert(
        self,
        wallet: str,
        severity: str,
        reason: str,
        since_created_at: int,
    ) -> bool:
        """True if an alert (wallet, severity, reason) exists with created_at >= since_created_at."""
        return self._backend.has_recent_alert(wallet, severity, reason, since_created_at)

    def get_alert_count(
        self,
        wallet: str,
        since_created_at: int,
        until_created_at: int | None = None,
    ) -> int:
        """Count alerts for wallet with created_at in [since_created_at, until_created_at] (until optional)."""
        return self._backend.get_alert_count(wallet, since_created_at, until_created_at)

    def insert_wallet_rolling_stats(
        self,
        wallet: str,
        period_end_ts: int,
        window_days: int,
        volume_lamports: int,
        tx_count: int,
        anomaly_count: int,
        avg_trust_score: float | None,
        alert_count: int,
    ) -> int:
        """Append one rolling stats snapshot. Returns row id."""
        return self._backend.insert_wallet_rolling_stats(
            wallet,
            period_end_ts,
            window_days,
            volume_lamports,
            tx_count,
            anomaly_count,
            avg_trust_score,
            alert_count,
        )

    def get_wallet_rolling_stats_history(
        self,
        wallet: str,
        window_days: int,
        *,
        limit: int = 32,
    ) -> list[tuple[int, int, int, int, float | None, int]]:
        """Return (period_end_ts, volume_lamports, tx_count, anomaly_count, avg_trust_score, alert_count) newest first."""
        return self._backend.get_wallet_rolling_stats_history(wallet, window_days, limit=limit)

    def get_alerts_for_wallet(
        self,
        wallet: str,
        since_created_at: int,
        until_created_at: int | None = None,
        limit: int = 200,
    ) -> list[tuple[int, str, str]]:
        """Return (created_at, severity, reason) for wallet, newest first."""
        return self._backend.get_alerts_for_wallet(
            wallet, since_created_at, until_created_at=until_created_at, limit=limit
        )

    def get_escalation_state(
        self,
        wallet: str,
    ) -> tuple[str, float, int | None, int | None, str | None, int] | None:
        """Return (risk_stage, escalation_score, last_alert_ts, last_clean_ts, state_json, updated_at) or None."""
        return self._backend.get_escalation_state(wallet)

    def upsert_escalation_state(
        self,
        wallet: str,
        risk_stage: str,
        escalation_score: float,
        last_alert_ts: int | None,
        last_clean_ts: int | None,
        state_json: str | None,
    ) -> None:
        """Insert or update escalation state for wallet."""
        self._backend.upsert_escalation_state(
            wallet, risk_stage, escalation_score, last_alert_ts, last_clean_ts, state_json
        )

    def get_wallet_priority(self, wallet: str) -> str | None:
        """Return tier (critical | watchlist | normal) for wallet, or None if not set (default normal)."""
        return self._backend.get_wallet_priority(wallet)

    def set_wallet_priority(self, wallet: str, tier: str) -> None:
        """Set wallet priority tier (critical | watchlist | normal)."""
        self._backend.set_wallet_priority(wallet, tier)

    def get_wallet_priorities_for_wallets(self, wallets: list[str]) -> dict[str, str]:
        """Return dict wallet -> tier for given wallets; missing wallets default to normal in scheduler."""
        return self._backend.get_wallet_priorities_for_wallets(wallets)

    def get_wallet_reputation_state(
        self,
        wallet: str,
    ) -> tuple[float, float | None, float | None, str, float | None, float, int] | None:
        """Return (current_score, avg_7d, avg_30d, trend, volatility, decay_factor, updated_at) or None."""
        return self._backend.get_wallet_reputation_state(wallet)

    def upsert_wallet_reputation_state(
        self,
        wallet: str,
        current_score: float,
        avg_7d: float | None,
        avg_30d: float | None,
        trend: str,
        volatility: float | None,
        decay_factor: float,
    ) -> None:
        """Insert or update reputation state for wallet."""
        self._backend.upsert_wallet_reputation_state(
            wallet, current_score, avg_7d, avg_30d, trend, volatility, decay_factor
        )

    def upsert_wallet_graph_edge(
        self,
        sender_wallet: str,
        receiver_wallet: str,
        amount_lamports: int,
        timestamp: int,
    ) -> None:
        """Increment edge stats for sender -> receiver. Idempotent per tx if called once per tx."""
        self._backend.upsert_wallet_graph_edge(
            sender_wallet, receiver_wallet, amount_lamports, timestamp
        )

    def get_wallet_graph_adjacent(self, wallet: str) -> list[str]:
        """Return distinct wallets that share an edge with wallet (sender or receiver)."""
        return self._backend.get_wallet_graph_adjacent(wallet)

    def get_wallet_graph_edges_all(
        self, limit: int = 50000
    ) -> list[tuple[str, str, int, int, int]]:
        """Return (sender, receiver, tx_count, total_volume, last_seen_timestamp) for clustering."""
        return self._backend.get_wallet_graph_edges_all(limit=limit)

    def insert_wallet_cluster(
        self, confidence_score: float, reason_tags_json: str | None
    ) -> int:
        """Insert a cluster; return cluster_id."""
        return self._backend.insert_wallet_cluster(confidence_score, reason_tags_json)

    def insert_wallet_cluster_member(self, cluster_id: int, wallet: str) -> None:
        """Add wallet to cluster. Idempotent."""
        self._backend.insert_wallet_cluster_member(cluster_id, wallet)

    def get_cluster_members(self, cluster_id: int) -> list[str]:
        """Return wallet addresses in the cluster."""
        return self._backend.get_cluster_members(cluster_id)

    def get_cluster_for_wallet(
        self, wallet: str
    ) -> tuple[int, float, str | None, float | None] | None:
        """Return (cluster_id, confidence_score, reason_tags_json, cluster_risk) or None."""
        return self._backend.get_cluster_for_wallet(wallet)

    def get_all_clusters(
        self,
    ) -> list[tuple[int, float, str | None, float | None, int | None]]:
        """Return (cluster_id, confidence_score, reason_tags_json, cluster_risk, risk_updated_at)."""
        return self._backend.get_all_clusters()

    def update_cluster_confidence(
        self, cluster_id: int, confidence_score: float, reason_tags_json: str | None
    ) -> None:
        """Update cluster confidence and reason tags."""
        self._backend.update_cluster_confidence(
            cluster_id, confidence_score, reason_tags_json
        )

    def update_cluster_risk(self, cluster_id: int, cluster_risk: float) -> None:
        """Update stored cluster risk."""
        self._backend.update_cluster_risk(cluster_id, cluster_risk)

    def delete_all_wallet_clusters(self) -> None:
        """Remove all clusters and members; used before full recompute."""
        self._backend.delete_all_wallet_clusters()

    def upsert_entity_profile(
        self,
        entity_id: int,
        cluster_id: int,
        reputation_score: float,
        risk_history_json: str | None,
        last_updated: int,
        decay_factor: float,
        reason_tags_json: str | None,
    ) -> None:
        """Insert or update entity profile (entity_id = cluster_id for 1:1)."""
        self._backend.upsert_entity_profile(
            entity_id,
            cluster_id,
            reputation_score,
            risk_history_json,
            last_updated,
            decay_factor,
            reason_tags_json,
        )

    def get_entity_profile(
        self, entity_id: int
    ) -> tuple[int, float, str | None, int, float, str | None] | None:
        """Return (cluster_id, reputation_score, risk_history_json, last_updated, decay_factor, reason_tags_json) or None."""
        return self._backend.get_entity_profile(entity_id)

    def get_entity_profile_by_cluster(
        self, cluster_id: int
    ) -> tuple[int, float, str | None, int, float, str | None] | None:
        """Return (entity_id, reputation_score, risk_history_json, last_updated, decay_factor, reason_tags_json) or None."""
        return self._backend.get_entity_profile_by_cluster(cluster_id)

    def insert_entity_reputation_history(
        self,
        entity_id: int,
        reputation_score: float,
        reason_tags_json: str | None,
        snapshot_at: int,
    ) -> int:
        """Append historical snapshot. Returns row id."""
        return self._backend.insert_entity_reputation_history(
            entity_id, reputation_score, reason_tags_json, snapshot_at
        )

    def get_entity_reputation_history(
        self,
        entity_id: int,
        *,
        limit: int = 100,
        since_ts: int | None = None,
    ) -> list[tuple[float, str | None, int]]:
        """Return (reputation_score, reason_tags_json, snapshot_at) newest first."""
        return self._backend.get_entity_reputation_history(
            entity_id, limit=limit, since_ts=since_ts
        )


def get_database(path: str | Path | None = None) -> Database:
    """
    Return a Database instance for MVP (SQLite).

    path: Path to the SQLite file (e.g. "data/blockid.db"). Default: "blockid.db" in cwd.
    For PostgreSQL later: use a different factory that builds PostgreSQLBackend from URL.
    """
    if path is None:
        path = Path("blockid.db")
    backend = SQLiteBackend(path)
    db = Database(backend)
    db.ensure_schema()
    return db
