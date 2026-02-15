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
            for stmt in (SCHEMA_WALLET_PROFILES, SCHEMA_TRANSACTIONS, SCHEMA_TRUST_SCORES):
                cur.executescript(stmt)

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
