"""
Initialize BlockID database tables.

Creates trust_scores, wallet_reasons, scam_wallets, wallet_clusters, wallet_history.
Safe to run multiple times.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

DB_PATH = Path(r"D:/BACKENDBLOCKID/blockid.db")


def _create_tables(cur: sqlite3.Cursor) -> None:
    # trust_scores
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trust_scores (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT UNIQUE,
            score REAL,
            risk_level TEXT,
            reason_codes TEXT,
            confidence_score REAL,
            metadata_json TEXT,
            computed_at INTEGER,
            updated_at INTEGER,
            created_at INTEGER
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_scores_wallet ON trust_scores(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_scores_computed ON trust_scores(computed_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_scores_updated ON trust_scores(updated_at)")

    # wallet_reasons
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_reasons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT,
            reason_code TEXT,
            weight INTEGER,
            created_at INTEGER
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_reasons_wallet ON wallet_reasons(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_reasons_code ON wallet_reasons(reason_code)")

    # scam_wallets
    cur.execute("""
        CREATE TABLE IF NOT EXISTS scam_wallets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT UNIQUE,
            source TEXT,
            label TEXT,
            detected_at INTEGER,
            notes TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_scam_wallets_wallet ON scam_wallets(wallet)")

    # wallet_clusters
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_clusters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT,
            cluster_id INTEGER,
            cluster_type TEXT,
            created_at INTEGER
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_clusters_wallet ON wallet_clusters(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_clusters_cluster ON wallet_clusters(cluster_id)")

    # wallet_history
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT,
            score REAL,
            risk_level TEXT,
            reason_codes TEXT,
            snapshot_at INTEGER
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_history_wallet ON wallet_history(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_history_snapshot ON wallet_history(snapshot_at)")


def main() -> int:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        _create_tables(cur)
        conn.commit()

    print(f"[init_tables] DB ready at {DB_PATH} (ts={int(time.time())})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
