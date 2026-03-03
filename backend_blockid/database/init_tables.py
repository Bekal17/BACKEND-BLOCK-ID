"""
Initialize BlockID database tables.

Creates trust_scores, wallet_reasons, scam_wallets, wallet_clusters, wallet_history.
Safe to run multiple times.
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(_PROJECT_ROOT / "blockid.db"))).resolve()


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
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_trust_scores_wallet
        ON trust_scores(wallet);
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_scores_wallet ON trust_scores(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_scores_computed ON trust_scores(computed_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_scores_updated ON trust_scores(updated_at)")

    # Reputation decay and graph distance columns (ALTER for existing tables)
    for col, typ in [
        ("wallet_age_days", "INTEGER DEFAULT 0"),
        ("last_scam_days", "INTEGER DEFAULT 9999"),
        ("decay_adjustment", "INTEGER DEFAULT 0"),
        ("graph_distance", "INTEGER DEFAULT 999"),
        ("graph_penalty", "INTEGER DEFAULT 0"),
        ("time_weighted_penalty", "INTEGER DEFAULT 0"),
        ("oldest_risk_days", "INTEGER DEFAULT 0"),
    ]:
        try:
            cur.execute(f"ALTER TABLE trust_scores ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass  # Column already exists

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
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_wallet_reason_unique
        ON wallet_reasons(wallet, reason_code)
    """)

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

    # wallet_risk_probabilities (Bayesian risk logging)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_risk_probabilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            prior REAL,
            posterior REAL,
            reason_code TEXT,
            likelihood REAL,
            confidence REAL,
            created_at INTEGER DEFAULT (strftime('%s','now'))
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_risk_wallet ON wallet_risk_probabilities(wallet)")

    # wallet_last_update (rate limit for realtime risk engine: max 1 update per 5 min per wallet)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_last_update (
            wallet TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_last_update_timestamp ON wallet_last_update(timestamp)")

    # wallet_badges (badge evolution timeline for UI and Phantom plugin)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_badges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            badge TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_badges_wallet ON wallet_badges(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_badges_timestamp ON wallet_badges(timestamp)")

    # helius_usage (API cost tracking)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS helius_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            endpoint TEXT NOT NULL,
            wallet TEXT NOT NULL,
            request_count INTEGER NOT NULL DEFAULT 1,
            estimated_cost REAL NOT NULL DEFAULT 0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_helius_usage_timestamp ON helius_usage(timestamp)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_helius_usage_wallet ON helius_usage(wallet)")

    # wallet_scan_meta (prioritizer: last scan timestamp per wallet)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_scan_meta (
            wallet TEXT PRIMARY KEY,
            last_scan_ts INTEGER NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_scan_meta_last_scan ON wallet_scan_meta(last_scan_ts)")

    # pipeline_run_log (monitoring: last pipeline run)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_run_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_start_ts INTEGER NOT NULL,
            run_end_ts INTEGER,
            success INTEGER DEFAULT 0,
            wallets_scanned INTEGER DEFAULT 0,
            errors_count INTEGER DEFAULT 0,
            steps_completed INTEGER DEFAULT 0,
            message TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_run_start ON pipeline_run_log(run_start_ts)")


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
