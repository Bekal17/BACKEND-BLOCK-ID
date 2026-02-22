"""
Initialize BlockID database tables.

Creates trust_scores, wallet_reasons, scam_wallets, wallet_clusters, wallet_history.
Uses CREATE TABLE IF NOT EXISTS — safe to run repeatedly.

Usage:
    py -m backend_blockid.database.init_tables
"""
import sqlite3
from pathlib import Path

DB = Path(r"D:/BACKENDBLOCKID/blockid.db")


def create_tables(conn):
    cur = conn.cursor()

    # 1️⃣ TRUST SCORES
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trust_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT UNIQUE,
        score REAL,
        risk_level TEXT,
        reason_codes TEXT,
        metadata_json TEXT,
        computed_at INTEGER,
        updated_at INTEGER,
        created_at INTEGER
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_trust_wallet ON trust_scores(wallet)")

    # 2️⃣ WALLET REASONS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_reasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT,
        reason_code TEXT,
        weight INTEGER,
        created_at INTEGER,
        tx_hash TEXT,
        tx_link TEXT
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_reason_wallet ON wallet_reasons(wallet)")

    # 3️⃣ SCAM WALLETS
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_scam_wallet ON scam_wallets(wallet)")

    # 4️⃣ WALLET CLUSTERS
    cur.execute("""
    CREATE TABLE IF NOT EXISTS wallet_clusters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT,
        cluster_id INTEGER,
        cluster_type TEXT,
        created_at INTEGER
    )
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_cluster_wallet ON wallet_clusters(wallet)")

    # 5️⃣ WALLET HISTORY
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

    cur.execute("CREATE INDEX IF NOT EXISTS idx_history_wallet ON wallet_history(wallet)")

    conn.commit()


def main():
    print("Initializing BlockID database...")
    conn = sqlite3.connect(DB)
    create_tables(conn)
    conn.close()
    print("BlockID DB ready at", DB)


if __name__ == "__main__":
    main()
