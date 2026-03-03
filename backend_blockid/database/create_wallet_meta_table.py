"""
Create wallet_meta table required by incremental_wallet_meta_scanner.

Run once: py -m backend_blockid.database.create_wallet_meta_table
"""
import sqlite3

from backend_blockid.database.connection import get_connection


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_meta (
            wallet TEXT PRIMARY KEY,
            first_tx_ts INTEGER,
            last_tx_ts INTEGER,
            wallet_age_days INTEGER,
            last_scam_tx_ts INTEGER,
            last_scan_time INTEGER,
            cluster_id TEXT,
            is_test_wallet INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_wallet_meta_wallet
        ON wallet_meta(wallet)
    """)

    conn.commit()
    conn.close()

    print("[create_wallet_meta_table] OK")


if __name__ == "__main__":
    main()
