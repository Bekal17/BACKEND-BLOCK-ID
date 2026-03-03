"""
Create the transactions table for Helius-fetched data (graph + ML pipeline).

Schema is minimal for BlockID graph clustering and flow analysis.
Run before fetch_helius_transactions.py.

Note: If an existing transactions table has a different schema (e.g. sender/receiver
from database.py), drop it first or use a fresh DB:
  sqlite3 blockid.db "DROP TABLE IF EXISTS transactions;"
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root is on path when run as script
if __name__ == "__main__" and __package__ is None:
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.connection import get_connection


def main() -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            signature TEXT PRIMARY KEY,
            wallet TEXT,
            from_wallet TEXT,
            to_wallet TEXT,
            amount REAL,
            token TEXT,
            timestamp INTEGER,
            program_id TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_wallet ON transactions(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp)")

    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}
    if "from_wallet" not in cols:
        print("[create_transactions_table] WARNING: transactions exists with different schema.")
        print("  Drop it and rerun: sqlite3 blockid.db \"DROP TABLE IF EXISTS transactions;\"")
        conn.close()
        return 1

    conn.commit()
    conn.close()

    print("[create_transactions_table] transactions table ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
