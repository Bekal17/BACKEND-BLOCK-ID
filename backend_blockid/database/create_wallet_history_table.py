"""
Create wallet_score_history table for Trust Badge History Chart.

Schema: wallet, timestamp, score, risk. PK = (wallet, timestamp).
Run before save_score_history.py.

Usage:
    py -m backend_blockid.database.create_wallet_history_table
"""
from __future__ import annotations

import sys
from pathlib import Path

if __name__ == "__main__" and __package__ is None:
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.connection import get_connection


def main() -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_score_history (
            wallet TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            score REAL NOT NULL,
            risk INTEGER NOT NULL,
            PRIMARY KEY (wallet, timestamp)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_score_history_wallet ON wallet_score_history(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_score_history_timestamp ON wallet_score_history(timestamp)")

    conn.commit()
    conn.close()

    print("[create_wallet_history_table] wallet_score_history table ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
