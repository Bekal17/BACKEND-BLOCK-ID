"""
Create review_queue table for BlockID manual review workflow.

Run once: py -m backend_blockid.database.create_review_queue_table
"""
from __future__ import annotations

from backend_blockid.database.connection import get_connection


def main() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS review_queue (
            wallet TEXT PRIMARY KEY,
            score REAL,
            confidence REAL,
            risk INTEGER,
            reasons TEXT,
            created_at INTEGER,
            status TEXT DEFAULT 'pending'
        )
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_review_queue_status
        ON review_queue(status)
    """)

    conn.commit()
    conn.close()

    print("[create_review_queue_table] OK")


if __name__ == "__main__":
    main()
