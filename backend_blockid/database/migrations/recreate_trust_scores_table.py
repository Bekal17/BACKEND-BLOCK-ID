"""
Recreate trust_scores table.

Usage:
    py -m backend_blockid.database.migrations.recreate_trust_scores_table
"""
import sqlite3
from pathlib import Path

DB_PATH = Path("D:/BACKENDBLOCKID/blockid.db")


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS trust_scores")

    cur.execute("""
    CREATE TABLE trust_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT UNIQUE,
        score REAL,
        risk_level TEXT,
        reason_codes TEXT,
        updated_at INTEGER
    )
    """)

    conn.commit()
    conn.close()

    print("trust_scores recreated")


if __name__ == "__main__":
    main()
