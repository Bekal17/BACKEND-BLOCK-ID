"""
Recreate wallet_reasons table with (wallet, reason_code, weight, created_at) schema.

Usage:
    py -m backend_blockid.database.migrations.recreate_wallet_reasons_table
"""
import sqlite3
from pathlib import Path

# blockid.db at project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DB_PATH = _PROJECT_ROOT / "blockid.db"


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS wallet_reasons")

    cur.execute("""
    CREATE TABLE wallet_reasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wallet TEXT,
        reason_code TEXT,
        weight INTEGER,
        created_at INTEGER
    )
    """)

    conn.commit()
    conn.close()

    print("wallet_reasons recreated")


if __name__ == "__main__":
    main()
