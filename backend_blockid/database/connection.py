"""
Database connection and session management.

Responsibilities:
- Create and manage connection pools (e.g., asyncpg, SQLAlchemy).
- Provide session/transaction context for repository operations.
- Handle connection lifecycle and retries.
"""
import os
import sqlite3
from pathlib import Path

# blockid.db at project root (same as recreate_wallet_reasons migration)
# DB_PATH env overrides for Docker/production (e.g. /app/data/blockid.db)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(_PROJECT_ROOT / "blockid.db"))).resolve()


def get_connection():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn