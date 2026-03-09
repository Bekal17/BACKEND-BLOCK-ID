"""
Database connection — PostgreSQL only.

SQLite has been removed. Use pg_connection for async PostgreSQL access.

  from backend_blockid.database.pg_connection import get_conn, release_conn, init_db

All repository functions in repositories.py are async and use asyncpg.
"""
from pathlib import Path
import os

from dotenv import load_dotenv

load_dotenv()

# Kept for compatibility; DB_PATH no longer used for SQLite
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DB_PATH = Path(os.getenv("DB_PATH", str(_PROJECT_ROOT / "blockid.db"))).resolve()


def get_connection():
    """Deprecated. Use pg_connection.get_conn() with async/await."""
    raise NotImplementedError(
        "SQLite has been removed. Use backend_blockid.database.pg_connection: "
        "async with get_conn()/release_conn() for PostgreSQL."
    )
