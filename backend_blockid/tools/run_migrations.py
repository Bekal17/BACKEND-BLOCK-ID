from __future__ import annotations

import time
from pathlib import Path
import sqlite3

from backend_blockid.database.connection import get_connection
from backend_blockid.tools import backup_db

MIGRATIONS_DIR = Path("backend_blockid/database/migrations")


def ensure_migration_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE,
            applied_at INTEGER
        )
    """)
    conn.commit()


def get_applied_migrations(conn):
    rows = conn.execute("SELECT filename FROM schema_migrations").fetchall()
    return {r[0] for r in rows}


def column_exists(conn, table, column):
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r[1] == column for r in rows)


def safe_exec(conn, statement):
    try:
        conn.execute(statement)
    except sqlite3.OperationalError as e:
        msg = str(e).lower()
        if "duplicate column name" in msg:
            print("⚠ Skip duplicate column:", statement)
        elif "already exists" in msg:
            print("⚠ Skip existing object:", statement)
        else:
            raise


def apply_migration(conn, filepath: Path):
    print(f"\nApplying migration: {filepath.name}")
    sql = filepath.read_text(encoding="utf-8")

    statements = [
        s.strip() for s in sql.split(";") if s.strip()
    ]

    for stmt in statements:
        safe_exec(conn, stmt)

    conn.execute(
        "INSERT OR IGNORE INTO schema_migrations(filename, applied_at) VALUES (?, ?)",
        (filepath.name, int(time.time())),
    )
    conn.commit()

    print("✔ Done:", filepath.name)


def run():
    if not MIGRATIONS_DIR.exists():
        print("No migrations directory found.")
        return

    backup_db.create_backup()
    conn = get_connection()
    ensure_migration_table(conn)
    applied = get_applied_migrations(conn)

    files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not files:
        print("No migration files found.")
        return

    for f in files:
        if f.name in applied:
            print("Skipping already applied:", f.name)
            continue
        apply_migration(conn, f)

    print("\nMigration complete.")


if __name__ == "__main__":
    run()
