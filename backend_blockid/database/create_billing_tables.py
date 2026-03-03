"""
Create billing tables for BlockID Stripe integration.

Run once: py -m backend_blockid.database.create_billing_tables
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection, DB_PATH


def create_tables() -> None:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key TEXT UNIQUE NOT NULL,
            stripe_customer_id TEXT,
            plan TEXT NOT NULL DEFAULT 'starter',
            created_at INTEGER NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customers_api_key ON customers(api_key)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_customers_stripe ON customers(stripe_customer_id)")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS api_usage (
            api_key TEXT PRIMARY KEY,
            wallet_checks INTEGER DEFAULT 0,
            batch_checks INTEGER DEFAULT 0,
            reports_generated INTEGER DEFAULT 0,
            webhook_events INTEGER DEFAULT 0,
            last_reset INTEGER NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    print(f"[create_billing_tables] OK. DB: {DB_PATH}")


def main() -> int:
    create_tables()
    return 0


if __name__ == "__main__":
    sys.exit(main())
