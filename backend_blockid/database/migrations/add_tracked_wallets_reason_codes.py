"""
Migration: add reason_codes column to tracked_wallets (Step 2 wallet tracking DB).

Tracked wallets live in db_wallet_tracking (SQLAlchemy); this migration is also
run automatically from init_db() in backend_blockid.api_server.db_wallet_tracking.

Run manually only if you need to apply the migration without full init:
  WALLET_TRACKING_DB_PATH=wallet_tracking.db python -m backend_blockid.database.migrations.add_tracked_wallets_reason_codes
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    db_path = (
        (os.getenv("WALLET_TRACKING_DB_PATH") or "").strip()
        or (os.getenv("DATABASE_PATH") or "").strip()
        or "wallet_tracking.db"
    )
    if not db_path:
        print("Set WALLET_TRACKING_DB_PATH or DATABASE_PATH", file=sys.stderr)
        return 1
    url = f"sqlite:///{db_path}"
    try:
        from sqlalchemy import create_engine, text
        engine = create_engine(url)
        with engine.connect() as conn:
            result = conn.execute(text("PRAGMA table_info(tracked_wallets)"))
            rows = result.fetchall()
            if not rows:
                print("Table tracked_wallets not found; run init_db first.", file=sys.stderr)
                return 1
            if any(r[1] == "reason_codes" for r in rows):
                print("Column reason_codes already exists.")
                return 0
            conn.execute(text("ALTER TABLE tracked_wallets ADD COLUMN reason_codes TEXT"))
            conn.commit()
        print("Added column reason_codes to tracked_wallets.")
        return 0
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
