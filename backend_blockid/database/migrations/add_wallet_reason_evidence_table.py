"""
Migration: create wallet_reason_evidence table (BlockID wallet tracking DB).

Evidence table linking wallets to reason codes with tx-level details.
Runs automatically via init_db() when Base.metadata.create_all executes.
Run manually to add the table to an existing DB without full init:

  DATABASE_URL=... python -m backend_blockid.database.migrations.add_wallet_reason_evidence_table
  # or SQLite:
  WALLET_TRACKING_DB_PATH=wallet_tracking.db python -m backend_blockid.database.migrations.add_wallet_reason_evidence_table
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure backend_blockid is on path when run as script
if __name__ == "__main__":
    _root = Path(__file__).resolve().parents[2]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    try:
        from dotenv import load_dotenv
        load_dotenv(_root / ".env")
    except Exception:
        pass


def _get_database_url() -> str:
    url = (os.getenv("BLOCKID_DB_URL") or os.getenv("DATABASE_URL") or "").strip()
    if url:
        return url
    path = (
        (os.getenv("DATABASE_PATH") or "").strip()
        or (os.getenv("WALLET_TRACKING_DB_PATH") or "").strip()
        or "wallet_tracking.db"
    )
    return f"sqlite:///{path}"


def main() -> int:
    from sqlalchemy import create_engine, text

    url = _get_database_url()
    is_sqlite = "sqlite" in url

    if is_sqlite:
        # SQLite: use CREATE TABLE IF NOT EXISTS
        ddl = """
        CREATE TABLE IF NOT EXISTS wallet_reason_evidence (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            wallet VARCHAR(64) NOT NULL,
            reason_code VARCHAR(64) NOT NULL,
            tx_signature VARCHAR(128),
            counterparty VARCHAR(64),
            amount VARCHAR(64),
            token VARCHAR(64),
            timestamp INTEGER
        );
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_wallet ON wallet_reason_evidence (wallet);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_reason_code ON wallet_reason_evidence (reason_code);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_tx_signature ON wallet_reason_evidence (tx_signature);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_counterparty ON wallet_reason_evidence (counterparty);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_token ON wallet_reason_evidence (token);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_timestamp ON wallet_reason_evidence (timestamp);
        """
    else:
        # PostgreSQL
        ddl = """
        CREATE TABLE IF NOT EXISTS wallet_reason_evidence (
            id SERIAL PRIMARY KEY,
            wallet VARCHAR(64) NOT NULL,
            reason_code VARCHAR(64) NOT NULL,
            tx_signature VARCHAR(128),
            counterparty VARCHAR(64),
            amount VARCHAR(64),
            token VARCHAR(64),
            timestamp INTEGER
        );
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_wallet ON wallet_reason_evidence (wallet);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_reason_code ON wallet_reason_evidence (reason_code);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_tx_signature ON wallet_reason_evidence (tx_signature);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_counterparty ON wallet_reason_evidence (counterparty);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_token ON wallet_reason_evidence (token);
        CREATE INDEX IF NOT EXISTS ix_wallet_reason_evidence_timestamp ON wallet_reason_evidence (timestamp);
        """

    try:
        engine = create_engine(url)
        with engine.connect() as conn:
            for stmt in (s.strip() for s in ddl.split(";") if s.strip()):
                if stmt.upper().startswith("CREATE INDEX"):
                    # Postgres: IF NOT EXISTS for CREATE INDEX needs 9.5+
                    if not is_sqlite:
                        # Postgres CREATE INDEX IF NOT EXISTS
                        conn.execute(text(stmt))
                    else:
                        conn.execute(text(stmt))
                else:
                    conn.execute(text(stmt))
            conn.commit()
        print("Created table wallet_reason_evidence and indexes.")
        return 0
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
