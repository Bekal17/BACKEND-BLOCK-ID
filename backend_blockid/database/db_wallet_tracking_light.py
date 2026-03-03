"""
Lightweight wallet list for pipeline mode — no FastAPI/SQLAlchemy.

Uses blockid.db trust_scores. Avoids importing api_server.
"""
from __future__ import annotations

import time

from backend_blockid.database.connection import get_connection


def init_db():
    return get_connection()


def load_active_wallets() -> list[str]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT wallet FROM trust_scores")
    out = [r[0] for r in cur.fetchall()]
    conn.close()
    return out


def insert_reason_evidence(
    wallet: str,
    reason_code: str,
    *,
    tx_signature: str | None = None,
    counterparty: str | None = None,
    amount: str | None = None,
    token: str | None = None,
    timestamp: int | None = None,
) -> int:
    """Insert one wallet_reason_evidence row. Returns the new row id."""
    wallet = (wallet or "").strip()
    reason_code = (reason_code or "").strip()
    if not wallet or not reason_code:
        raise ValueError("wallet and reason_code are required")
    ts = timestamp if timestamp is not None else int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_reason_evidence (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            reason_code TEXT NOT NULL,
            tx_signature TEXT,
            counterparty TEXT,
            amount TEXT,
            token TEXT,
            timestamp INTEGER
        )
        """
    )
    cur.execute(
        """
        INSERT INTO wallet_reason_evidence (wallet, reason_code, tx_signature, counterparty, amount, token, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            wallet,
            reason_code,
            (tx_signature or "").strip() or None,
            (counterparty or "").strip() or None,
            str(amount).strip() if amount is not None else None,
            (token or "").strip() or None,
            ts,
        ),
    )
    row_id = cur.lastrowid or 0
    conn.commit()
    conn.close()
    return row_id
