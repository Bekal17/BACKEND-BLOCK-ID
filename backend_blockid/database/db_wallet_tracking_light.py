"""
Lightweight wallet list for pipeline mode — no FastAPI/SQLAlchemy.

Uses PostgreSQL via asyncpg. Avoids importing api_server.
"""
from __future__ import annotations

import asyncio
import time

from backend_blockid.database.pg_connection import get_conn, release_conn


async def init_db_async():
    return await get_conn()


def init_db():
    return asyncio.get_event_loop().run_until_complete(init_db_async())


async def load_active_wallets_async() -> list[str]:
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT wallet FROM trust_scores")
        return [r["wallet"] for r in rows if r.get("wallet")]
    finally:
        await release_conn(conn)


def load_active_wallets() -> list[str]:
    return asyncio.get_event_loop().run_until_complete(load_active_wallets_async())


async def insert_reason_evidence_async(
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
    conn = await get_conn()
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallet_reason_evidence (
                id SERIAL PRIMARY KEY,
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
        row_id = await conn.fetchval(
            """
            INSERT INTO wallet_reason_evidence (wallet, reason_code, tx_signature, counterparty, amount, token, timestamp)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            wallet,
            reason_code,
            (tx_signature or "").strip() or None,
            (counterparty or "").strip() or None,
            str(amount).strip() if amount is not None else None,
            (token or "").strip() or None,
            ts,
        )
        return row_id or 0
    finally:
        await release_conn(conn)


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
    """Sync wrapper for insert_reason_evidence_async."""
    return asyncio.get_event_loop().run_until_complete(
        insert_reason_evidence_async(
            wallet,
            reason_code,
            tx_signature=tx_signature,
            counterparty=counterparty,
            amount=amount,
            token=token,
            timestamp=timestamp,
        )
    )
