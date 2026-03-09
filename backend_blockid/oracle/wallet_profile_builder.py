"""
Wallet profile builder — creates/updates wallet_profiles for realtime pipeline.

Ensures wallet_profiles has a row so ML features are available and predict_wallet_score
does not fall back to ml_score = 0.

Usage:
  from backend_blockid.oracle.wallet_profile_builder import build_wallet_profile
  await build_wallet_profile(wallet)
"""

from __future__ import annotations

import time

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.database.repositories import get_wallet_meta

logger = get_logger(__name__)


async def build_wallet_profile(wallet: str) -> None:
    """
    Create or update wallet_profiles row for a wallet.
    Uses wallet_meta (first_tx_ts, last_tx_ts) or transactions table; fallback to now.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return

    now = int(time.time())
    first_seen = now
    last_seen = now

    meta = await get_wallet_meta(wallet)
    if meta and (meta.get("first_tx_ts") or meta.get("last_tx_ts")):
        first_seen = int(meta.get("first_tx_ts") or now)
        last_seen = int(meta.get("last_tx_ts") or now)

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts
            FROM transactions
            WHERE wallet = $1 AND timestamp IS NOT NULL
            """,
            wallet,
        )
        if row and (row["min_ts"] is not None or row["max_ts"] is not None):
            if row["min_ts"] is not None:
                first_seen = min(first_seen, int(row["min_ts"]))
            if row["max_ts"] is not None:
                last_seen = max(last_seen, int(row["max_ts"]))

        await conn.execute(
            """
            INSERT INTO wallet_profiles (wallet, first_seen_at, last_seen_at, profile_json, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (wallet) DO UPDATE SET
                first_seen_at = LEAST(wallet_profiles.first_seen_at, EXCLUDED.first_seen_at),
                last_seen_at = GREATEST(wallet_profiles.last_seen_at, EXCLUDED.last_seen_at),
                updated_at = EXCLUDED.updated_at
            """,
            wallet,
            first_seen,
            last_seen,
            None,
            now,
            now,
        )
        logger.debug("wallet_profile_built", wallet=wallet[:16], first_seen=first_seen, last_seen=last_seen)
    finally:
        await release_conn(conn)
