"""
Badge Engine for BlockID — reputation badge evolution and timeline.

Computes badge from score, records badge changes, provides evolution timeline.
For UI (app.blockidscore.fun) and Phantom plugin overlay.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.badge_rules import BADGES
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)


def get_badge(score: float) -> str:
    """Return badge name for given score (0–100)."""
    s = max(0, min(100, float(score)))
    for name, low, high in BADGES:
        if low <= s <= high:
            return name
    return BADGES[-1][0]  # fallback


async def _ensure_wallet_badges_table_async(conn) -> None:
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_badges (
            id SERIAL PRIMARY KEY,
            wallet TEXT NOT NULL,
            badge TEXT NOT NULL,
            timestamp BIGINT NOT NULL
        )
        """
    )
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_badges_wallet ON wallet_badges(wallet)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_wallet_badges_timestamp ON wallet_badges(timestamp)")


async def record_badge_if_changed_async(
    wallet: str,
    old_score: float | None,
    new_score: float,
    timestamp: int | None = None,
) -> bool:
    """
    Insert into wallet_badges when badge changes.
    Returns True if badge was recorded.
    """
    import time

    old_badge = get_badge(old_score) if old_score is not None else None
    new_badge = get_badge(new_score)
    if old_badge == new_badge:
        return False

    ts = timestamp if timestamp is not None else int(time.time())
    conn = await get_conn()
    try:
        await _ensure_wallet_badges_table_async(conn)
        await conn.execute(
            "INSERT INTO wallet_badges (wallet, badge, timestamp) VALUES ($1, $2, $3)",
            wallet.strip(), new_badge, ts,
        )
        old_str = old_badge or "NONE"
        logger.info("badge_change", wallet=wallet[:16], old=old_str, new=new_badge)
        print(f"[badge_change] wallet={wallet[:16]}... old={old_str} new={new_badge}")
        return True
    finally:
        await release_conn(conn)


def record_badge_if_changed(
    wallet: str,
    old_score: float | None,
    new_score: float,
    timestamp: int | None = None,
    conn=None,  # deprecated parameter, kept for compatibility
) -> bool:
    """Sync wrapper for record_badge_if_changed_async."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(
        record_badge_if_changed_async(wallet, old_score, new_score, timestamp)
    )


async def _table_exists_async(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _get_table_columns_async(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name=$1",
        table,
    )
    return {r["column_name"] for r in rows}


async def get_badge_timeline_async(wallet: str) -> list[dict[str, Any]]:
    """
    Load wallet_history scores, compute badge per timestamp.
    Returns list of {"date": "YYYY-MM-DD HH:MM", "badge": "TRUSTED", "score": 65}.
    """
    conn = await get_conn()
    try:
        rows = []
        if await _table_exists_async(conn, "wallet_history"):
            cols = await _get_table_columns_async(conn, "wallet_history")
            if "posterior" in cols:
                rows = await conn.fetch(
                    """
                    SELECT COALESCE(score, (1.0 - COALESCE(posterior, 0.5)) * 100) AS score, snapshot_at
                    FROM wallet_history
                    WHERE wallet = $1
                    ORDER BY snapshot_at ASC
                    """,
                    wallet.strip(),
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT COALESCE(score, 50) AS score, snapshot_at
                    FROM wallet_history
                    WHERE wallet = $1
                    ORDER BY snapshot_at ASC
                    """,
                    wallet.strip(),
                )

        timeline: list[dict[str, Any]] = []
        for r in rows:
            score = float(r["score"] or 50)
            ts = int(r["snapshot_at"] or 0)
            dt = datetime.utcfromtimestamp(ts)
            date_str = dt.strftime("%Y-%m-%d %H:%M")
            badge = get_badge(score)
            timeline.append({"date": date_str, "badge": badge, "score": int(round(score))})

        row = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1 LIMIT 1",
            wallet.strip(),
        )
        if row:
            score = float(row["score"] or 50)
            if not timeline or abs(timeline[-1]["score"] - score) > 0.5:
                import time
                ts = int(time.time())
                dt = datetime.utcfromtimestamp(ts)
                timeline.append({
                    "date": dt.strftime("%Y-%m-%d %H:%M"),
                    "badge": get_badge(score),
                    "score": int(round(score)),
                })

        return timeline
    finally:
        await release_conn(conn)


def get_badge_timeline(wallet: str, conn=None) -> list[dict[str, Any]]:
    """Sync wrapper for get_badge_timeline_async."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(get_badge_timeline_async(wallet))
