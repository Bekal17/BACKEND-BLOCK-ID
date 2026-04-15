"""
Background worker that scans social posts for @sage mentions
and triggers Sage auto-replies.
"""

from __future__ import annotations

import asyncio
import os

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.api_server.sage_api import process_sage_mention


logger = get_logger(__name__)

SAGE_POLL_INTERVAL_SEC = float(os.getenv("SAGE_POLL_INTERVAL_SEC", "15").strip() or "15")
SAGE_BATCH_LIMIT = int(os.getenv("SAGE_BATCH_LIMIT", "20").strip() or "20")


async def _fetch_pending_mentions():
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT sp.id, sp.content, sp.wallet AS author_wallet, COALESCE(sp.handle, '') AS author_handle
            FROM social_posts sp
            WHERE sp.parent_id IS NULL
              AND sp.content ILIKE '%@sage%'
              AND NOT EXISTS (
                SELECT 1
                FROM social_posts r
                WHERE r.parent_id = sp.id
                  AND LOWER(COALESCE(r.handle, '')) = 'sage'
              )
            ORDER BY sp.created_at DESC
            LIMIT $1
            """,
            SAGE_BATCH_LIMIT,
        )
        return rows
    finally:
        await release_conn(conn)


async def start_sage_worker():
    """Run forever: process posts that mention @sage and have no sage reply."""
    logger.info("sage_worker_started", interval_sec=SAGE_POLL_INTERVAL_SEC, batch_limit=SAGE_BATCH_LIMIT)
    while True:
        try:
            rows = await _fetch_pending_mentions()
            for row in rows:
                try:
                    await process_sage_mention(
                        post_id=row["id"],
                        content=row["content"] or "",
                        author_wallet=row["author_wallet"] or "",
                        author_handle=row["author_handle"] or "",
                    )
                except Exception as e:
                    logger.warning(
                        "sage_worker_process_failed",
                        post_id=row.get("id"),
                        error=str(e),
                    )
        except Exception as e:
            logger.warning("sage_worker_loop_error", error=str(e))

        await asyncio.sleep(SAGE_POLL_INTERVAL_SEC)
