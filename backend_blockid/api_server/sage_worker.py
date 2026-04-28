"""
Background polling worker for @sage mentions in social feed.
"""

from __future__ import annotations

import asyncio

from backend_blockid.api_server.sage_api import process_sage_mention
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

last_processed_id = 0


async def _initialize_last_processed_id() -> None:
    """Set starting cursor so restarts do not reply to old posts."""
    global last_processed_id
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT COALESCE(MAX(id), 0) AS max_id
            FROM social_posts
            WHERE handle != 'sage'
            """
        )
        last_processed_id = int(row["max_id"] or 0) if row else 0
        logger.info("sage_worker_initialized", last_processed_id=last_processed_id)
    finally:
        await release_conn(conn)


async def poll_sage_mentions() -> None:
    """Fetch and process new @sage mentions since last processed post ID."""
    global last_processed_id
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT id, wallet, handle, content
            FROM social_posts
            WHERE id > $1
              AND handle != 'sage'
              AND content ILIKE '%@sage%'
              AND parent_id IS NULL
            ORDER BY id ASC
            LIMIT 20
            """,
            last_processed_id,
        )
    finally:
        await release_conn(conn)

    for row in rows:
        post_id = int(row["id"])
        try:
            await process_sage_mention(
                post_id=post_id,
                content=row["content"] or "",
                author_wallet=row["wallet"] or "",
                author_handle=row["handle"] or "",
            )
        except Exception as e:
            logger.warning("sage_worker_process_error", post_id=post_id, error=str(e))
        finally:
            last_processed_id = post_id


async def start_sage_worker() -> None:
    """Run forever: poll @sage mentions every 30 seconds."""
    await _initialize_last_processed_id()
    while True:
        try:
            await poll_sage_mentions()
        except Exception as e:
            logger.warning("sage_worker_poll_error", error=str(e))
        await asyncio.sleep(30)
