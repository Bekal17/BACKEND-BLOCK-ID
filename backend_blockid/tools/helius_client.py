"""
BlockID Helius API client wrapper.

Logs each request to helius_usage for cost tracking.
Use helius_request() after each successful Helius API call.

Usage:
    from backend_blockid.tools.helius_client import helius_request

    resp = requests.get(url)
    helius_request("addresses/transactions", wallet, request_count=1)
"""
from __future__ import annotations

import asyncio
import time

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

COST_PER_CALL = 0.00001  # USD per API call (configurable)

_logger = get_logger(__name__)


async def helius_request_async(endpoint: str, wallet: str, request_count: int = 1) -> None:
    """
    Log a Helius API request to helius_usage and emit [helius_cost] log line.

    Call this after each successful Helius API request.
    """
    estimated_cost = request_count * COST_PER_CALL
    ts = int(time.time())

    try:
        conn = await get_conn()
        try:
            await conn.execute(
                """
                INSERT INTO helius_usage (timestamp, endpoint, wallet, request_count, estimated_cost)
                VALUES ($1, $2, $3, $4, $5)
                """,
                ts, endpoint, wallet, request_count, estimated_cost,
            )
        finally:
            await release_conn(conn)
    except Exception as e:
        _logger.warning("helius_usage_insert_failed", endpoint=endpoint, wallet=wallet[:16], error=str(e))

    wallet_short = wallet[:16] + "..." if len(wallet) > 16 else wallet
    _logger.info("helius_cost", wallet=wallet_short, calls=request_count, cost=round(estimated_cost, 5))


def helius_request(endpoint: str, wallet: str, request_count: int = 1) -> None:
    """Sync wrapper for helius_request_async."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(helius_request_async(endpoint, wallet, request_count))
        else:
            loop.run_until_complete(helius_request_async(endpoint, wallet, request_count))
    except Exception as e:
        _logger.warning("helius_usage_insert_failed", endpoint=endpoint, wallet=wallet[:16], error=str(e))
