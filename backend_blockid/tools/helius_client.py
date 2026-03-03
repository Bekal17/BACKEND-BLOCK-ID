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

import time

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.connection import get_connection

COST_PER_CALL = 0.00001  # USD per API call (configurable)

_logger = get_logger(__name__)


def helius_request(endpoint: str, wallet: str, request_count: int = 1) -> None:
    """
    Log a Helius API request to helius_usage and emit [helius_cost] log line.

    Call this after each successful Helius API request.
    """
    estimated_cost = request_count * COST_PER_CALL
    ts = int(time.time())

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO helius_usage (timestamp, endpoint, wallet, request_count, estimated_cost)
            VALUES (?, ?, ?, ?, ?)
            """,
            (ts, endpoint, wallet, request_count, estimated_cost),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        _logger.warning("helius_usage_insert_failed", endpoint=endpoint, wallet=wallet[:16], error=str(e))

    wallet_short = wallet[:16] + "..." if len(wallet) > 16 else wallet
    _logger.info("helius_cost", wallet=wallet_short, calls=request_count, cost=round(estimated_cost, 5))
