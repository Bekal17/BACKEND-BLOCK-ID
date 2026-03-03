"""
RPC health checker for BlockID Multi-RPC Failover.

Uses getLatestBlockhash to measure latency and verify RPC is responsive.
"""

from __future__ import annotations

import time

import requests

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = 5


def check_rpc_health(
    url: str,
    timeout: float | None = None,
) -> tuple[bool, float | None]:
    """
    Check RPC health via getLatestBlockhash.
    Returns (success, latency_seconds). latency is None on failure.
    """
    timeout = timeout if timeout is not None else DEFAULT_TIMEOUT
    payload = {"jsonrpc": "2.0", "id": "blockid-health", "method": "getLatestBlockhash", "params": []}
    start = time.perf_counter()
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        err = data.get("error")
        if err:
            return False, None
        if "result" not in data:
            return False, None
        elapsed = time.perf_counter() - start
        return True, elapsed
    except Exception as e:
        logger.debug("rpc_health_failed", url=url[:50], error=str(e))
        return False, None
