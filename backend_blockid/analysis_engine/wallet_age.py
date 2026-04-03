"""
Fetch the true first transaction timestamp for a wallet from Helius/Solana RPC.
Paginates backwards through getSignaturesForAddress to find the oldest signature.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"


async def get_wallet_first_tx_timestamp(wallet: str) -> int | None:
    """
    Get the timestamp of the very first transaction for a wallet.
    Returns Unix timestamp (seconds) or None if not found.

    Strategy: paginate getSignaturesForAddress backwards until we reach
    the oldest signature. Uses limit=1000 per call to minimize API calls.
    """
    if not HELIUS_API_KEY:
        logger.warning("wallet_age_no_helius_key")
        return None

    wallet = (wallet or "").strip()
    if not wallet:
        return None

    try:
        oldest_timestamp: int | None = None
        before: str | None = None
        max_pages = 50  # Safety limit: 50 * 1000 = 50k tx max

        async with httpx.AsyncClient(timeout=30) as client:
            for page in range(max_pages):
                params_inner: dict[str, str | int] = {"limit": 1000}
                if before:
                    params_inner["before"] = before

                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [wallet, params_inner],
                }

                resp = await client.post(HELIUS_RPC_URL, json=payload)
                if resp.status_code != 200:
                    logger.warning(
                        "wallet_age_http_error",
                        status_code=resp.status_code,
                        body=(resp.text[:200] if resp.text else ""),
                    )
                    break
                data = resp.json()

                if "error" in data:
                    logger.warning("wallet_age_rpc_error", error=data["error"], page=page)
                    break

                signatures = data.get("result", [])
                if not signatures:
                    break

                last_sig = signatures[-1]
                oldest_timestamp = last_sig.get("blockTime")

                if len(signatures) < 1000:
                    break

                before = last_sig["signature"]

        if oldest_timestamp is not None:
            logger.info(
                "wallet_age_found",
                wallet_prefix=wallet[:16] + "...",
                first_tx_time=datetime.fromtimestamp(oldest_timestamp, tz=timezone.utc).isoformat(),
            )
            return int(oldest_timestamp)

        return None

    except Exception as e:
        logger.warning("wallet_age_fetch_failed", wallet_prefix=wallet[:16] + "...", error=str(e))
        return None


def calculate_wallet_age_days(first_tx_timestamp: int | None) -> int:
    """Calculate wallet age in days from first tx timestamp."""
    if first_tx_timestamp is None:
        return 0
    now = int(datetime.now(timezone.utc).timestamp())
    age_seconds = max(now - int(first_tx_timestamp), 0)
    return max(int(age_seconds / 86400), 1)  # At least 1 day if tx exists
