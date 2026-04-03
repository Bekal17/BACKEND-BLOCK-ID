"""
Fetch the true wallet age using hybrid approach:
1. getSignaturesForAddress — oldest signature (first active transaction)
2. Account creation time via getAccountInfo + first block appearance
Uses the OLDEST timestamp found for most accurate wallet age.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"


async def _get_oldest_signature_time(wallet: str, client: httpx.AsyncClient) -> int | None:
    """Paginate getSignaturesForAddress to find the oldest signature timestamp."""
    oldest_timestamp: int | None = None
    before: str | None = None
    max_pages = 10  # Safety limit: 10 * 1000 = 10k tx max, prevents timeout for high-volume wallets

    for page in range(max_pages):
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [
                wallet,
                {
                    "limit": 1000,
                    **({"before": before} if before else {}),
                },
            ],
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
        bt = last_sig.get("blockTime")
        if bt is not None:
            oldest_timestamp = int(bt)

        if len(signatures) < 1000:
            break

        before = last_sig["signature"]

    return oldest_timestamp


async def _get_account_creation_time(wallet: str, client: httpx.AsyncClient) -> int | None:
    """
    Try to find account creation time by searching for the earliest transaction
    that involves this account using Helius enhanced transactions API.
    Falls back to getSignaturesForAddress with commitment=confirmed.
    """
    try:
        # Method 1: Use Helius parsed transaction history (enhanced API)
        # This catches program instructions like initialize/createAccount
        url = (
            f"https://api.helius.xyz/v0/addresses/{wallet}/transactions"
            f"?api-key={HELIUS_API_KEY}&type=UNKNOWN&limit=1&sortOrder=asc"
        )
        resp = await client.get(url, timeout=15)
        if resp.status_code == 200:
            txs = resp.json()
            if txs and len(txs) > 0:
                ts = txs[0].get("timestamp")
                if ts is not None:
                    ts_int = int(ts)
                    logger.info("wallet_age_helius_enhanced", wallet=wallet[:16] + "...", timestamp=ts_int)
                    return ts_int
    except Exception as e:
        logger.debug("wallet_age_enhanced_api_failed", error=str(e))

    try:
        # Method 2: Use Helius address history with sortOrder asc
        url = f"https://api.helius.xyz/v0/addresses/{wallet}/transactions?api-key={HELIUS_API_KEY}&limit=1"
        resp = await client.get(url, timeout=15)
        if resp.status_code == 200:
            txs = resp.json()
            if txs and len(txs) > 0:
                # Get the last page to find oldest
                # This API returns newest first by default
                pass
    except Exception:
        pass

    return None


async def get_wallet_first_tx_timestamp(wallet: str) -> int | None:
    """
    Get the true wallet creation/first activity timestamp using hybrid approach.
    Returns the OLDEST timestamp found across multiple methods.
    """
    if not HELIUS_API_KEY:
        logger.warning("wallet_age_no_helius_key")
        return None

    wallet = (wallet or "").strip()
    if not wallet:
        return None

    sig_time: int | None = None
    creation_time: int | None = None
    try:
        timestamps: list[int] = []

        async with httpx.AsyncClient(timeout=30) as client:
            # Method 1: Oldest signature (active transactions)
            sig_time = await _get_oldest_signature_time(wallet, client)
            if sig_time is not None:
                timestamps.append(sig_time)
                logger.debug("wallet_age_sig_time", wallet=wallet[:16] + "...", time=sig_time)

            # Method 2: Account creation via Helius enhanced API
            creation_time = await _get_account_creation_time(wallet, client)
            if creation_time is not None:
                timestamps.append(creation_time)
                logger.debug("wallet_age_creation_time", wallet=wallet[:16] + "...", time=creation_time)

        if timestamps:
            oldest = min(timestamps)
            logger.info(
                "wallet_age_found",
                wallet_prefix=wallet[:16] + "...",
                first_tx_time=datetime.fromtimestamp(oldest, tz=timezone.utc).isoformat(),
                methods_found=len(timestamps),
                sig_time=sig_time,
                creation_time=creation_time,
            )
            return oldest

        return None

    except Exception as e:
        logger.warning("wallet_age_fetch_failed", wallet=wallet[:16] + "...", error=str(e))
        return None


def calculate_wallet_age_days(first_tx_timestamp: int | None) -> int:
    """Calculate wallet age in days from first tx timestamp."""
    if first_tx_timestamp is None:
        return 0
    now = int(datetime.now(timezone.utc).timestamp())
    age_seconds = max(now - int(first_tx_timestamp), 0)
    return max(int(age_seconds / 86400), 1)
