"""
CEX Behavioral Fingerprint Detector

Detects Centralized Exchange (CEX) interaction patterns per wallet and
emits reason codes for badge display.

Pattern mirrors detect_positive_reasons — query transactions table,
bucket into tiers, emit reason codes compatible with insert_wallet_reason().

Tiers (USD-based, sum of deposits + withdrawals):
  - WHALE   > $10,000
  - ACTIVE  $1,000 - $10,000
  - CASUAL  < $1,000 (minimum 1 tx)

CEX coverage: Binance, OKX, Coinbase, Bybit, Kraken, KuCoin
(reuses KNOWN_CEX_ADDRESSES from behavioral_linking.py)
"""

from __future__ import annotations

import os
from typing import Any

import structlog

from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ml.behavioral_linking import KNOWN_CEX_ADDRESSES
from backend_blockid.ml.reason_codes import REASON_WEIGHTS

logger = structlog.get_logger(__name__)

# ============================================================
# Configuration
# ============================================================

WHALE_USD_THRESHOLD = float(os.getenv("CEX_WHALE_USD", "10000"))
ACTIVE_USD_THRESHOLD = float(os.getenv("CEX_ACTIVE_USD", "1000"))
SOL_USD_FALLBACK = float(os.getenv("SOL_USD_FALLBACK", "150.0"))
LAMPORTS_PER_SOL = 1_000_000_000
MIN_TX_COUNT = 1

CEX_NAME_MAP = {
    "Binance":  "BINANCE",
    "OKX":      "OKX",
    "Coinbase": "COINBASE",
    "Bybit":    "BYBIT",
    "Kraken":   "KRAKEN",
    "KuCoin":   "KUCOIN",
}

# ============================================================
# SOL -> USD conversion
# ============================================================

async def get_sol_usd_price() -> float:
    """
    Get current SOL/USD price with graceful fallback.

    Strategy:
      1. Try Jupiter Price API v6 (public, no auth)
      2. Fallback to SOL_USD_FALLBACK env var (default $150)

    Never raises — always returns a float.
    """
    try:
        import httpx
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(
                "https://price.jup.ag/v6/price",
                params={"ids": "SOL"},
            )
            if resp.status_code == 200:
                data = resp.json()
                price = data.get("data", {}).get("SOL", {}).get("price")
                if price and float(price) > 0:
                    return float(price)
    except Exception as e:
        logger.debug("jupiter_price_fetch_failed", error=str(e))

    return SOL_USD_FALLBACK


# ============================================================
# CEX interaction query
# ============================================================

async def _query_cex_interactions(
    wallet: str,
    conn,
) -> dict[str, dict[str, Any]]:
    """
    Query transactions table for CEX interactions (both directions).

    Returns:
      {
        "Binance": {"tx_count": 8, "volume_lamports": 12450000000},
        "OKX":     {"tx_count": 2, "volume_lamports":   500000000},
      }
    """
    cex_addresses = list(KNOWN_CEX_ADDRESSES.keys())
    if not cex_addresses:
        return {}

    try:
        rows = await conn.fetch(
            """
            SELECT
                counterparty,
                COUNT(*) AS tx_count,
                COALESCE(SUM(amount_lamports), 0)::BIGINT AS volume_lamports
            FROM (
                SELECT receiver AS counterparty, amount_lamports
                FROM transactions
                WHERE sender = $1
                  AND receiver = ANY($2::text[])
                UNION ALL
                SELECT sender AS counterparty, amount_lamports
                FROM transactions
                WHERE receiver = $1
                  AND sender = ANY($2::text[])
            ) combined
            GROUP BY counterparty
            """,
            wallet,
            cex_addresses,
        )
    except Exception as e:
        logger.debug(
            "cex_fingerprint_query_failed",
            wallet=wallet[:16],
            error=str(e),
        )
        return {}

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        address = row["counterparty"]
        cex_name = KNOWN_CEX_ADDRESSES.get(address)
        if not cex_name:
            continue

        if cex_name not in result:
            result[cex_name] = {"tx_count": 0, "volume_lamports": 0}

        result[cex_name]["tx_count"] += int(row["tx_count"])
        result[cex_name]["volume_lamports"] += int(row["volume_lamports"])

    return result


# ============================================================
# Tier classification
# ============================================================

def _classify_tier(volume_usd: float, tx_count: int) -> str | None:
    """
    Classify CEX interaction into tier based on USD volume.
    """
    if volume_usd > WHALE_USD_THRESHOLD:
        return "WHALE"
    if volume_usd > ACTIVE_USD_THRESHOLD:
        return "ACTIVE"
    if tx_count >= MIN_TX_COUNT:
        return "CASUAL"
    return None


# ============================================================
# Main detector
# ============================================================

async def detect_cex_fingerprint(wallet: str) -> list[dict[str, Any]]:
    """
    Detect CEX behavioral fingerprint for a wallet.

    Queries transactions table for interactions with known CEX hot wallets
    (both deposits and withdrawals), computes USD volume, and emits one
    reason code per CEX that meets tier criteria.

    Output format (same as detect_positive_reasons):
      [
        {
            "code": "BINANCE_WHALE",
            "weight": 0,
            "confidence": 1.0,
            "tx_hash": None,
            "source": "on_chain",
            "metadata": {
                "cex": "Binance",
                "tier": "whale",
                "tx_count": 8,
                "volume_sol": 82.93,
                "volume_usd": 12450.0,
            },
        },
        ...
      ]

    Graceful degradation: never raises, returns [] on error.
    """
    if not wallet:
        return []

    conn = None
    try:
        conn = await get_conn()

        interactions = await _query_cex_interactions(wallet, conn)
        if not interactions:
            return []

        sol_usd = await get_sol_usd_price()

        results: list[dict[str, Any]] = []
        for cex_name, stats in interactions.items():
            tx_count = stats["tx_count"]
            volume_lamports = stats["volume_lamports"]
            volume_sol = volume_lamports / LAMPORTS_PER_SOL
            volume_usd = volume_sol * sol_usd

            tier = _classify_tier(volume_usd, tx_count)
            if tier is None:
                continue

            cex_prefix = CEX_NAME_MAP.get(cex_name)
            if not cex_prefix:
                logger.warning(
                    "cex_fingerprint_unknown_cex",
                    cex=cex_name,
                    wallet=wallet[:16],
                )
                continue

            code = f"{cex_prefix}_{tier}"
            weight = REASON_WEIGHTS.get(code, 0)

            results.append({
                "code": code,
                "weight": weight,
                "confidence": 1.0,
                "tx_hash": None,
                "source": "on_chain",
                "metadata": {
                    "cex": cex_name,
                    "tier": tier.lower(),
                    "tx_count": tx_count,
                    "volume_sol": round(volume_sol, 4),
                    "volume_usd": round(volume_usd, 2),
                    "sol_usd_price": round(sol_usd, 2),
                },
            })

        logger.info(
            "cex_fingerprint_detected",
            wallet=wallet[:16],
            badge_count=len(results),
            codes=[r["code"] for r in results],
        )
        return results

    except Exception as e:
        logger.debug(
            "cex_fingerprint_detect_failed",
            wallet=wallet[:16],
            error=str(e),
        )
        return []

    finally:
        if conn is not None:
            await release_conn(conn)
