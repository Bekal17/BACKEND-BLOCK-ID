"""
CEX Behavioral Fingerprint Detector (Hybrid + Cached)

Detection strategy:
  1. Check cex_fingerprint_cache — return if fresh (<30 days)
  2. Query transactions table — fast path if data exists
  3. Fallback to Helius getSignaturesForAddress + getTransaction
     — slow path for comprehensive coverage

Cache TTL: 30 days, auto-refresh via Thursday cron pipeline.

Tiers (USD-based, sum of deposits + withdrawals):
  - WHALE   > $10,000
  - ACTIVE  $1,000 - $10,000
  - CASUAL  < $1,000 (minimum 1 tx)
"""

from __future__ import annotations

import json
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

# Helius API config (uses existing env vars from predict_wallet_score.py)
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

# Max signatures to fetch per wallet (Helius limit management)
HELIUS_MAX_SIGNATURES = int(os.getenv("CEX_HELIUS_MAX_SIGS", "500"))

# Cache TTL (should match cron Kamis frequency)
CACHE_TTL_DAYS = int(os.getenv("CEX_CACHE_TTL_DAYS", "30"))

CEX_NAME_MAP = {
    "Binance":  "BINANCE",
    "OKX":      "OKX",
    "Coinbase": "COINBASE",
    "Bybit":    "BYBIT",
    "Kraken":   "KRAKEN",
    "KuCoin":   "KUCOIN",
}

# ============================================================
# Cache layer
# ============================================================

async def _get_cached_badges(wallet: str, conn) -> list[dict[str, Any]] | None:
    """
    Return cached badges if fresh (<CACHE_TTL_DAYS old).
    Returns None if no cache or expired.
    """
    try:
        row = await conn.fetchrow(
            """
            SELECT badges
            FROM cex_fingerprint_cache
            WHERE wallet = $1 AND expires_at > NOW()
            """,
            wallet,
        )
        if row and row["badges"]:
            badges = row["badges"]
            if isinstance(badges, str):
                badges = json.loads(badges)
            logger.debug("cex_cache_hit", wallet=wallet[:16], count=len(badges))
            return badges
    except Exception as e:
        logger.debug("cex_cache_read_failed", wallet=wallet[:16], error=str(e))
    return None


async def _write_cache(
    wallet: str,
    badges: list[dict[str, Any]],
    source: str,
    sol_usd: float,
    conn,
) -> None:
    """
    Upsert detection result into cache table.
    """
    total_count = sum(b.get("metadata", {}).get("tx_count", 0) for b in badges)
    total_volume = sum(b.get("metadata", {}).get("volume_usd", 0) for b in badges)

    try:
        await conn.execute(
            f"""
            INSERT INTO cex_fingerprint_cache (
                wallet, detected_at, expires_at, badges, source,
                sol_usd_price, total_cex_count, total_volume_usd,
                refresh_count, last_refreshed_at
            ) VALUES (
                $1, NOW(), NOW() + INTERVAL '{CACHE_TTL_DAYS} days',
                $2::jsonb, $3, $4, $5, $6, 1, NOW()
            )
            ON CONFLICT (wallet) DO UPDATE SET
                detected_at = NOW(),
                expires_at = NOW() + INTERVAL '{CACHE_TTL_DAYS} days',
                badges = EXCLUDED.badges,
                source = EXCLUDED.source,
                sol_usd_price = EXCLUDED.sol_usd_price,
                total_cex_count = EXCLUDED.total_cex_count,
                total_volume_usd = EXCLUDED.total_volume_usd,
                refresh_count = cex_fingerprint_cache.refresh_count + 1,
                last_refreshed_at = NOW()
            """,
            wallet,
            json.dumps(badges),
            source,
            sol_usd,
            total_count,
            total_volume,
        )
    except Exception as e:
        logger.warning(
            "cex_cache_write_failed",
            wallet=wallet[:16],
            error=str(e),
        )


# ============================================================
# SOL -> USD price
# ============================================================

async def get_sol_usd_price() -> float:
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
# FAST PATH: transactions table query
# ============================================================

async def _detect_via_transactions_table(
    wallet: str,
    conn,
) -> dict[str, dict[str, Any]]:
    """
    Query transactions table for CEX interactions.
    Returns {} if no match found (caller should fallback to Helius).
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
                WHERE sender = $1 AND receiver = ANY($2::text[])
                UNION ALL
                SELECT sender AS counterparty, amount_lamports
                FROM transactions
                WHERE receiver = $1 AND sender = ANY($2::text[])
            ) combined
            GROUP BY counterparty
            """,
            wallet,
            cex_addresses,
        )
    except Exception as e:
        logger.debug(
            "cex_transactions_query_failed",
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
# SLOW PATH: Helius API fallback
# ============================================================

async def _detect_via_helius(wallet: str) -> dict[str, dict[str, Any]]:
    """
    Fallback: fetch signature history from Helius, extract SOL transfers,
    match against KNOWN_CEX_ADDRESSES.
    Returns {} on any error (graceful degrade).
    """
    if not HELIUS_API_KEY:
        logger.debug("cex_helius_no_api_key", wallet=wallet[:16])
        return {}

    try:
        import httpx

        # Step 1: Get signatures (up to HELIUS_MAX_SIGNATURES)
        async with httpx.AsyncClient(timeout=15.0) as client:
            sig_resp = await client.post(
                HELIUS_RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getSignaturesForAddress",
                    "params": [wallet, {"limit": HELIUS_MAX_SIGNATURES}],
                },
            )
            if sig_resp.status_code != 200:
                return {}
            sigs_data = sig_resp.json().get("result", [])
            if not sigs_data:
                return {}

            signatures = [s["signature"] for s in sigs_data if s.get("signature")]

            # Step 2: Fetch parsed transactions in batches
            # Helius getTransactions supports batch via enhanced API
            # For simplicity here, use parsed transaction API
            tx_resp = await client.post(
                f"https://api.helius.xyz/v0/transactions?api-key={HELIUS_API_KEY}",
                json={"transactions": signatures[:100]},  # max 100 per call
                timeout=20.0,
            )
            if tx_resp.status_code != 200:
                logger.debug(
                    "cex_helius_tx_fetch_failed",
                    wallet=wallet[:16],
                    status=tx_resp.status_code,
                )
                return {}

            transactions = tx_resp.json()
            if not isinstance(transactions, list):
                return {}

            # Step 3: Extract native SOL transfers and match CEX
            result: dict[str, dict[str, Any]] = {}
            for tx in transactions:
                native_transfers = tx.get("nativeTransfers", []) or []
                for transfer in native_transfers:
                    from_addr = transfer.get("fromUserAccount")
                    to_addr = transfer.get("toUserAccount")
                    amount = int(transfer.get("amount", 0))

                    # Determine counterparty (the non-wallet side)
                    counterparty = None
                    if from_addr == wallet and to_addr in KNOWN_CEX_ADDRESSES:
                        counterparty = to_addr
                    elif to_addr == wallet and from_addr in KNOWN_CEX_ADDRESSES:
                        counterparty = from_addr

                    if not counterparty:
                        continue

                    cex_name = KNOWN_CEX_ADDRESSES[counterparty]
                    if cex_name not in result:
                        result[cex_name] = {"tx_count": 0, "volume_lamports": 0}
                    result[cex_name]["tx_count"] += 1
                    result[cex_name]["volume_lamports"] += amount

            logger.info(
                "cex_helius_detection_done",
                wallet=wallet[:16],
                sigs_fetched=len(signatures),
                cex_matches=len(result),
            )
            return result

    except Exception as e:
        logger.debug("cex_helius_detect_failed", wallet=wallet[:16], error=str(e))
        return {}


# ============================================================
# Tier classification
# ============================================================

def _classify_tier(volume_usd: float, tx_count: int) -> str | None:
    if volume_usd > WHALE_USD_THRESHOLD:
        return "WHALE"
    if volume_usd > ACTIVE_USD_THRESHOLD:
        return "ACTIVE"
    if tx_count >= MIN_TX_COUNT:
        return "CASUAL"
    return None


# ============================================================
# Main detector (hybrid + cached)
# ============================================================

async def detect_cex_fingerprint(
    wallet: str,
    force_refresh: bool = False,
) -> list[dict[str, Any]]:
    """
    Detect CEX behavioral fingerprint for a wallet.

    Strategy:
      1. Check cache (if not force_refresh)
      2. Query transactions table (fast path)
      3. Fallback to Helius API (slow path)

    Args:
        wallet: Solana wallet address
        force_refresh: Bypass cache, force fresh detection (cron Kamis use)

    Returns:
        List of badge dicts compatible with insert_wallet_reason().
        Empty list if no CEX interactions or on error.
    """
    if not wallet:
        return []

    conn = None
    try:
        conn = await get_conn()

        # Step 1: cache check
        if not force_refresh:
            cached = await _get_cached_badges(wallet, conn)
            if cached is not None:
                return cached

        # Step 2: fast path — transactions table
        interactions = await _detect_via_transactions_table(wallet, conn)
        source = "transactions_table"

        # Step 3: slow path — Helius fallback
        if not interactions:
            interactions = await _detect_via_helius(wallet)
            source = "helius_api"

        if not interactions:
            # Cache empty result too (avoid re-fetching for 30 days)
            await _write_cache(wallet, [], "none", 0.0, conn)
            return []

        # Step 4: build badges
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
                    "detection_source": source,
                },
            })

        # Step 5: write to cache
        await _write_cache(wallet, results, source, sol_usd, conn)

        logger.info(
            "cex_fingerprint_detected",
            wallet=wallet[:16],
            badge_count=len(results),
            source=source,
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
