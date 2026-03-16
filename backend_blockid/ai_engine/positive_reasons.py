"""
Positive reason codes for BlockID Trust Score.

These reasons explain why a wallet is considered safe or low-risk.
Reads real on-chain data from trust_scores and transactions tables.
"""

from __future__ import annotations

import json
from typing import List, Dict, Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ml.reason_codes import get_reason_weights

logger = get_logger(__name__)

# DEX program identifiers (base58 prefixes or full IDs)
DEX_PROGRAM_PREFIXES = ("JUP", "whirLb", "9W959Dp", "srmqPvym", "675kPX9", "CAMMCzo")

# Negative reason codes (for clean history check)
NEGATIVE_REASON_CODES = frozenset({
    "SCAM_CLUSTER_MEMBER", "SCAM_CLUSTER_MEMBER_SMALL", "SCAM_CLUSTER_MEMBER_LARGE",
    "RUG_PULL_DEPLOYER", "BLACKLISTED_CREATOR", "DRAINER_FLOW_DETECTED",
    "DRAINER_FLOW", "MEGA_DRAINER", "NEAR_SCAM_CLUSTER", "DRAINER_INTERACTION",
    "HIGH_VOLUME_TO_SCAM", "RUGPULL_DEPLOYER", "SCAM_DISTANCE",
})

POSITIVE_REASON_CODES: List[Dict[str, Any]] = [
    {"code": "NO_RISK_DETECTED", "weight": 0, "description": "No suspicious activity detected."},
    {"code": "NO_SCAM_HISTORY", "weight": 10, "description": "Wallet has no known scam history."},
    {"code": "NORMAL_ACTIVITY_PATTERN", "weight": 5, "description": "Wallet transaction pattern looks normal."},
    {"code": "LOW_RISK_CLUSTER", "weight": 8, "description": "Wallet is not connected to known scam clusters."},
    {"code": "CLEAN_HISTORY", "weight": 10, "description": "No suspicious transaction history."},
    {"code": "LONG_HISTORY", "weight": 10, "description": "Wallet has long transaction history."},
    {"code": "LONG_TERM_ACTIVE", "weight": 10, "description": "Wallet has been active for over a year."},
    {"code": "MULTI_YEAR_ACTIVITY", "weight": 10, "description": "Wallet active across multiple years."},
    {"code": "AGE_1Y", "weight": 5, "description": "Wallet age at least 1 year."},
    {"code": "AGE_3Y", "weight": 10, "description": "Wallet age at least 3 years."},
    {"code": "AGE_5Y", "weight": 15, "description": "Wallet age at least 5 years."},
    {"code": "AGE_7Y", "weight": 18, "description": "Wallet age at least 7 years."},
    {"code": "AGE_10Y", "weight": 20, "description": "Wallet age at least 10 years."},
    {"code": "FAR_FROM_SCAM_CLUSTER", "weight": 6, "description": "Wallet is far from scam clusters."},
    {"code": "NFT_COLLECTOR", "weight": 5, "description": "Wallet holds NFT collections."},
    {"code": "DEX_TRADER", "weight": 5, "description": "Wallet participates in DEX trading."},
    {"code": "WHALE_100_SOL", "weight": 3, "description": "Wallet has held 100+ SOL."},
    {"code": "WHALE_1K_SOL", "weight": 5, "description": "Wallet has held 1K+ SOL."},
    {"code": "WHALE_5K_SOL", "weight": 8, "description": "Wallet has held 5K+ SOL."},
    {"code": "WHALE_10K_SOL", "weight": 10, "description": "Wallet has held 10K+ SOL."},
    {"code": "WHALE_50K_SOL", "weight": 12, "description": "Wallet has held 50K+ SOL."},
]


def default_positive_reason() -> Dict[str, Any]:
    """Return a safe default positive reason."""
    return {
        "code": "NO_RISK_DETECTED",
        "weight": 0,
        "confidence": 1.0,
        "tx_hash": None,
        "source": "default",
    }


def _reason(code: str, weight: int | None = None, tx_hash: str | None = None) -> Dict[str, Any]:
    """Build a reason dict. Uses get_reason_weights for weight if not provided."""
    w = get_reason_weights()
    wt = weight if weight is not None else w.get(code, 0)
    return {
        "code": code,
        "weight": wt,
        "confidence": 1.0,
        "tx_hash": tx_hash,
        "source": "on_chain",
    }


async def detect_positive_reasons(wallet: str) -> List[Dict[str, Any]]:
    """
    Detect positive reasons from trust_scores and transactions.
    Async, reads from PostgreSQL. Graceful degradation on any query failure.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return []

    reasons: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def add(code: str, weight: int | None = None, tx_hash: str | None = None) -> None:
        if code not in seen:
            seen.add(code)
            reasons.append(_reason(code, weight, tx_hash))

    conn = await get_conn()
    try:
        # Load trust_scores row
        row = await conn.fetchrow(
            """
            SELECT wallet_age_days, graph_distance, metadata_json, risk_level
            FROM trust_scores WHERE wallet = $1 LIMIT 1
            """,
            wallet,
        )

        wallet_age_days = 0
        graph_distance = 999
        metadata: dict = {}
        risk_level = ""

        if row:
            wallet_age_days = int(row.get("wallet_age_days") or 0)
            graph_distance = int(row.get("graph_distance") or 999)
            raw = row.get("metadata_json")
            if raw:
                try:
                    metadata = json.loads(raw) if isinstance(raw, str) else (raw or {})
                except Exception:
                    metadata = {}
            risk_level = str(row.get("risk_level") or "").upper()

        # Fallback: wallet_meta for age
        if wallet_age_days <= 0:
            try:
                meta_row = await conn.fetchrow(
                    "SELECT wallet_age_days FROM wallet_meta WHERE wallet = $1",
                    wallet,
                )
                if meta_row and meta_row.get("wallet_age_days") is not None:
                    wallet_age_days = int(meta_row["wallet_age_days"] or 0)
            except Exception:
                pass

        # Age-based reasons
        if wallet_age_days >= 3650:
            add("AGE_10Y")
        elif wallet_age_days >= 2555:
            add("AGE_7Y")
        elif wallet_age_days >= 1825:
            add("AGE_5Y")
        elif wallet_age_days >= 1095:
            add("AGE_3Y")
        elif wallet_age_days >= 365:
            add("AGE_1Y")

        if wallet_age_days >= 730:
            add("LONG_HISTORY")
        if wallet_age_days >= 365:
            add("LONG_TERM_ACTIVE")
        if wallet_age_days >= 730:
            add("MULTI_YEAR_ACTIVITY")

        # Cluster
        if graph_distance > 5:
            add("LOW_RISK_CLUSTER")
        if graph_distance > 3:
            add("FAR_FROM_SCAM_CLUSTER")

        # Whale from metadata
        max_balance = float(metadata.get("max_balance_sol") or metadata.get("max_balance") or 0)
        if max_balance >= 50000:
            add("WHALE_50K_SOL")
        elif max_balance >= 10000:
            add("WHALE_10K_SOL")
        elif max_balance >= 5000:
            add("WHALE_5K_SOL")
        elif max_balance >= 1000:
            add("WHALE_1K_SOL")
        elif max_balance >= 100:
            add("WHALE_100_SOL")

        # NFT from metadata
        nft_count = int(metadata.get("nft_count") or metadata.get("nft_count_held") or 0)
        if nft_count >= 500:
            add("NFT_500_PLUS")
        elif nft_count >= 200:
            add("NFT_200_PLUS")
        elif nft_count >= 100:
            add("NFT_100_PLUS")
        elif nft_count >= 50:
            add("NFT_50_PLUS")
        elif nft_count >= 10:
            add("NFT_10_PLUS")
        elif nft_count >= 1:
            add("NFT_COLLECTOR")

        # DEX count (transactions may not have program_id; graceful skip)
        dex_count = 0
        try:
            cols_row = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
            )
            cols = {r["column_name"] for r in cols_row}
            if "program_id" in cols:
                # Match wallet (schema may use wallet, from_wallet, sender)
                wallet_col = "wallet" if "wallet" in cols else ("from_wallet" if "from_wallet" in cols else "sender")
                dex_row = await conn.fetchrow(
                    f"""
                    SELECT COUNT(*)::int AS cnt FROM transactions
                    WHERE {wallet_col} = $1 AND program_id IS NOT NULL
                    AND (
                        program_id LIKE 'JUP%%' OR program_id LIKE 'whirLb%%'
                        OR program_id LIKE '9W959Dp%%' OR program_id LIKE 'srmqPvym%%'
                        OR program_id LIKE '675kPX9%%' OR program_id LIKE 'CAMMCzo%%'
                    )
                    """,
                    wallet,
                )
                if dex_row:
                    dex_count = int(dex_row.get("cnt") or 0)
        except Exception as e:
            logger.debug("positive_reasons_dex_skip", wallet=wallet[:16], error=str(e))

        if dex_count >= 500:
            add("DEX_TRADER_500_PLUS")
        elif dex_count >= 200:
            add("DEX_TRADER_200_PLUS")
        elif dex_count >= 100:
            add("DEX_TRADER_100_PLUS")
        elif dex_count >= 50:
            add("DEX_TRADER_50_PLUS")
        elif dex_count >= 10:
            add("DEX_TRADER_10_PLUS")
        elif dex_count >= 1:
            add("DEX_TRADER")

        # Clean history: LOW/SAFE risk and no negative reasons in wallet_reasons
        if risk_level in ("LOW", "SAFE"):
            try:
                wr_rows = await conn.fetch(
                    "SELECT reason_code FROM wallet_reasons WHERE wallet = $1",
                    wallet,
                )
                has_negative = any(
                    (r.get("reason_code") or "").strip() in NEGATIVE_REASON_CODES
                    for r in wr_rows
                )
                if not has_negative:
                    add("CLEAN_HISTORY")
                    add("NO_SCAM_HISTORY")
            except Exception as e:
                logger.debug("positive_reasons_clean_skip", wallet=wallet[:16], error=str(e))

    except Exception as e:
        logger.warning("positive_reasons_failed", wallet=wallet[:16], error=str(e))
    finally:
        await release_conn(conn)

    return reasons
