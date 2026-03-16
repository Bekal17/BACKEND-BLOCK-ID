"""
BlockID Identity NFT eligibility checker.

Rules:
1. already_minted → ALREADY_MINTED (not eligible)
2. risk_level in ("HIGH", "CRITICAL") → HIGH_RISK_WALLET (not eligible)
   SAFE, LOW, MEDIUM → eligible
3. tx_count == 0 → NO_TRANSACTION_HISTORY (not eligible)
4. All pass → eligible = True
"""

from __future__ import annotations


async def check_eligibility(wallet: str, conn) -> dict:
    """
    Check all eligibility rules for minting Identity NFT.

    Returns:
        {
            "eligible": bool,
            "reason": str | None,  # if not eligible
            "trust_score": float,
            "risk_level": str,
            "tx_count": int,
            "already_minted": bool
        }
    """
    wallet = (wallet or "").strip()
    result = {
        "eligible": False,
        "reason": None,
        "trust_score": 0.0,
        "risk_level": "",
        "tx_count": 0,
        "already_minted": False,
    }

    if not wallet:
        result["reason"] = "INVALID_WALLET"
        return result

    # 1. Check already minted
    existing = await conn.fetchrow(
        """
        SELECT mint_status, trust_score, risk_level
        FROM identity_nft
        WHERE wallet = $1
        """,
        wallet,
    )
    if existing and (existing.get("mint_status") or "").upper() == "MINTED":
        result["already_minted"] = True
        result["reason"] = "ALREADY_MINTED"
        result["trust_score"] = float(existing.get("trust_score") or 0)
        result["risk_level"] = str(existing.get("risk_level") or "")
        return result

    # 2. Get trust_scores (score, risk_level)
    ts_row = await conn.fetchrow(
        """
        SELECT score, risk_level, wallet_age_days, metadata_json
        FROM trust_scores
        WHERE wallet = $1
        ORDER BY computed_at DESC NULLS LAST, last_updated DESC NULLS LAST
        LIMIT 1
        """,
        wallet,
    )

    # 3. Get tx_count from transactions
    tx_row = await conn.fetchrow(
        """
        SELECT COUNT(*)::int AS cnt
        FROM transactions
        WHERE wallet = $1
        """,
        wallet,
    )
    tx_count = int(tx_row.get("cnt") or 0) if tx_row else 0

    # If no trust_scores row, wallet not scored yet - caller should run pipeline
    trust_score = 0.0
    risk_level = ""
    if ts_row:
        trust_score = float(ts_row.get("score") or 0)
        risk_level = str(ts_row.get("risk_level") or "").strip().upper()

    result["trust_score"] = trust_score
    result["risk_level"] = risk_level
    result["tx_count"] = tx_count

    # Rule 3: tx_count == 0 → NO_TRANSACTION_HISTORY
    if tx_count == 0:
        result["reason"] = "NO_TRANSACTION_HISTORY"
        return result

    # Rule 2: risk_level HIGH or CRITICAL → HIGH_RISK_WALLET
    if risk_level in ("HIGH", "CRITICAL"):
        result["reason"] = "HIGH_RISK_WALLET"
        return result

    result["eligible"] = True
    result["reason"] = None
    return result
