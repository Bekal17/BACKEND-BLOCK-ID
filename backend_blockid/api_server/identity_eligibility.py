"""
BlockID Identity NFT eligibility checker.

Rules:
1. already_minted → ALREADY_MINTED (not eligible)
2. score < 30 → BLOCKED_LOW_SCORE (not eligible)
3. risk_level in ("HIGH", "CRITICAL") → HIGH_RISK_WALLET (not eligible)
4. score >= 30 → eligible = True

Score tiers: BLOCKED (0-29), READ_ONLY (30-39), BASIC (40-49), STANDARD (50-69), TRUSTED (70+)
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
            "already_minted": bool,
            "score_tier": str,
        }
    """
    result = {
        "eligible": False,
        "reason": None,
        "trust_score": 0.0,
        "risk_level": "",
        "tx_count": 0,
        "already_minted": False,
        "score_tier": "BLOCKED",
    }

    wallet = (wallet or "").strip()
    if not wallet:
        result["reason"] = "INVALID_WALLET"
        return result

    # 1. Check already minted
    existing = await conn.fetchrow(
        "SELECT mint_status, trust_score, risk_level "
        "FROM identity_nft WHERE wallet = $1",
        wallet,
    )
    if existing and (existing.get("mint_status") or "").upper() == "MINTED":
        result["already_minted"] = True
        result["reason"] = "ALREADY_MINTED"
        result["trust_score"] = float(existing.get("trust_score") or 0)
        result["risk_level"] = str(existing.get("risk_level") or "")
        return result

    # 2. Get trust score
    ts_row = await conn.fetchrow(
        "SELECT score, risk_level FROM trust_scores "
        "WHERE wallet = $1 "
        "ORDER BY computed_at DESC NULLS LAST LIMIT 1",
        wallet,
    )
    trust_score = float(ts_row.get("score") or 0) if ts_row else 0.0
    risk_level = str(ts_row.get("risk_level") or "").strip().upper() if ts_row else ""

    result["trust_score"] = trust_score
    result["risk_level"] = risk_level

    # 3. Determine score tier
    if trust_score >= 70:
        result["score_tier"] = "TRUSTED"
    elif trust_score >= 50:
        result["score_tier"] = "STANDARD"
    elif trust_score >= 40:
        result["score_tier"] = "BASIC"
    elif trust_score >= 30:
        result["score_tier"] = "READ_ONLY"
    else:
        result["score_tier"] = "BLOCKED"

    # 4. Block if score < 30
    if trust_score < 30:
        result["reason"] = "BLOCKED_LOW_SCORE"
        return result

    # 5. Block if HIGH/CRITICAL risk
    if risk_level in ("HIGH", "CRITICAL"):
        result["reason"] = "HIGH_RISK_WALLET"
        return result

    # 6. Eligible (score >= 30 and not high risk)
    result["eligible"] = True
    result["reason"] = None
    return result


def get_score_tier(trust_score: float) -> str:
    """Return score tier string for a given trust score."""
    if trust_score >= 70:
        return "TRUSTED"
    elif trust_score >= 50:
        return "STANDARD"
    elif trust_score >= 40:
        return "BASIC"
    elif trust_score >= 30:
        return "READ_ONLY"
    else:
        return "BLOCKED"
