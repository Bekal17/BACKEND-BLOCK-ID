"""
BlockID Identity NFT metadata builder.

Builds NFT metadata from trust_scores + wallet_reasons.
"""

from __future__ import annotations

from datetime import date


async def get_wallet_age_days_from_meta(conn, wallet: str) -> int:
    """Fetch wallet_age_days from wallet_meta table (source of truth)."""
    try:
        age_row = await conn.fetchrow(
            "SELECT wallet_age_days FROM wallet_meta WHERE wallet = $1",
            wallet,
        )
        return (
            int(age_row["wallet_age_days"])
            if age_row and age_row.get("wallet_age_days")
            else 0
        )
    except Exception:
        return 0


def build_metadata(
    wallet: str,
    trust_score_row: dict,
    reasons: list[str],
    *,
    is_sanctioned: bool = False,
    daemon_risk_score: int | None = None,
    daemon_risk_level: str | None = None,
    wallet_age_days: int | None = None,
) -> dict:
    """
    Build NFT metadata from trust_scores + wallet_reasons.

    behavioral_fingerprint logic:
    - score >= 80 and CLEAN_HISTORY → "clean_trader"
    - DEX_TRADER_* in reasons → "active_dex_trader"
    - NFT_COLLECTOR or NFT_*_PLUS in reasons → "nft_collector"
    - WHALE_* in reasons → "whale"
    - score < 40 → "high_risk"
    - default → "standard_user"

    badges: top 5 positive reason_codes (weight > 0, ordered by weight DESC)
    """
    score = float(trust_score_row.get("score") or 0)
    risk_level = str(trust_score_row.get("risk_level") or "").strip()
    age_days = (
        wallet_age_days
        if wallet_age_days is not None and wallet_age_days > 0
        else int(trust_score_row.get("wallet_age_days") or 0)
    )

    reason_set = {r.upper() for r in reasons if r}

    # behavioral_fingerprint
    behavioral_fingerprint = "standard_user"
    if score < 40:
        behavioral_fingerprint = "high_risk"
    elif "WHALE_100_SOL" in reason_set or "WHALE_1K_SOL" in reason_set or "WHALE_5K_SOL" in reason_set or "WHALE_10K_SOL" in reason_set or "WHALE_50K_SOL" in reason_set:
        behavioral_fingerprint = "whale"
    elif "NFT_COLLECTOR" in reason_set or any(r.startswith("NFT_") and "_PLUS" in r for r in reason_set):
        behavioral_fingerprint = "nft_collector"
    elif any(r.startswith("DEX_TRADER") for r in reason_set):
        behavioral_fingerprint = "active_dex_trader"
    elif score >= 80 and "CLEAN_HISTORY" in reason_set:
        behavioral_fingerprint = "clean_trader"

    return {
        "wallet": wallet,
        "trust_score": round(score, 1),
        "risk_level": risk_level,
        "handle": None,
        "badges": list(reasons[:5]) if isinstance(reasons, list) else [],
        "wallet_age_days": age_days,
        "behavioral_fingerprint": behavioral_fingerprint,
        "is_sanctioned": is_sanctioned,
        "daemon_risk_score": daemon_risk_score,
        "daemon_risk_level": daemon_risk_level,
        "last_updated": date.today().isoformat(),
    }
