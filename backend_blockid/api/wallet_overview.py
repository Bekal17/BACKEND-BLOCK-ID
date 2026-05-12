"""
Wallet overview API — dashboard endpoint for trust score and behavioral pattern.

GET /wallet_overview/{wallet}
Read-only. Uses wallets, wallet_scores, wallet_reasons tables.
"""

from __future__ import annotations

from cachetools import TTLCache
from fastapi import APIRouter, HTTPException

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter()

overview_cache: TTLCache = TTLCache(maxsize=2000, ttl=300)

DEFAULT_BEHAVIORAL_PATTERN = [
    "No suspicious activity detected",
    "Limited transaction history",
]

REASON_TO_LABEL = {
    "SCAM_CLUSTER_MEMBER": "Cluster-linked wallet",
    "SCAM_CLUSTER_MEMBER_SMALL": "Cluster-linked wallet",
    "SCAM_CLUSTER_MEMBER_LARGE": "Cluster-linked wallet",
    "HIGH_PROPAGATION_RISK": "High network exposure",
    "LOW_ACTIVITY": "Long-term holder",
    "NEW_WALLET": "New wallet",
    "CLEAN_HISTORY": "Clean history",
    "LONG_TERM_ACTIVE": "Long-term active",
    "DEX_TRADER": "DEX trader",
    "NFT_COLLECTOR": "NFT collector",
}


def _default_response(wallet: str) -> dict:
    return {
        "wallet": wallet,
        "trust_score": 0,
        "risk_level": "unknown",
        "behavioral_pattern": list(DEFAULT_BEHAVIORAL_PATTERN),
    }


def _get_recommended_actions(
    behavioral_pattern: list[str],
    cluster_info: dict | None,
    score: float,
) -> list[str]:
    actions = []

    if cluster_info and cluster_info.get("cluster_type") == "scam":
        actions.append("Disconnect from all token approvals linked to this cluster.")
        actions.append("Avoid interacting with wallets in Cluster #{}.".format(cluster_info.get("cluster_id", "?")))

    if any("cluster" in p.lower() for p in behavioral_pattern):
        actions.append("Review recent counterparties for suspicious activity.")
        actions.append("Reassess cluster-related transaction patterns.")

    if any("new wallet" in p.lower() for p in behavioral_pattern):
        actions.append("Monitor this new wallet closely before trusting it.")

    if any("low activity" in p.lower() or "holder" in p.lower() for p in behavioral_pattern):
        actions.append("Verify wallet identity before large transfers.")

    if any("dex" in p.lower() for p in behavioral_pattern):
        actions.append("Review DEX transaction history for wash trading patterns.")

    if any("nft" in p.lower() for p in behavioral_pattern):
        actions.append("Check NFT collection for known scam projects.")

    if any("clean" in p.lower() for p in behavioral_pattern):
        actions.append("No immediate action required. Continue monitoring.")

    if score < 30:
        actions.append("Do NOT send funds to this wallet.")
        actions.append("Report wallet to Solana community watchlists.")
    elif score < 50:
        actions.append("Exercise caution before any interaction.")

    if not actions:
        actions.append("Monitor wallet for unusual activity.")
        actions.append("Verify all recent transactions.")

    return actions


def _build_summary_message(
    score: float,
    cluster_info: dict | None,
    reason_codes: list[str],
    raw_ml_score: float,
) -> str:
    """
    Build summary message based on actual reason codes and score.
    """
    reason_set = set(reason_codes)

    # Highest priority: scam cluster
    if cluster_info and cluster_info.get("cluster_type") == "scam":
        size = cluster_info.get("size", 0)
        return f"Wallet is linked to a scam cluster with {size} other members."

    # Critical reason codes
    if "MEGA_DRAINER" in reason_set or "DRAINER_FLOW" in reason_set or "DRAINER_FLOW_DETECTED" in reason_set:
        return "Wallet is associated with drainer activity. Do not send funds."
    if "RUG_PULL_DEPLOYER" in reason_set:
        return "Wallet is associated with rug pull activity. Proceed with extreme caution."
    if any(r in reason_set for r in ["SCAM_CLUSTER_MEMBER", "SCAM_CLUSTER_MEMBER_LARGE", "SCAM_CLUSTER_MEMBER_SMALL"]):
        return "Wallet is linked to high-risk network activity."
    if "BLACKLISTED_CREATOR" in reason_set:
        return "Wallet is flagged as a known scammer. Avoid all interactions."
    if "HIGH_RISK_TOKEN_INTERACTION" in reason_set or "SUSPICIOUS_TOKEN_MINT" in reason_set:
        return "High-risk token interaction detected. Verify before transacting."
    if "DRAINER_INTERACTION" in reason_set:
        return "Wallet has interacted with a known drainer. Exercise caution."

    # ML score signal
    if raw_ml_score > 0 and raw_ml_score < 30 and score < 30:
        return "ML model detected high-risk behavioral patterns. Do not transact."

    # Score-based with reason context
    if score < 30:
        if "NEW_WALLET" in reason_set:
            return "New wallet with high-risk behavioral signals detected."
        return "Multiple risk signals detected. Do not transact with this wallet."

    if score < 50:
        if "NEW_WALLET" in reason_set:
            return "New wallet with limited history. Exercise caution."
        if "HIGH_PROPAGATION_RISK" in reason_set:
            return "Wallet has indirect exposure to risky network activity."
        return "Moderate risk signals detected. Verify wallet before transacting."

    if score < 70:
        if "NEW_WALLET" in reason_set:
            return "New wallet with no established history. Monitor activity."
        return "Low risk signals present. Continue monitoring."

    return "No major threats detected."


@router.get("/wallet_overview/{wallet}")
async def get_wallet_overview(wallet: str) -> dict:
    """
    Return wallet overview for dashboard.
    Read-only. Uses wallets, wallet_scores, wallet_reasons.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    cache_key = f"overview:{wallet}"
    if cache_key in overview_cache:
        return overview_cache[cache_key]

    try:
        conn = await get_conn()
    except Exception as e:
        # DB unavailable — return minimal response so frontend still works
        return _default_response(wallet)

    try:
        score_row = await conn.fetchrow(
            "SELECT score, risk_level, raw_ml_score FROM trust_scores WHERE wallet = $1 ORDER BY updated_at DESC LIMIT 1",
            wallet,
        )
        if score_row is None:
            result = _default_response(wallet)
            overview_cache[cache_key] = result
            return result

        score = float(score_row["score"]) if score_row["score"] is not None else 0.0
        raw_risk = str(score_row["risk_level"] or "").strip()
        if raw_risk:
            risk_level = raw_risk
        elif score < 30:
            risk_level = "HIGH"
        elif score < 50:
            risk_level = "MEDIUM"
        elif score < 70:
            risk_level = "LOW"
        else:
            risk_level = "SAFE"
        raw_ml_score = float(score_row["raw_ml_score"] or 0) if score_row and score_row["raw_ml_score"] else 0.0

        behavioral_pattern: list[str] = []
        reason_rows = await conn.fetch(
            "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 ORDER BY created_at DESC LIMIT 5",
            wallet,
        )
        raw_reason_codes = []
        for r in reason_rows:
            code = (r["reason_code"] or "").strip()
            if code:
                raw_reason_codes.append(code)
        for r in reason_rows:
            code = (r["reason_code"] or "").strip()
            if code:
                label = REASON_TO_LABEL.get(code, code.replace("_", " ").title())
                if label not in behavioral_pattern:
                    behavioral_pattern.append(label)

        if not behavioral_pattern:
            behavioral_pattern = list(DEFAULT_BEHAVIORAL_PATTERN)

        # Lookup cluster membership
        cluster_info = None
        cluster_members: list[str] = []
        propagation_signal = "LOW"
        primary_risk_driver = None

        cluster_row = await conn.fetchrow(
            """
            SELECT wc.cluster_id, wc.cluster_type, wc.confidence_score
            FROM wallet_clusters wc
            WHERE wc.wallet = $1
            LIMIT 1
            """,
            wallet,
        )

        if cluster_row:
            cluster_id = cluster_row["cluster_id"]
            cluster_info = {
                "cluster_id": str(cluster_id),
                "cluster_type": cluster_row["cluster_type"] or "unknown",
                "confidence": float(cluster_row["confidence_score"] or 0.0),
                "size": 0,  # will be updated after fetching members
            }

            # Get all members of this cluster
            member_rows = await conn.fetch(
                """
                SELECT wallet FROM wallet_cluster_members
                WHERE cluster_id = $1
                """,
                cluster_id,
            )
            cluster_members = [
                r["wallet"] for r in member_rows
                if r["wallet"] and r["wallet"] != wallet
            ]

            # Also check wallet_clusters for other members with same cluster_id
            wc_rows = await conn.fetch(
                """
                SELECT wallet FROM wallet_clusters
                WHERE cluster_id = $1 AND wallet != $2
                """,
                cluster_id, wallet,
            )
            for r in wc_rows:
                if r["wallet"] and r["wallet"] not in cluster_members:
                    cluster_members.append(r["wallet"])

            cluster_info["size"] = len(cluster_members) + 1  # +1 to include the wallet itself

        propagation_signal = "LOW"
        if cluster_info:
            if score < 40:
                propagation_signal = "HIGH"
            elif score < 60:
                propagation_signal = "MEDIUM"
            else:
                propagation_signal = "LOW"

        primary_risk_driver = None
        if cluster_info and cluster_info["cluster_type"] == "scam":
            primary_risk_driver = "SCAM_CLUSTER"

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database error: {e}") from e
    finally:
        await release_conn(conn)

    result = {
        "wallet": wallet,
        "trust_score": int(round(score)),
        "risk_tier": risk_level,
        "risk_level": risk_level,
        "risk_color": (
            "RED" if score < 30
            else "ORANGE" if score < 50
            else "YELLOW" if score < 70
            else "GREEN"
        ),
        "behavioral_pattern": behavioral_pattern,
        "cluster": cluster_info,
        "cluster_members": cluster_members,
        "propagation_signal": propagation_signal,
        "primary_risk_driver": primary_risk_driver,
        "category": (
            "SCAM_CLUSTER" if cluster_info and cluster_info.get("cluster_type") == "scam"
            else "HIGH_RISK" if score < 30
            else "MEDIUM_RISK" if score < 50
            else "LOW_RISK" if score < 70
            else "SAFE"
        ),
        "badges": [],
        "confidence": "HIGH" if cluster_info else "MEDIUM",
        "summary_message": _build_summary_message(score, cluster_info, raw_reason_codes, raw_ml_score),
        "recommended_actions": _get_recommended_actions(behavioral_pattern, cluster_info, score),
        "counterparties": [],
        "evidence": [],
        "exposure_ratio": round((100 - score) / 100, 2),
    }
    overview_cache[cache_key] = result
    return result
