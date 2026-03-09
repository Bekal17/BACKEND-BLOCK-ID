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
            "SELECT score, risk_level FROM trust_scores WHERE wallet = $1 ORDER BY updated_at DESC LIMIT 1",
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

        behavioral_pattern: list[str] = []
        reason_rows = await conn.fetch(
            "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 ORDER BY created_at DESC LIMIT 5",
            wallet,
        )
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
        "summary_message": (
            f"Wallet is linked to a scam cluster with {len(cluster_members)} other members."
            if cluster_info and cluster_info["cluster_type"] == "scam"
            else "No major threats detected."
        ),
        "recommended_actions": _get_recommended_actions(behavioral_pattern, cluster_info, score),
        "counterparties": [],
        "evidence": [],
        "exposure_ratio": round((100 - score) / 100, 2),
    }
    overview_cache[cache_key] = result
    return result
