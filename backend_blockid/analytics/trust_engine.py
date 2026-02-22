"""
Trust engine: compute 0-100 trust score from metrics and risk.

Realistic formula: base 40 + tx/10 (capped) + age/30 (capped) + programs*2 (capped)
minus penalty per risk flag. Clamps to 0-100. Returns LOW / MEDIUM / HIGH and reason_codes.
"""

from __future__ import annotations

from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

BASE_SCORE = 40
TX_BONUS_CAP = 30
TX_DIVISOR = 10
AGE_BONUS_CAP = 20
AGE_MONTH_DIVISOR = 30
PROGRAMS_BONUS_CAP = 20
PROGRAMS_MULTIPLIER = 2
PENALTY_PER_FLAG = 15
SCORE_MIN = 0
SCORE_MAX = 100

SCAM_PENALTY = 40
NFT_SCAM_SCAMMER_PENALTY = 35
RUGPULL_TOKEN_PENALTY = 30
SCAM_CLUSTER_PENALTY = 50

ROLE_SCAMMER = "scammer"

# Reason codes returned to API and stored in DB
REASON_NEW_WALLET = "NEW_WALLET"
REASON_LOW_ACTIVITY = "LOW_ACTIVITY"
REASON_KNOWN_SCAM_PROGRAM = "KNOWN_SCAM_PROGRAM"
REASON_SCAM_NFT_CREATOR = "SCAM_NFT_CREATOR"
REASON_SCAM_NFT_RECEIVED = "SCAM_NFT_RECEIVED"
REASON_RUG_PULL_TOKEN = "RUG_PULL_TOKEN"
REASON_SERVICE_WALLET = "SERVICE_WALLET"
REASON_COLD_WALLET = "COLD_WALLET"
REASON_SCAM_CLUSTER = "SCAM_CLUSTER"


def _build_reason_codes(
    risk: dict[str, Any],
    scam_interactions: int,
    rugpull_interactions: int,
    in_scam_cluster: bool,
    nft_scam_role: str | None,
    wallet_type: str | None,
    nft_scam: dict[str, Any] | None,
) -> list[str]:
    """Build ordered list of reason codes from risk flags, scam signals, and wallet type."""
    codes: list[str] = []
    flags = risk.get("flags") or []

    if "new_wallet" in flags:
        codes.append(REASON_NEW_WALLET)
    if "low_activity" in flags or "inactive" in flags:
        codes.append(REASON_LOW_ACTIVITY)
    if scam_interactions > 0:
        codes.append(REASON_KNOWN_SCAM_PROGRAM)
        codes.append(REASON_RUG_PULL_TOKEN)
    if rugpull_interactions > 0 and REASON_RUG_PULL_TOKEN not in codes:
        codes.append(REASON_RUG_PULL_TOKEN)
    if in_scam_cluster:
        codes.append(REASON_SCAM_CLUSTER)
    if nft_scam_role == ROLE_SCAMMER:
        codes.append(REASON_SCAM_NFT_CREATOR)
    if nft_scam:
        received = int(nft_scam.get("received_scam_nft") or 0)
        if received > 0:
            codes.append(REASON_SCAM_NFT_RECEIVED)
        if nft_scam.get("is_creator") and REASON_SCAM_NFT_CREATOR not in codes:
            codes.append(REASON_SCAM_NFT_CREATOR)
    if wallet_type == "service_wallet":
        codes.append(REASON_SERVICE_WALLET)
    if wallet_type == "cold_wallet":
        codes.append(REASON_COLD_WALLET)

    return codes


def calculate_trust(
    metrics: dict[str, Any],
    risk: dict[str, Any],
    scam_interactions: int = 0,
    rugpull_interactions: int = 0,
    in_scam_cluster: bool = False,
    nft_scam_role: str | None = None,
    wallet_type: str | None = None,
    nft_scam: dict[str, Any] | None = None,
) -> tuple[int, str, list[str]]:
    """
    Compute trust score (0-100), risk label, and reason codes.

    Formula: score = 40 + ... - len(flags)*15 - scam_penalty - nft_scam_penalty - rugpull_penalty - scam_cluster_penalty. Clamp 0-100.
    If scam_interactions > 0: subtract SCAM_PENALTY and force risk_label to HIGH.
    If nft_scam_role == "scammer": subtract NFT_SCAM_SCAMMER_PENALTY and force risk_label to HIGH.
    If in_scam_cluster (wallet in cluster with scam counterparty): subtract SCAM_CLUSTER_PENALTY and force risk_label to HIGH.
    Returns (score, risk_level string, reason_codes list).
    """
    score = BASE_SCORE

    tx_count = int(metrics.get("tx_count") or 0)
    score += min(tx_count // TX_DIVISOR, TX_BONUS_CAP)

    wallet_age_days = int(metrics.get("wallet_age_days") or 0)
    score += min(wallet_age_days // AGE_MONTH_DIVISOR, AGE_BONUS_CAP)

    unique_programs = int(metrics.get("unique_programs") or 0)
    score += min(unique_programs * PROGRAMS_MULTIPLIER, PROGRAMS_BONUS_CAP)

    flags = risk.get("flags") or []
    score -= len(flags) * PENALTY_PER_FLAG

    if scam_interactions > 0:
        score -= SCAM_PENALTY
        risk_label = "HIGH"
    elif nft_scam_role == ROLE_SCAMMER:
        score -= NFT_SCAM_SCAMMER_PENALTY
        risk_label = "HIGH"
    elif in_scam_cluster:
        score -= SCAM_CLUSTER_PENALTY
        risk_label = "HIGH"
    else:
        risk_label = risk.get("risk_level") or "LOW"

    if rugpull_interactions > 0:
        score -= RUGPULL_TOKEN_PENALTY

    score = max(SCORE_MIN, min(SCORE_MAX, score))
    reason_codes = _build_reason_codes(
        risk, scam_interactions, rugpull_interactions, in_scam_cluster, nft_scam_role, wallet_type, nft_scam
    )

    logger.debug(
        "trust_engine_result",
        wallet=(metrics.get("wallet") or "")[:16] + "...",
        score=score,
        risk_label=risk_label,
        reason_codes=reason_codes,
    )
    return (score, risk_label, reason_codes)
