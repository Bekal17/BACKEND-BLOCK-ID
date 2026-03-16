"""
Trust engine: compute 0-100 trust score from metrics, internal risk signals,
and Daemon Protocol external data (risk score + sanctions).

Step 1 (BlockID base): base 40 + tx/age/programs bonuses - internal penalties
Step 2 (Daemon modifier): daemon_modifier = -(daemon.risk_score * 0.4)
Step 3 (CRITICAL override): if daemon.is_critical or daemon.is_sanctioned
    → risk_label HIGH, score capped at 20
Step 4: resolve_final_risk() applies hierarchy (see docstring)

Returns (score: int, risk_label: str, reason_codes: list[str])
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from backend_blockid.blockid_logging import get_logger

if TYPE_CHECKING:
    from backend_blockid.integrations.daemon_client import DaemonResult

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Score components
# ---------------------------------------------------------------------------
BASE_SCORE = 40  # unchanged — on-chain bonuses capped at +30
# positive reasons capped at +20; linking signals capped at +15
# absolute max = 97 (hard cap)
SCORE_ABSOLUTE_MAX = 97  # never reach 100
POSITIVE_REASONS_CAP = 20
LINKING_SIGNALS_CAP = 15
TX_BONUS_CAP = 30
TX_DIVISOR = 10
AGE_BONUS_CAP = 20
AGE_MONTH_DIVISOR = 30
PROGRAMS_BONUS_CAP = 20
PROGRAMS_MULTIPLIER = 2

# ---------------------------------------------------------------------------
# Internal penalties
# ---------------------------------------------------------------------------
PENALTY_PER_FLAG = 15
SCAM_PENALTY = 40
NFT_SCAM_SCAMMER_PENALTY = 35
RUGPULL_TOKEN_PENALTY = 30
SCAM_CLUSTER_PENALTY = 50

ROLE_SCAMMER = "scammer"

# ---------------------------------------------------------------------------
# Risk label thresholds
# ---------------------------------------------------------------------------
THRESHOLD_HIGH = 40
THRESHOLD_MEDIUM = 70

# ---------------------------------------------------------------------------
# Reason codes
# ---------------------------------------------------------------------------
REASON_NEW_WALLET = "NEW_WALLET"
REASON_LOW_ACTIVITY = "LOW_ACTIVITY"
REASON_KNOWN_SCAM_PROGRAM = "KNOWN_SCAM_PROGRAM"
REASON_SCAM_NFT_CREATOR = "SCAM_NFT_CREATOR"
REASON_SCAM_NFT_RECEIVED = "SCAM_NFT_RECEIVED"
REASON_RUG_PULL_TOKEN = "RUG_PULL_TOKEN"
REASON_SERVICE_WALLET = "SERVICE_WALLET"
REASON_COLD_WALLET = "COLD_WALLET"
REASON_SCAM_CLUSTER = "SCAM_CLUSTER"
# Daemon-sourced reason codes
REASON_DAEMON_SANCTIONED = "DAEMON_SANCTIONED"
REASON_DAEMON_CRITICAL = "DAEMON_CRITICAL"
REASON_DAEMON_HIGH_RISK = "DAEMON_HIGH_RISK"
REASON_DAEMON_MODIFIER = "DAEMON_MODIFIER"


def resolve_final_risk(
    internal_score: int,
    internal_risk: str,
    daemon: "DaemonResult | None",
    wallet: str = "",
) -> tuple[int, str]:
    """
    Resolve final score and risk_label using Daemon hierarchy.

    daemon_modifier = -(daemon.risk_score * 0.4)
    Daemon 100 → -40, Daemon 50 → -20, Daemon 0 → 0

    Priority 1: Daemon CRITICAL or isSanctioned
                → force risk_label HIGH, cap score at 20
    Priority 2: Daemon HIGH + Internal HIGH (double confirmed)
                → apply full daemon_modifier, risk HIGH
    Priority 3: Daemon LOW + Internal HIGH
                → trust internal (Solana-specific), apply only 50% daemon_modifier
    Priority 4: Daemon HIGH + Internal LOW
                → flag for review, apply full daemon_modifier, risk_label MEDIUM
    Priority 5: Daemon None (down/timeout)
                → return internal_score and internal_risk unchanged

    Returns (final_score, final_risk_label).
    """
    if daemon is None:
        return (internal_score, internal_risk)

    daemon_modifier = -(daemon.risk_score * 0.4)
    internal_high = internal_risk == "HIGH"
    daemon_high = daemon.risk_level in ("HIGH", "CRITICAL") or daemon.risk_score >= 50
    daemon_critical = daemon.is_critical or daemon.is_sanctioned
    wallet_preview = (wallet or "")[:16] + "..." if wallet else ""

    # Priority 1: Daemon CRITICAL or sanctioned → cap score at 20, risk HIGH
    if daemon_critical:
        after = max(0, min(97, internal_score + daemon_modifier))
        final_score = min(int(after), 20)
        logger.info(
            "trust_engine_daemon_modifier",
            wallet=wallet_preview,
            daemon_risk_score=daemon.risk_score,
            daemon_modifier=daemon_modifier,
            daemon_is_sanctioned=daemon.is_sanctioned,
            before_score=internal_score,
            after_score=final_score,
        )
        return (final_score, "HIGH")

    # Priority 2: Daemon HIGH + Internal HIGH → full modifier
    if daemon_high and internal_high:
        after = max(0, min(97, internal_score + daemon_modifier))
        logger.info(
            "trust_engine_daemon_modifier",
            wallet=wallet_preview,
            daemon_risk_score=daemon.risk_score,
            daemon_modifier=daemon_modifier,
            daemon_is_sanctioned=daemon.is_sanctioned,
            before_score=internal_score,
            after_score=int(after),
        )
        return (int(after), "HIGH")

    # Priority 3: Daemon LOW + Internal HIGH → 50% modifier
    if not daemon_high and internal_high:
        half_mod = daemon_modifier * 0.5
        after = max(0, min(97, internal_score + half_mod))
        logger.info(
            "trust_engine_daemon_modifier",
            wallet=wallet_preview,
            daemon_risk_score=daemon.risk_score,
            daemon_modifier=half_mod,
            daemon_is_sanctioned=daemon.is_sanctioned,
            before_score=internal_score,
            after_score=int(after),
        )
        return (int(after), internal_risk)

    # Priority 4: Daemon HIGH + Internal LOW → full modifier, risk MEDIUM
    if daemon_high and not internal_high:
        after = max(0, min(97, internal_score + daemon_modifier))
        final_score = int(after)
        risk_label = "MEDIUM" if final_score >= THRESHOLD_HIGH else "HIGH"
        logger.info(
            "trust_engine_daemon_modifier",
            wallet=wallet_preview,
            daemon_risk_score=daemon.risk_score,
            daemon_modifier=daemon_modifier,
            daemon_is_sanctioned=daemon.is_sanctioned,
            before_score=internal_score,
            after_score=final_score,
        )
        return (final_score, risk_label)

    # Default: apply full modifier
    after = max(0, min(97, internal_score + daemon_modifier))
    logger.info(
        "trust_engine_daemon_modifier",
        wallet=wallet_preview,
        daemon_risk_score=daemon.risk_score,
        daemon_modifier=daemon_modifier,
        daemon_is_sanctioned=daemon.is_sanctioned,
        before_score=internal_score,
        after_score=int(after),
    )
    return (int(after), internal_risk)


def _build_reason_codes(
    risk: dict[str, Any],
    scam_interactions: int,
    rugpull_interactions: int,
    in_scam_cluster: bool,
    nft_scam_role: str | None,
    wallet_type: str | None,
    nft_scam: dict[str, Any] | None,
    daemon: "DaemonResult | None" = None,
) -> list[str]:
    """Build ordered list of reason codes from all signal sources."""
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

    # --- Daemon-sourced reason codes ---
    if daemon is not None:
        if daemon.is_sanctioned:
            codes.append(REASON_DAEMON_SANCTIONED)
        if daemon.is_critical:
            codes.append(REASON_DAEMON_CRITICAL)
        elif daemon.risk_score >= 50:
            codes.append(REASON_DAEMON_HIGH_RISK)
        codes.append(REASON_DAEMON_MODIFIER)

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
    daemon: "DaemonResult | None" = None,  # NEW: Daemon Protocol data
) -> tuple[int, str, list[str]]:
    """
    Compute trust score (0-100), risk label, and reason codes.

    Inputs:
      - metrics: on-chain data from Helius (tx_count, wallet_age_days, unique_programs)
      - risk: internal risk flags from analysis_engine
      - scam_interactions, rugpull_interactions, in_scam_cluster: internal scam signals
      - nft_scam_role, wallet_type, nft_scam: NFT-specific signals
      - daemon: DaemonResult from Daemon Protocol (optional, degrades gracefully if None)

    CRITICAL override logic:
      If daemon.is_critical OR daemon.is_sanctioned → force risk_label = "HIGH"
      regardless of computed score.

    Returns (score: int, risk_label: str, reason_codes: list[str])
    """
    score = BASE_SCORE

    # --- On-chain bonuses (from Helius transaction data) ---
    tx_count = int(metrics.get("tx_count") or 0)
    score += min(tx_count // TX_DIVISOR, TX_BONUS_CAP)

    wallet_age_days = int(metrics.get("wallet_age_days") or 0)
    score += min(wallet_age_days // AGE_MONTH_DIVISOR, AGE_BONUS_CAP)

    unique_programs = int(metrics.get("unique_programs") or 0)
    score += min(unique_programs * PROGRAMS_MULTIPLIER, PROGRAMS_BONUS_CAP)

    # --- Internal flag penalties ---
    flags = risk.get("flags") or []
    score -= len(flags) * PENALTY_PER_FLAG

    # --- Internal scam penalties + risk_label from internal signals ---
    force_high = False
    if scam_interactions > 0:
        score -= SCAM_PENALTY
        force_high = True
    elif nft_scam_role == ROLE_SCAMMER:
        score -= NFT_SCAM_SCAMMER_PENALTY
        force_high = True
    elif in_scam_cluster:
        score -= SCAM_CLUSTER_PENALTY
        force_high = True

    if rugpull_interactions > 0:
        score -= RUGPULL_TOKEN_PENALTY

    # --- Clamp score (before Daemon modifier) ---
    # Apply absolute hard cap — never reach 100
    score = max(0, min(SCORE_ABSOLUTE_MAX, score))

    # --- Determine risk_label from internal signals ---
    if force_high:
        internal_risk = "HIGH"
    elif score >= THRESHOLD_MEDIUM:
        internal_risk = "LOW"
    elif score >= THRESHOLD_HIGH:
        internal_risk = "MEDIUM"
    else:
        internal_risk = "HIGH"

    # --- Apply Daemon hierarchy via resolve_final_risk ---
    score, risk_label = resolve_final_risk(
        score, internal_risk, daemon, wallet=(metrics.get("wallet") or ""),
    )

    reason_codes = _build_reason_codes(
        risk,
        scam_interactions,
        rugpull_interactions,
        in_scam_cluster,
        nft_scam_role,
        wallet_type,
        nft_scam,
        daemon=daemon,
    )

    logger.debug(
        "trust_engine_result",
        wallet=(metrics.get("wallet") or "")[:16] + "...",
        score=score,
        risk_label=risk_label,
        reason_codes=reason_codes,
        daemon_applied=daemon is not None,
    )

    return (score, risk_label, reason_codes)
