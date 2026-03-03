"""
BlockID Conditional Dynamic Risk Engine.

Dynamic risk calculation runs ONLY for wallets with scam/drainer signals.
Clean or normal wallets skip dynamic risk.
"""
from backend_blockid.ml.time_weighted_risk import apply_time_weighted_penalties

SCAM_TRIGGERS = {
    "SCAM_CLUSTER_MEMBER",
    "SCAM_CLUSTER_MEMBER_SMALL",
    "SCAM_CLUSTER_MEMBER_LARGE",
    "RUG_PULL_DEPLOYER",
    "DRAINER_FLOW_DETECTED",
    "DRAINER_FLOW",
    "MEGA_DRAINER",
    "BLACKLISTED_CREATOR",
    "HIGH_RISK_TOKEN_INTERACTION",
    "TRANSFER_TO_SCAM",
}


def should_run_dynamic_risk(reasons: list[dict]) -> bool:
    """Return True only if wallet has scam/drainer signals."""
    codes = {r.get("code") for r in reasons}
    return any(c in SCAM_TRIGGERS for c in codes)


def compute_dynamic_risk(
    wallet: str,
    reasons: list[dict],
    cluster_size: int | float = 0,
    flow_amount: int | float = 0,
    tx_count: int | float = 0,
) -> tuple[int, str]:
    """
    Compute dynamic penalty (0-60) for wallets with scam triggers.
    Returns (penalty, reason_string).
    """
    if not should_run_dynamic_risk(reasons):
        return 0, "no_dynamic_risk"

    penalty = apply_time_weighted_penalties(reasons)
    # Penalty is negative (sum of negative weights); convert to positive amount to subtract
    penalty = max(0, -penalty)
    penalty = min(penalty, 60)

    return penalty, "dynamic_risk_applied"


def apply_dynamic_risk(
    wallet: str,
    reasons: list[dict],
    cluster_size: int | float = 0,
    flow_amount: int | float = 0,
    tx_count: int | float = 0,
) -> tuple[list[dict], int]:
    """
    Apply dynamic risk ONLY if wallet has scam reason (any negative weight).
    Returns (reasons, penalty).
    """
    if not any(r.get("weight", 0) < 0 for r in reasons):
        return reasons, 0
    penalty, _ = compute_dynamic_risk(wallet, reasons, cluster_size, flow_amount, tx_count)
    return reasons, penalty
