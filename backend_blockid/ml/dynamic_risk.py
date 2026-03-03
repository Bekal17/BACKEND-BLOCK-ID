"""
Dynamic risk penalty computation for wallet scoring.
"""
from backend_blockid.ai_engine.dynamic_risk_engine import should_run_dynamic_risk
from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.time_weighted_risk import time_weighted_penalty
from backend_blockid.ml.cluster_risk import cluster_risk_penalty
from backend_blockid.ml.cluster_confidence import compute_cluster_confidence

logger = get_logger(__name__)


def compute_dynamic_penalty(
    reasons: list[dict],
    wallet: str = "",
    cluster_size: int | float = 0,
    flow_amount: int | float = 0,
    tx_count: int | float = 0,
) -> tuple[int, list[dict]]:
    """
    Compute penalty from reasons using time-weighted dynamic risk.
    Returns (penalty, reasons).
    """
    print("======================================")
    print("DYNAMIC_RISK DEBUG START")
    print("INPUT REASONS:", reasons)

    if not should_run_dynamic_risk(reasons) or not any(r.get("weight", 0) < 0 for r in reasons):
        logger.info("dynamic_risk_debug", wallet=wallet, reasons=reasons, total_penalty=0)
        print("FINAL PENALTY: 0 (no scam triggers or negative weights)")
        print("DYNAMIC_RISK DEBUG END")
        print("======================================")
        return 0, reasons

    total_penalty = 0
    # Precompute simple cluster-level features from inputs and reasons
    cluster_total = int(cluster_size or 0)
    cluster_scam_count = 1 if any(r.get("code") == "SCAM_CLUSTER_MEMBER" for r in reasons) else 0
    has_drainer = any(r.get("code") == "DRAINER_INTERACTION" for r in reasons)
    cluster_drainer_ratio = 1.0 if has_drainer else 0.0
    cluster_avg_flow = float(flow_amount or 0)

    for r in reasons:
        code = r.get("code")
        weight = r.get("weight", 0) or 0
        reason_conf = r.get("confidence")
        days_old = r.get("days_old")

        adjusted_weight = time_weighted_penalty(weight, days_old or 0)
        total_penalty += adjusted_weight

        if code == "SCAM_CLUSTER_MEMBER":
            distance = r.get("graph_distance", 1)
            cluster_conf = compute_cluster_confidence(
                scam_wallets=cluster_scam_count,
                total_wallets=cluster_total or 1,
                shared_drainer_ratio=cluster_drainer_ratio,
                avg_flow_amount=cluster_avg_flow,
            )
            extra = cluster_risk_penalty(
                scam_penalty=-100,
                distance=distance,
                days_old=days_old or 0,
                confidence=cluster_conf,
            )
            total_penalty += extra

        print(
            "  Reason:",
            "code=", code,
            "weight=", weight,
            "confidence=", reason_conf,
            "days_old=", days_old,
        )
        print("TIME-WEIGHTED RESULT:", adjusted_weight)

    print("TOTAL PENALTY BEFORE CLAMP:", total_penalty)

    if total_penalty < -100:
        total_penalty = -100
    if total_penalty > 100:
        total_penalty = 100

    print("FINAL PENALTY:", total_penalty)
    print("DYNAMIC_RISK DEBUG END")
    print("======================================")

    logger.info(
        "dynamic_risk_debug",
        wallet=wallet,
        reasons=reasons,
        total_penalty=int(total_penalty),
    )

    return int(total_penalty), reasons
