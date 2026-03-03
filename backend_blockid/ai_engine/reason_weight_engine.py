from typing import List, Dict

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)


def clamp_score(score: int) -> int:
    return max(0, min(100, int(score)))


def aggregate_score(
    base_score: int,
    reasons: List[Dict],
) -> int:
    """
    Combine base ML score with reason weights (explainable scoring model).
    Credit-score standard: cap positive bonuses to prevent manipulation.
    """
    positive_sum = sum(r.get("weight", 0) for r in reasons if r.get("weight", 0) > 0)
    negative_sum = sum(r.get("weight", 0) for r in reasons if r.get("weight", 0) < 0)

    # Credit-score standard: cap bonuses
    positive_sum = min(positive_sum, 40)

    return clamp_score(base_score + positive_sum + negative_sum)


def explain_score(base_score: int, reasons: List[Dict]) -> Dict:
    return {
        "base_score": base_score,
        "reason_weights": [(r["code"], r.get("weight", 0)) for r in reasons],
        "final_score": aggregate_score(base_score, reasons),
    }


def finalize_reasons(base_score: int, reasons: list[dict]) -> dict:
    summary_parts = []

    for r in reasons:
        code = r.get("code")

        if code == "SCAM_CLUSTER_MEMBER":
            summary_parts.append("Wallet connected to known scam cluster")
        elif code == "DRAINER_TX":
            summary_parts.append("Wallet interacted with drainer address")
        elif code == "TRANSFER_TO_SCAM":
            summary_parts.append("Wallet transferred funds to scam wallet")
        elif code == "CLEAN_HISTORY":
            summary_parts.append("No suspicious transaction history")
        elif code == "LOW_ACTIVITY":
            summary_parts.append("Wallet has low transaction history")
        elif code:
            summary_parts.append(code.replace("_", " ").title())

    if not summary_parts:
        summary = "No significant risk signals detected"
    else:
        summary = ", ".join(summary_parts) + "."

    return {
        "summary": summary,
    }
