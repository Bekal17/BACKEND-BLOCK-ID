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
    """
    total_weight = 0
    seen: set[str] = set()
    for r in reasons:
        code = r.get("code")
        if code and code in seen:
            continue
        if code:
            seen.add(code)
        if "weight" not in r or r.get("weight") is None:
            logger.warning("reason_missing_weight", reason_code=code)
            weight = 0
        else:
            weight = r.get("weight", 0)
        if weight:
            total_weight += weight

    final_score = base_score + total_weight
    return clamp_score(final_score)


def explain_score(base_score: int, reasons: List[Dict]) -> Dict:
    return {
        "base_score": base_score,
        "reason_weights": [(r["code"], r.get("weight", 0)) for r in reasons],
        "final_score": aggregate_score(base_score, reasons),
    }
