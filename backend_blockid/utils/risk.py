"""
BlockID risk level utilities — derive risk from trust score or reason severity.
"""

CRITICAL_CODES = {"RUG_PULL_DEPLOYER", "MEGA_DRAINER"}


def risk_level_from_reasons(reasons: list[dict]) -> str:
    """Risk level from reason severity, not score. Returns critical|high|medium|low."""
    codes = {r.get("code") for r in reasons}
    weights = [r.get("weight", 0) for r in reasons]

    if any(c in CRITICAL_CODES for c in codes):
        return "critical"
    if any(w < -50 for w in weights):
        return "high"
    if any(w < -20 for w in weights):
        return "medium"
    return "low"


def score_to_risk(score: int) -> str:
    """Map trust score (0-100) to risk level string."""
    if score <= 25:
        return "HIGH"
    if score <= 50:
        return "MEDIUM"
    if score <= 75:
        return "LOW"
    return "SAFE"
