"""
BlockID Reason Summary Engine — human-readable explanation for trust score API.
"""


def build_summary(reasons: list, score: int) -> str:
    """Build human-readable summary from reason weights."""
    if not reasons:
        return "No suspicious activity detected."

    negatives = [r for r in reasons if r["weight"] < 0]
    positives = [r for r in reasons if r["weight"] > 0]

    if negatives:
        codes = ", ".join(r["code"] for r in negatives[:3])
        return f"Wallet flagged for: {codes}. Trust score reduced."

    if positives:
        codes = ", ".join(r["code"] for r in positives[:3])
        return f"Wallet shows positive signals: {codes}."

    return "No strong signals."
