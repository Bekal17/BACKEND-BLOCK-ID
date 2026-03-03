"""
Badge rules for BlockID trust display.

Rule-based. Sorted by min_score descending for lookup.
"""

BADGE_RULES = [
    {"name": "Trusted", "min_score": 80, "color": "green"},
    {"name": "Caution", "min_score": 50, "color": "yellow"},
    {"name": "Risky", "min_score": 20, "color": "orange"},
    {"name": "Scam Suspected", "min_score": 0, "color": "red"},
]

BADGE_RULES_SORTED = sorted(BADGE_RULES, key=lambda r: r["min_score"], reverse=True)

RISK_LEVEL_TEXT = {
    "0": "Low Risk",
    "1": "Moderate Risk",
    "2": "High Risk",
    "3": "Critical Risk",
}


def get_badge_for_score(score: float) -> dict:
    """Return badge rule for given score. Highest matching min_score wins."""
    for rule in BADGE_RULES_SORTED:
        if score >= rule["min_score"]:
            return rule
    return BADGE_RULES_SORTED[-1]
