"""
Badge rules for BlockID reputation evolution.

Defines score ranges and badge levels for timeline and Phantom plugin.
Future: NFT-style reputation, soulbound tokens, DAO voting, cross-chain.
"""

BADGES = [
    ("SCAM_SUSPECTED", 0, 19),
    ("HIGH_RISK", 20, 39),
    ("CAUTION", 40, 59),
    ("TRUSTED", 60, 79),
    ("EXCELLENT", 80, 100),
]

# Badge name -> color for UI/Phantom overlay
BADGE_COLORS = {
    "SCAM_SUSPECTED": "#ef4444",
    "HIGH_RISK": "#f97316",
    "CAUTION": "#eab308",
    "TRUSTED": "#22c55e",
    "EXCELLENT": "#10b981",
}
