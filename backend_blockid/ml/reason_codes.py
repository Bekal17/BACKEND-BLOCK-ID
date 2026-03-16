# backend_blockid/ml/reason_codes.py

"""
Central reason code registry for BlockID.
All modules must import from here.

SCORE SIMULATION EXAMPLES:
#
# Average wallet:
#   Base 40 + on-chain +15 + own_reasons +10 = 65 → MEDIUM
#
# Good wallet (no linking):
#   Base 40 + on-chain +28 + own_reasons +18 = 86 → SAFE
#   (like our test wallet 9hXa2... score 86.4)
#
# Good wallet + 1 clean linked wallet (confidence 0.85):
#   Base 86 + linking_boost +12 = 98 → capped at 97 ✅
#
# Good wallet + HIGH RISK linked wallet (confidence 0.80):
#   Base 86 + linking_penalty -16 = 70 → MEDIUM ⚠️
#
# Perfect scenario (all signals max):
#   Base 40 + on-chain +30 + own_reasons +20 + linking +15 = 105
#   → Hard capped at 97 ✅ (never 100)
"""
from pathlib import Path

_OPTIMIZED_WEIGHTS_PATH = Path(__file__).resolve().parent.parent / "models" / "reason_weights_optimized.csv"

# Default reason weights. Positive = trust boost, negative = penalty.
# Positive own-wallet total capped at +20; linking signals capped at +15.
REASON_WEIGHTS: dict[str, int] = {
    # === CRITICAL / MEGA RISK (unchanged) ===
    "MEGA_DRAINER": -95,
    "RUG_PULL_DEPLOYER": -80,
    "DRAINER_FLOW": -60,
    # === MULTI-LEVEL SCAM CLUSTER (unchanged) ===
    "SCAM_CLUSTER_MEMBER_SMALL": -20,
    "SCAM_CLUSTER_MEMBER_LARGE": -40,
    # === HIGH RISK (unchanged) ===
    "BLACKLISTED_CREATOR": -45,
    "DRAINER_FLOW_DETECTED": -35,
    "SCAM_CLUSTER_MEMBER": -30,
    # === MEDIUM RISK (unchanged) ===
    "HIGH_RISK_TOKEN_INTERACTION": -40,
    "SUSPICIOUS_TOKEN_MINT": -30,
    "DRAINER_INTERACTION": -20,
    "HIGH_VALUE_OUTFLOW": -10,
    "HIGH_RAPID_TX": -5,
    "NEAR_SCAM_CLUSTER": -8,
    # === LOW RISK (unchanged) ===
    "NEW_WALLET": -3,
    "LOW_ACTIVITY": -2,
    # === INFO (unchanged) ===
    "VICTIM_OF_SCAM": 0,
    "NO_RISK_DETECTED": 0,
    # === POSITIVE OWN WALLET (rebalanced — cap +20 total) ===
    "AGE_1Y": 2,
    "AGE_3Y": 4,
    "AGE_5Y": 6,
    "AGE_7Y": 7,
    "AGE_10Y": 8,
    "LONG_HISTORY": 3,
    "LONG_TERM_ACTIVE": 3,
    "MULTI_YEAR_ACTIVITY": 4,
    "NORMAL_ACTIVITY_PATTERN": 2,
    "CLEAN_HISTORY": 4,
    "NO_SCAM_HISTORY": 4,
    "LOW_RISK_CLUSTER": 3,
    "FAR_FROM_SCAM_CLUSTER": 2,
    "DEX_TRADER": 1,
    "DEX_TRADER_10_PLUS": 1,
    "DEX_TRADER_50_PLUS": 2,
    "DEX_TRADER_100_PLUS": 3,
    "DEX_TRADER_200_PLUS": 3,
    "DEX_TRADER_500_PLUS": 4,
    "DEX_HIGH_VOLUME": 2,
    "DEX_LONG_TERM_ACTIVITY": 2,
    "NFT_COLLECTOR": 1,
    "NFT_10_PLUS": 1,
    "NFT_50_PLUS": 2,
    "NFT_100_PLUS": 3,
    "NFT_200_PLUS": 3,
    "NFT_500_PLUS": 4,
    "NFT_VERIFIED_COLLECTION": 2,
    "NFT_TRADER_ACTIVE": 2,
    "WHALE_100_SOL": 1,
    "WHALE_1K_SOL": 2,
    "WHALE_5K_SOL": 3,
    "WHALE_10K_SOL": 3,
    "WHALE_50K_SOL": 4,
    "LONG_TERM_BALANCE": 2,
    "HIGH_BALANCE_HISTORY": 2,
    "DAO_MEMBER": 2,
    # === LINKING SIGNALS (cap +15 total) ===
    "VERIFIED_WALLET_LINK": 10,
    "MULTI_WALLET_IDENTITY": 7,
    "LINKED_WHALE": 5,
    "LINKED_LONG_HISTORY": 5,
    "LINKED_CLEAN_HISTORY": 8,
    "LINKED_HIGH_RISK": -20,
    "LINKED_SCAM_HISTORY": -30,
    "LINKED_SANCTIONED": -40,
    # === SOCIAL ENDORSEMENT ===
    "SOCIAL_ENDORSEMENT": 5,   # endorsed by trusted wallet
    "SOCIAL_MULTI_ENDORSE": 8, # endorsed by 3+ trusted wallets
    # === CONTENT VIOLATION PENALTIES ===
    "CONTENT_VIOLATION_MINOR": -2,
    "CONTENT_VIOLATION_MODERATE": -5,
    "CONTENT_VIOLATION_SEVERE": -15,
    "CONTENT_VIOLATION_CRITICAL": -50,
}


def get_reason_weights() -> dict[str, int]:
    """
    Return reason weights. Loads optimized weights from reason_weights_optimized.csv
    when present (merged over defaults). Used by reason_weight_engine integration.
    """
    out = dict(REASON_WEIGHTS)
    if _OPTIMIZED_WEIGHTS_PATH.exists():
        try:
            import csv
            with open(_OPTIMIZED_WEIGHTS_PATH, newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    code = (row.get("reason_code") or "").strip()
                    if code:
                        try:
                            out[code] = int(row.get("weight", 0))
                        except (TypeError, ValueError):
                            pass
        except Exception:
            pass
    return out
