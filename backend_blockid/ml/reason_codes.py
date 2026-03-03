# backend_blockid/ml/reason_codes.py

"""
Central reason code registry for BlockID.
All modules must import from here.
"""
from pathlib import Path

_OPTIMIZED_WEIGHTS_PATH = Path(__file__).resolve().parent.parent / "models" / "reason_weights_optimized.csv"

REASON_WEIGHTS = {
    # === CRITICAL / MEGA RISK ===
    "MEGA_DRAINER": -95,
    "RUG_PULL_DEPLOYER": -80,
    "DRAINER_FLOW": -60,

    # === MULTI-LEVEL SCAM CLUSTER ===
    "SCAM_CLUSTER_MEMBER_SMALL": -20,
    "SCAM_CLUSTER_MEMBER_LARGE": -40,

    # === HIGH RISK (legacy/compat) ===
    "BLACKLISTED_CREATOR": -45,
    "DRAINER_FLOW_DETECTED": -35,
    "SCAM_CLUSTER_MEMBER": -30,

    # === MEDIUM RISK ===
    "HIGH_RISK_TOKEN_INTERACTION": -40,
    "SUSPICIOUS_TOKEN_MINT": -30,
    "DRAINER_INTERACTION": -20,
    "HIGH_VALUE_OUTFLOW": -10,

    # === LOW RISK ===
    "NEW_WALLET": -5,
    "LOW_ACTIVITY": -3,

    # === INFO
    "VICTIM_OF_SCAM": 0,

    # === POSITIVE
    #max_positive_bonus = 40
    "CLEAN_HISTORY": 10,
    # === MVP POSITIVE REASONS ===
    "NO_SCAM_HISTORY": 10,
    "LOW_RISK_CLUSTER": 8,
    "FAR_FROM_SCAM_CLUSTER": 6,
    "LONG_HISTORY": 10,
    # === POSITIVE LEGIT SIGNALS ===
    "LONG_TERM_ACTIVE": 10,
    "NFT_COLLECTOR": 5,
    "DEX_TRADER": 5,
    "DAO_MEMBER": 5,
    "HIGH_BALANCE_HISTORY": 5,
    "MULTI_YEAR_ACTIVITY": 10,
    "WHALE_100_SOL": 3,
    "WHALE_1K_SOL": 5,
    "WHALE_5K_SOL": 8,
    "WHALE_10K_SOL": 10,
    "WHALE_50K_SOL": 12,
    "LONG_TERM_BALANCE": 5,
    "DEX_TRADER_10_PLUS": 3,
    "DEX_TRADER_50_PLUS": 5,
    "DEX_TRADER_100_PLUS": 8,
    "DEX_TRADER_200_PLUS": 10,
    "DEX_TRADER_500_PLUS": 12,
    "DEX_HIGH_VOLUME": 5,
    "DEX_LONG_TERM_ACTIVITY": 5,
    "NFT_10_PLUS": 3,
    "NFT_50_PLUS": 5,
    "NFT_100_PLUS": 8,
    "NFT_200_PLUS": 10,
    "NFT_500_PLUS": 12,
    "NFT_VERIFIED_COLLECTION": 5,
    "NFT_TRADER_ACTIVE": 5,
    "AGE_1Y": 5,
    "AGE_3Y": 10,
    "AGE_5Y": 15,
    "AGE_7Y": 18,
    "AGE_10Y": 20,
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