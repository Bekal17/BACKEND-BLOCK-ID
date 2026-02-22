# backend_blockid/ml/reason_codes.py

"""
Central reason code registry for BlockID.
All modules must import from here.
"""

REASON_WEIGHTS = {
    # === HIGH RISK ===
    "BLACKLISTED_CREATOR": -90,
    "RUG_PULL_DEPLOYER": -80,
    "DRAINER_FLOW_DETECTED": -70,
    "SCAM_CLUSTER_MEMBER": -60,

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
    "CLEAN_HISTORY": 10,
}