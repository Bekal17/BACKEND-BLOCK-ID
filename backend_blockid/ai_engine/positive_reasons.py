"""
Positive reason codes for BlockID Trust Score.

These reasons explain why a wallet is considered safe or low-risk.
They are used by API layer first, then later integrated into ML pipeline.
"""

from typing import List, Dict


POSITIVE_REASON_CODES: List[Dict] = [
    {
        "code": "NO_RISK_DETECTED",
        "weight": 0,
        "description": "No suspicious activity detected."
    },
    {
        "code": "NO_SCAM_HISTORY",
        "weight": 5,
        "description": "Wallet has no known scam history."
    },
    {
        "code": "NORMAL_ACTIVITY_PATTERN",
        "weight": 5,
        "description": "Wallet transaction pattern looks normal."
    },
    {
        "code": "LOW_RISK_CLUSTER",
        "weight": 5,
        "description": "Wallet is not connected to known scam clusters."
    },
]


def default_positive_reason():
    """
    Return a safe default positive reason.
    """
    return {
        "code": "NO_RISK_DETECTED",
        "weight": 0,
        "confidence": 1.0,
        "tx_hash": None,
        "solscan": None,
    }
