"""
This module provides static rule-based recommended actions for Explorer UI.
It does not modify scoring logic.
"""

from __future__ import annotations


def generate_recommended_actions(
    category: str | None,
    intrinsic_risk_level: str | None,
    contextual_risk_level: str | None,
) -> list[str]:
    """
    Return 3–4 recommended action strings based on category and risk levels.
    Fully static rule-based. Never returns empty list.
    """
    cat = (category or "").strip().upper()
    intrinsic = (intrinsic_risk_level or "").strip().upper()
    contextual = (contextual_risk_level or "").strip().upper()

    if cat == "SELF_RISK":
        return [
            "Review recent counterparties for suspicious activity.",
            "Avoid interacting with flagged or high-risk entities.",
            "Disconnect unknown token approvals and monitor wallet permissions.",
            "Reassess cluster-related transaction patterns.",
        ]

    if cat == "NETWORK_EXPOSED":
        return [
            "Reduce exposure to high-risk counterparties.",
            "Diversify transaction activity across trusted protocols.",
            "Monitor network risk signals and cluster changes.",
        ]

    if cat == "CLEAN":
        return [
            "Maintain consistent and healthy transaction behavior.",
            "Avoid interaction with newly flagged high-risk wallets.",
            "Continue long-term on-chain activity to strengthen reputation.",
        ]

    if intrinsic == "HIGH":
        return [
            "Review recent wallet activity carefully.",
            "Avoid high-risk token or contract interactions.",
            "Monitor on-chain exposure trends.",
        ]

    if contextual == "HIGH":
        return [
            "Monitor exposure to risky counterparties.",
            "Limit interaction with flagged wallet clusters.",
            "Diversify network relationships.",
        ]

    return [
        "Monitor wallet activity regularly.",
        "Maintain interaction with trusted protocols.",
        "Review exposure before large transactions.",
    ]
