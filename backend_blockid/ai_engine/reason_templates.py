"""
Template-based reason explanations for BlockID.

Rule-based, deterministic. No heavy ML.
"""

REASON_TEMPLATES: dict[str, dict[str, str]] = {
    "en": {
        "SCAM_CLUSTER_MEMBER": "This wallet interacted with a cluster linked to scam wallets within the last 30 days.",
        "NEW_WALLET": "This wallet is newly created and has limited transaction history.",
        "LOW_ACTIVITY": "This wallet has very low activity, which increases uncertainty in trust evaluation.",
        "HIGH_VOLUME_TO_SCAM": "This wallet transferred significant funds to a wallet flagged as scam.",
        "NO_RISK_DETECTED": "No significant risk indicators were detected for this wallet.",
        "SCAM_DISTANCE": "This wallet is {distance} hops away from a scam wallet.",
        "DRAINER_TX": "This wallet exhibited drainer-like transaction patterns.",
        "RUG_PULL_DEPLOYER": "This wallet is associated with rug pull deployment activity.",
        "CLEAN_HISTORY": "No suspicious activity detected in transaction history.",
    },
    # Future: "id": {...}, "jp": {...}, "zh": {...}
}

DEFAULT_LANG = "en"


def get_template(code: str, lang: str = DEFAULT_LANG, **placeholders: str) -> str:
    """Return template text for code. Fallback to en if lang not found."""
    templates = REASON_TEMPLATES.get(lang) or REASON_TEMPLATES[DEFAULT_LANG]
    text = templates.get(code)
    if not text:
        return f"[{code}]"
    if placeholders:
        try:
            text = text.format(**placeholders)
        except KeyError:
            pass
    return text
