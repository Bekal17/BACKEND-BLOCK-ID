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
        "NO_SCAM_HISTORY": "Wallet has no known scam history.",
        "LOW_RISK_CLUSTER": "Wallet is not connected to known scam clusters.",
        "FAR_FROM_SCAM_CLUSTER": "Wallet is far from known scam clusters.",
        "LONG_HISTORY": "Wallet has long transaction history.",
        "LONG_TERM_ACTIVE": "Wallet has been active for over a year.",
        "MULTI_YEAR_ACTIVITY": "Wallet active across multiple years.",
        "AGE_1Y": "Wallet age at least 1 year.",
        "AGE_3Y": "Wallet age at least 3 years.",
        "AGE_5Y": "Wallet age at least 5 years.",
        "AGE_7Y": "Wallet age at least 7 years.",
        "AGE_10Y": "Wallet age at least 10 years.",
        "NFT_COLLECTOR": "Wallet holds NFT collections.",
        "NFT_10_PLUS": "Wallet holds 10+ NFTs.",
        "NFT_50_PLUS": "Wallet holds 50+ NFTs.",
        "NFT_100_PLUS": "Wallet holds 100+ NFTs.",
        "NFT_200_PLUS": "Wallet holds 200+ NFTs.",
        "NFT_500_PLUS": "Wallet holds 500+ NFTs.",
        "DEX_TRADER": "Wallet participates in DEX trading.",
        "DEX_TRADER_10_PLUS": "Wallet has 10+ DEX transactions.",
        "DEX_TRADER_50_PLUS": "Wallet has 50+ DEX transactions.",
        "DEX_TRADER_100_PLUS": "Wallet has 100+ DEX transactions.",
        "DEX_TRADER_200_PLUS": "Wallet has 200+ DEX transactions.",
        "DEX_TRADER_500_PLUS": "Wallet has 500+ DEX transactions.",
        "WHALE_100_SOL": "Wallet has held 100+ SOL.",
        "WHALE_1K_SOL": "Wallet has held 1K+ SOL.",
        "WHALE_5K_SOL": "Wallet has held 5K+ SOL.",
        "WHALE_10K_SOL": "Wallet has held 10K+ SOL.",
        "WHALE_50K_SOL": "Wallet has held 50K+ SOL.",
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
