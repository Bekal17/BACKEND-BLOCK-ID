"""
Risk engine: derive risk flags and level from wallet metrics and type.

Cold and service wallets are not marked risky for low_activity or inactive.
Only new_wallet, (scam program), and suspicious_distribution apply to them.
Other types use full rules. Risk level: 0 flags -> LOW, 1-2 -> MEDIUM, 3+ -> HIGH.
"""

from __future__ import annotations

from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

FLAG_NEW_WALLET = "new_wallet"
FLAG_LOW_ACTIVITY = "low_activity"
FLAG_INACTIVE = "inactive"
FLAG_SUSPICIOUS_DISTRIBUTION = "suspicious_distribution"

RISK_LOW = "LOW"
RISK_MEDIUM = "MEDIUM"
RISK_HIGH = "HIGH"

# Wallet types we do not mark as risky for inactivity/low activity
PROTECTED_WALLET_TYPES = ("cold_wallet", "service_wallet")


def calculate_risk(metrics: dict[str, Any], wallet_type: str | None = None) -> dict[str, Any]:
    """
    Compute risk flags and level from scanner metrics and optional wallet type.

    For cold_wallet and service_wallet: only new_wallet and suspicious_distribution
    are applied (not low_activity, not inactive). For others, all flags apply.
    Expects metrics: wallet_age_days, tx_count, unique_programs, token_accounts.
    Returns: { "flags": [...], "risk_level": "LOW" | "MEDIUM" | "HIGH" }.
    """
    flags: list[str] = []

    wallet_age_days = int(metrics.get("wallet_age_days") or 0)
    tx_count = int(metrics.get("tx_count") or 0)
    unique_programs = int(metrics.get("unique_programs") or 0)
    token_accounts = int(metrics.get("token_accounts") or 0)

    is_protected = wallet_type in PROTECTED_WALLET_TYPES

    if wallet_age_days < 3:
        flags.append(FLAG_NEW_WALLET)
    if not is_protected:
        if tx_count < 5:
            flags.append(FLAG_LOW_ACTIVITY)
        if unique_programs == 0:
            flags.append(FLAG_INACTIVE)
    if token_accounts > 100:
        flags.append(FLAG_SUSPICIOUS_DISTRIBUTION)

    if len(flags) == 0:
        risk_level = RISK_LOW
    elif len(flags) <= 2:
        risk_level = RISK_MEDIUM
    else:
        risk_level = RISK_HIGH

    result = {"flags": flags, "risk_level": risk_level}
    logger.debug(
        "risk_engine_result",
        wallet=(metrics.get("wallet") or "")[:16] + "...",
        wallet_type=wallet_type,
        flags=flags,
        risk_level=risk_level,
    )
    return result
