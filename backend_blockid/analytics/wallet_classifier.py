"""
Wallet type classification for BlockID analytics.

Classifies wallets before risk scoring so cold/service wallets are not
incorrectly flagged as inactive or low-activity. Used by the analytics pipeline.
"""

from __future__ import annotations

from typing import Any

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

WALLET_TYPE_COLD = "cold_wallet"
WALLET_TYPE_SERVICE = "service_wallet"
WALLET_TYPE_TRADER = "trader_wallet"
WALLET_TYPE_NFT = "nft_wallet"
WALLET_TYPE_INACTIVE = "inactive_wallet"
WALLET_TYPE_UNKNOWN = "unknown"


def classify_wallet(metrics: dict[str, Any]) -> str:
    """
    Classify wallet from scanner metrics. Order of checks matters.

    Returns one of: cold_wallet, service_wallet, trader_wallet, nft_wallet,
    inactive_wallet, unknown. Handles None metrics by treating as 0.
    """
    tx_count = int(metrics.get("tx_count") or 0)
    wallet_age_days = int(metrics.get("wallet_age_days") or 0)
    unique_programs = int(metrics.get("unique_programs") or 0)
    token_accounts = int(metrics.get("token_accounts") or 0)

    # Many token accounts: likely NFT/collectibles holder
    if token_accounts > 20:
        return WALLET_TYPE_NFT

    # Old wallet, very few txs: cold storage
    if tx_count < 10 and wallet_age_days > 365:
        return WALLET_TYPE_COLD

    # High tx volume, few distinct programs: service/bot wallet
    if tx_count > 500 and unique_programs <= 5:
        return WALLET_TYPE_SERVICE

    # High activity, many programs: active trader
    if tx_count > 200 and unique_programs > 10:
        return WALLET_TYPE_TRADER

    # Clearly inactive: no txs or very few and young
    if tx_count < 3:
        return WALLET_TYPE_INACTIVE
    if tx_count < 5 and wallet_age_days < 90:
        return WALLET_TYPE_INACTIVE

    return WALLET_TYPE_UNKNOWN
