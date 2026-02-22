"""
Pytest tests for wallet type classification and risk engine with wallet_type.
"""

from __future__ import annotations

import pytest

from backend_blockid.analytics.wallet_classifier import (
    WALLET_TYPE_COLD,
    WALLET_TYPE_INACTIVE,
    WALLET_TYPE_NFT,
    WALLET_TYPE_SERVICE,
    WALLET_TYPE_TRADER,
    WALLET_TYPE_UNKNOWN,
    classify_wallet,
)
from backend_blockid.analytics.risk_engine import (
    FLAG_INACTIVE,
    FLAG_LOW_ACTIVITY,
    FLAG_NEW_WALLET,
    FLAG_SUSPICIOUS_DISTRIBUTION,
    calculate_risk,
    RISK_LOW,
    RISK_MEDIUM,
)


def test_classify_cold_wallet():
    """Low tx count + old wallet -> cold_wallet."""
    assert classify_wallet({"tx_count": 5, "wallet_age_days": 400, "unique_programs": 2, "token_accounts": 0}) == WALLET_TYPE_COLD
    assert classify_wallet({"tx_count": 9, "wallet_age_days": 366, "unique_programs": 0, "token_accounts": 1}) == WALLET_TYPE_COLD


def test_classify_trader_wallet():
    """High tx + many programs -> trader_wallet."""
    assert classify_wallet({"tx_count": 300, "wallet_age_days": 100, "unique_programs": 15, "token_accounts": 5}) == WALLET_TYPE_TRADER
    assert classify_wallet({"tx_count": 201, "wallet_age_days": 30, "unique_programs": 11, "token_accounts": 0}) == WALLET_TYPE_TRADER


def test_classify_nft_wallet():
    """Many token accounts -> nft_wallet (checked before cold/trader)."""
    assert classify_wallet({"tx_count": 50, "wallet_age_days": 200, "unique_programs": 5, "token_accounts": 25}) == WALLET_TYPE_NFT
    assert classify_wallet({"tx_count": 5, "wallet_age_days": 400, "unique_programs": 0, "token_accounts": 21}) == WALLET_TYPE_NFT


def test_classify_service_wallet():
    """High tx + few programs -> service_wallet."""
    assert classify_wallet({"tx_count": 600, "wallet_age_days": 90, "unique_programs": 3, "token_accounts": 2}) == WALLET_TYPE_SERVICE
    assert classify_wallet({"tx_count": 501, "wallet_age_days": 10, "unique_programs": 5, "token_accounts": 0}) == WALLET_TYPE_SERVICE


def test_classify_inactive_wallet():
    """Very low tx or low tx + young -> inactive_wallet."""
    assert classify_wallet({"tx_count": 0, "wallet_age_days": 0, "unique_programs": 0, "token_accounts": 0}) == WALLET_TYPE_INACTIVE
    assert classify_wallet({"tx_count": 2, "wallet_age_days": 100, "unique_programs": 1, "token_accounts": 0}) == WALLET_TYPE_INACTIVE
    assert classify_wallet({"tx_count": 4, "wallet_age_days": 30, "unique_programs": 2, "token_accounts": 0}) == WALLET_TYPE_INACTIVE


def test_classify_unknown():
    """Does not match other rules -> unknown."""
    assert classify_wallet({"tx_count": 50, "wallet_age_days": 100, "unique_programs": 8, "token_accounts": 3}) == WALLET_TYPE_UNKNOWN


def test_risk_engine_cold_wallet_not_inactive():
    """Cold wallet: no low_activity or inactive flags; can still get new_wallet or suspicious_distribution."""
    metrics = {"tx_count": 5, "wallet_age_days": 400, "unique_programs": 0, "token_accounts": 0}
    risk = calculate_risk(metrics, wallet_type=WALLET_TYPE_COLD)
    assert FLAG_LOW_ACTIVITY not in risk["flags"]
    assert FLAG_INACTIVE not in risk["flags"]
    assert risk["risk_level"] == RISK_LOW


def test_risk_engine_service_wallet_not_inactive():
    """Service wallet: no low_activity or inactive flags."""
    metrics = {"tx_count": 600, "wallet_age_days": 90, "unique_programs": 3, "token_accounts": 0}
    risk = calculate_risk(metrics, wallet_type=WALLET_TYPE_SERVICE)
    assert FLAG_LOW_ACTIVITY not in risk["flags"]
    assert FLAG_INACTIVE not in risk["flags"]
    assert risk["risk_level"] == RISK_LOW


def test_risk_engine_cold_wallet_new_wallet_still_flagged():
    """Cold wallet that is also new: new_wallet flag still applied."""
    metrics = {"tx_count": 2, "wallet_age_days": 2, "unique_programs": 0, "token_accounts": 0}
    risk = calculate_risk(metrics, wallet_type=WALLET_TYPE_COLD)
    assert FLAG_NEW_WALLET in risk["flags"]


def test_risk_engine_cold_wallet_suspicious_distribution():
    """Cold wallet with many token accounts: suspicious_distribution still applied."""
    metrics = {"tx_count": 5, "wallet_age_days": 400, "unique_programs": 0, "token_accounts": 150}
    risk = calculate_risk(metrics, wallet_type=WALLET_TYPE_COLD)
    assert FLAG_SUSPICIOUS_DISTRIBUTION in risk["flags"]
    assert risk["risk_level"] in (RISK_LOW, RISK_MEDIUM)


def test_risk_engine_normal_wallet_full_rules():
    """Non-protected wallet: full rules (low_activity, inactive apply)."""
    metrics = {"tx_count": 2, "wallet_age_days": 30, "unique_programs": 0, "token_accounts": 0}
    risk = calculate_risk(metrics, wallet_type=WALLET_TYPE_INACTIVE)
    assert FLAG_LOW_ACTIVITY in risk["flags"]
    assert FLAG_INACTIVE in risk["flags"]
