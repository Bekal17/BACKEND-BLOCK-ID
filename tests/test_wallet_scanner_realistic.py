"""
Realistic wallet_scanner tests with mocked RPC responses.

Verifies: wallet_age_days > 0 from oldest blockTime, unique_programs > 0 from
get_transaction instructions, token_accounts from safe dict parsing (account.data.parsed.info.mint).
"""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

VALID_WALLET = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"
VALID_SIG = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"


def test_wallet_scanner_realistic_wallet_age_gt_zero():
    """wallet_age_days > 0 when signatures include an old blockTime (OLDEST used)."""
    from backend_blockid.analytics.wallet_scanner import scan_wallet

    now = int(time.time())
    oldest_ts = now - (365 * 86400)
    mock_sigs = [
        {"signature": VALID_SIG, "blockTime": now - 100, "err": None},
        {"signature": VALID_SIG, "blockTime": oldest_ts, "err": None},
    ]

    with patch("solana.rpc.api.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_signatures_for_address.return_value = MagicMock(value=mock_sigs)
        mock_client.get_transaction.return_value = MagicMock(value=None)
        mock_client.get_token_accounts_by_owner_json_parsed.return_value = MagicMock(value=[])

        result = scan_wallet(VALID_WALLET)

    assert result["wallet_age_days"] is not None
    assert result["wallet_age_days"] > 0
    assert result["wallet_age_days"] >= 364
    assert result["tx_count"] == 2


def test_wallet_scanner_realistic_unique_programs_gt_zero():
    """unique_programs > 0 when get_transaction returns instructions with programId."""
    from backend_blockid.analytics.wallet_scanner import scan_wallet

    mock_sigs = [
        {"signature": VALID_SIG, "blockTime": 1700000000, "err": None},
    ]
    mock_tx = {
        "transaction": {
            "message": {
                "instructions": [
                    {"programId": "11111111111111111111111111111111"},
                    {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
                ],
            },
        },
    }

    with patch("solana.rpc.api.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_signatures_for_address.return_value = MagicMock(value=mock_sigs)
        mock_client.get_transaction.return_value = MagicMock(value=mock_tx)
        mock_client.get_token_accounts_by_owner_json_parsed.return_value = MagicMock(value=[])

        result = scan_wallet(VALID_WALLET)

    assert result["unique_programs"] is not None
    assert result["unique_programs"] > 0
    assert result["unique_programs"] == 2


def test_wallet_scanner_realistic_token_accounts_parsed_correctly():
    """token_accounts counted via safe dict parsing (account.data.parsed.info.mint)."""
    from backend_blockid.analytics.wallet_scanner import scan_wallet

    mock_sigs = [{"signature": VALID_SIG, "blockTime": 1700000000, "err": None}]
    token_list = [
        {"account": {"data": {"parsed": {"info": {"mint": "MintA"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "MintB"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "MintC"}}}}},
    ]

    with patch("solana.rpc.api.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_signatures_for_address.return_value = MagicMock(value=mock_sigs)
        mock_client.get_transaction.return_value = MagicMock(value=None)
        mock_client.get_token_accounts_by_owner_json_parsed.return_value = MagicMock(
            value=token_list
        )

        result = scan_wallet(VALID_WALLET)

    assert result["token_accounts"] is not None
    assert result["token_accounts"] == 3


def test_wallet_scanner_parse_token_accounts_safe_no_mint_attribute():
    """_parse_token_accounts_safe uses only dict.get(); never .mint (avoids AttributeError)."""
    from backend_blockid.analytics.wallet_scanner import _parse_token_accounts_safe

    # Dict-shaped entries as returned by solana-py
    token_accounts = [
        {"account": {"data": {"parsed": {"info": {"mint": "So11111111111111111111111111111111111111112"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"}}}}},
    ]
    count, failed = _parse_token_accounts_safe(token_accounts, "test")
    assert count == 2
    assert failed is False

    # Empty or missing info still counts as 0 for that item, no crash
    mixed = [
        {"account": {"data": {"parsed": {"info": {"mint": "M1"}}}}},
        {"account": {"data": {}}},
    ]
    count2, failed2 = _parse_token_accounts_safe(mixed, "test")
    assert count2 == 1
    assert failed2 is False
