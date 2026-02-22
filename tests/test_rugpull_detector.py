"""
Tests for rugpull token detection (rugpull_detector.detect_rugpull_tokens).

Uses mocked RPC: get_token_accounts_by_owner with dict-style token account
payloads containing mint addresses; compares against scam_tokens blacklist.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend_blockid.analytics.rugpull_detector import (
    DEFAULT_BLACKLIST_PATH,
    _get_mints_from_token_accounts,
    _load_scam_tokens,
    detect_rugpull_tokens,
)

RUGPULL_MINT_1 = "RugpullToken11111111111111111111111111111"
RUGPULL_MINT_2 = "ScamToken22222222222222222222222222222222"
LEGIT_MINT = "So11111111111111111111111111111111111111112"
VALID_PUBKEY = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"


def _token_account(mint: str) -> dict:
    """Build token account entry as returned by get_token_accounts_by_owner (jsonParsed)."""
    return {
        "account": {
            "data": {
                "parsed": {
                    "info": {
                        "mint": mint,
                        "owner": VALID_PUBKEY,
                        "tokenAmount": {"amount": "100", "decimals": 6},
                    }
                }
            }
        }
    }


def test_load_scam_tokens_default_path():
    """Default path points to oracle/scam_tokens.json."""
    assert "oracle" in str(DEFAULT_BLACKLIST_PATH)
    assert DEFAULT_BLACKLIST_PATH.name == "scam_tokens.json"


def test_load_scam_tokens_from_file(tmp_path):
    """Blacklist loads from JSON array of mint addresses."""
    path = tmp_path / "scam_tokens.json"
    path.write_text(json.dumps([RUGPULL_MINT_1, RUGPULL_MINT_2]), encoding="utf-8")
    with patch.dict("os.environ", {"SCAM_TOKENS_PATH": str(path)}):
        loaded = _load_scam_tokens()
    assert loaded == {RUGPULL_MINT_1, RUGPULL_MINT_2}


def test_load_scam_tokens_missing_file():
    """Missing file returns empty set."""
    with patch.dict("os.environ", {"SCAM_TOKENS_PATH": "/nonexistent/scam_tokens.json"}):
        with patch("pathlib.Path.is_file", return_value=False):
            loaded = _load_scam_tokens()
    assert loaded == set()


def test_get_mints_from_token_accounts():
    """_get_mints_from_token_accounts extracts mint from parsed info."""
    value = [_token_account(RUGPULL_MINT_1), _token_account(LEGIT_MINT)]
    mints = _get_mints_from_token_accounts(MagicMock(value=value))
    assert RUGPULL_MINT_1 in mints
    assert LEGIT_MINT in mints
    assert len(mints) == 2


def test_get_mints_from_token_accounts_none():
    """_get_mints_from_token_accounts returns empty list for None value."""
    assert _get_mints_from_token_accounts(None) == []
    assert _get_mints_from_token_accounts(MagicMock(value=None)) == []


def test_detect_rugpull_empty_wallet():
    """No token accounts returns 0 interactions and empty list."""
    with patch(
        "backend_blockid.analytics.rugpull_detector._load_scam_tokens",
        return_value={RUGPULL_MINT_1, RUGPULL_MINT_2},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            mock_client.return_value.get_token_accounts_by_owner.return_value = MagicMock(value=[])
            out = detect_rugpull_tokens(VALID_PUBKEY)
    assert out["rugpull_interactions"] == 0
    assert out["rugpull_tokens"] == []


def test_detect_rugpull_one_match():
    """One token account with rugpull mint -> rugpull_interactions=1, rugpull_tokens=[mint]."""
    value = [_token_account(RUGPULL_MINT_1), _token_account(LEGIT_MINT)]
    with patch(
        "backend_blockid.analytics.rugpull_detector._load_scam_tokens",
        return_value={RUGPULL_MINT_1, RUGPULL_MINT_2},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            mock_client.return_value.get_token_accounts_by_owner.return_value = MagicMock(value=value)
            out = detect_rugpull_tokens(VALID_PUBKEY)
    assert out["rugpull_interactions"] == 1
    assert out["rugpull_tokens"] == [RUGPULL_MINT_1]


def test_detect_rugpull_three_interactions():
    """Three token accounts holding rugpull mints -> rugpull_interactions=3, rugpull_tokens sorted."""
    value = [
        _token_account(RUGPULL_MINT_1),
        _token_account(RUGPULL_MINT_2),
        _token_account(RUGPULL_MINT_1),
    ]
    with patch(
        "backend_blockid.analytics.rugpull_detector._load_scam_tokens",
        return_value={RUGPULL_MINT_1, RUGPULL_MINT_2},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            mock_client.return_value.get_token_accounts_by_owner.return_value = MagicMock(value=value)
            out = detect_rugpull_tokens(VALID_PUBKEY)
    assert out["rugpull_interactions"] == 3
    assert set(out["rugpull_tokens"]) == {RUGPULL_MINT_1, RUGPULL_MINT_2}
    assert out["rugpull_tokens"] == sorted(out["rugpull_tokens"])


def test_detect_rugpull_blacklist_empty():
    """Empty blacklist -> no matches, return 0 and []."""
    with patch("backend_blockid.analytics.rugpull_detector._load_scam_tokens", return_value=set()):
        out = detect_rugpull_tokens(VALID_PUBKEY)
    assert out["rugpull_interactions"] == 0
    assert out["rugpull_tokens"] == []


def test_detect_rugpull_invalid_wallet():
    """Invalid pubkey returns 0 and []."""
    with patch(
        "backend_blockid.analytics.rugpull_detector._load_scam_tokens",
        return_value={RUGPULL_MINT_1},
    ):
        out = detect_rugpull_tokens("not-a-pubkey")
    assert out["rugpull_interactions"] == 0
    assert out["rugpull_tokens"] == []


def test_detect_rugpull_empty_string_wallet():
    """Empty wallet string returns 0 and []."""
    out = detect_rugpull_tokens("")
    assert out["rugpull_interactions"] == 0
    assert out["rugpull_tokens"] == []
