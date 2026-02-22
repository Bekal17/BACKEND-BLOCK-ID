"""
Tests for scam program detection (scam_detector.detect_scam_interactions).

Uses mocked RPC: get_signatures_for_address and get_transaction with
dict-style tx payloads containing blacklisted program IDs.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend_blockid.analytics.scam_detector import (
    DEFAULT_BLACKLIST_PATH,
    _load_scam_blacklist,
    _program_ids_from_tx,
    detect_scam_interactions,
)

FAKE_SCAM_1 = "FakeMint111111111111111111111111111111111"
FAKE_SCAM_2 = "RugPull222222222222222222222222222222222"
VALID_PUBKEY = "So11111111111111111111111111111111111111112"


def _tx_value(program_ids: list[str], inner_program_ids: list[str] | None = None) -> dict:
    """Build tx.value-like dict (transaction.message.instructions + meta.inner)."""
    instructions = [{"programId": pid} for pid in program_ids]
    msg = {"instructions": instructions}
    tx_obj = {"message": msg}
    out = {"transaction": tx_obj}
    if inner_program_ids:
        out["meta"] = {
            "innerInstructions": [{"instructions": [{"programId": pid} for pid in inner_program_ids]}]
        }
    return out


def test_load_scam_blacklist_default_path():
    """Default path points to oracle/scam_programs.json."""
    assert "oracle" in str(DEFAULT_BLACKLIST_PATH)
    assert DEFAULT_BLACKLIST_PATH.name == "scam_programs.json"


def test_load_scam_blacklist_from_file(tmp_path):
    """Blacklist loads from JSON array."""
    path = tmp_path / "scam.json"
    path.write_text(json.dumps([FAKE_SCAM_1, FAKE_SCAM_2]), encoding="utf-8")
    with patch.dict("os.environ", {"SCAM_PROGRAMS_PATH": str(path)}):
        loaded = _load_scam_blacklist()
    assert loaded == {FAKE_SCAM_1, FAKE_SCAM_2}


def test_load_scam_blacklist_missing_file():
    """Missing file returns empty set."""
    with patch.dict("os.environ", {"SCAM_PROGRAMS_PATH": "/nonexistent/scam.json"}):
        with patch("pathlib.Path.is_file", return_value=False):
            loaded = _load_scam_blacklist()
    assert loaded == set()


def test_program_ids_from_tx_dict():
    """_program_ids_from_tx extracts programId from dict message.instructions."""
    tx_value = _tx_value([FAKE_SCAM_1, "SystemProgram11111111111111111111111111111111"])
    ids = _program_ids_from_tx(tx_value)
    assert FAKE_SCAM_1 in ids
    assert "SystemProgram11111111111111111111111111111111" in ids
    assert len(ids) == 2


def test_program_ids_from_tx_inner():
    """_program_ids_from_tx includes meta.innerInstructions programIds."""
    tx_value = _tx_value(["SystemProgram11111111111111111111111111111111"], inner_program_ids=[FAKE_SCAM_2])
    ids = _program_ids_from_tx(tx_value)
    assert FAKE_SCAM_2 in ids
    assert "SystemProgram11111111111111111111111111111111" in ids


def test_program_ids_from_tx_none():
    """_program_ids_from_tx returns empty set for None."""
    assert _program_ids_from_tx(None) == set()


def test_detect_scam_empty_wallet():
    """Empty wallet returns 0 interactions and empty list."""
    with patch(
        "backend_blockid.analytics.scam_detector._load_scam_blacklist",
        return_value={FAKE_SCAM_1, FAKE_SCAM_2},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            mock_client.return_value.get_signatures_for_address.return_value = MagicMock(value=[])
            out = detect_scam_interactions(VALID_PUBKEY)
    assert out["scam_interactions"] == 0
    assert out["scam_programs"] == []


def test_detect_scam_one_tx_one_scam():
    """One tx with one scam program -> scam_interactions=1, scam_programs=[that]."""
    tx_val = _tx_value([FAKE_SCAM_1, "SystemProgram11111111111111111111111111111111"])
    mock_sig = MagicMock(signature="sig1")
    mock_tx_resp = MagicMock()
    mock_tx_resp.value = tx_val
    with patch(
        "backend_blockid.analytics.scam_detector._load_scam_blacklist",
        return_value={FAKE_SCAM_1, FAKE_SCAM_2},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            with patch("solders.signature.Signature.from_string", return_value=MagicMock()):
                client = mock_client.return_value
                client.get_signatures_for_address.return_value = MagicMock(value=[mock_sig])
                client.get_transaction.return_value = mock_tx_resp
                out = detect_scam_interactions(VALID_PUBKEY)
    assert out["scam_interactions"] == 1
    assert out["scam_programs"] == [FAKE_SCAM_1]


def test_detect_scam_two_txs_two_scams():
    """Two txs with different scam programs -> scam_interactions=2, scam_programs sorted."""
    tx1 = _tx_value([FAKE_SCAM_1])
    tx2 = _tx_value([FAKE_SCAM_2])
    mock_sig1 = MagicMock(signature="s1")
    mock_sig2 = MagicMock(signature="s2")
    with patch(
        "backend_blockid.analytics.scam_detector._load_scam_blacklist",
        return_value={FAKE_SCAM_1, FAKE_SCAM_2},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            with patch("solders.signature.Signature.from_string", return_value=MagicMock()):
                client = mock_client.return_value
                client.get_signatures_for_address.return_value = MagicMock(value=[mock_sig1, mock_sig2])
                client.get_transaction.side_effect = [
                    MagicMock(value=tx1),
                    MagicMock(value=tx2),
                ]
                out = detect_scam_interactions(VALID_PUBKEY)
    assert out["scam_interactions"] == 2
    assert set(out["scam_programs"]) == {FAKE_SCAM_1, FAKE_SCAM_2}
    assert out["scam_programs"] == sorted(out["scam_programs"])


def test_detect_scam_blacklist_empty():
    """Empty blacklist -> no scan, return 0 and []."""
    with patch("backend_blockid.analytics.scam_detector._load_scam_blacklist", return_value=set()):
        out = detect_scam_interactions(VALID_PUBKEY)
    assert out["scam_interactions"] == 0
    assert out["scam_programs"] == []


def test_detect_scam_invalid_wallet():
    """Invalid pubkey string returns 0 and []."""
    with patch("backend_blockid.analytics.scam_detector._load_scam_blacklist", return_value={FAKE_SCAM_1}):
        out = detect_scam_interactions("not-a-pubkey")
    assert out["scam_interactions"] == 0
    assert out["scam_programs"] == []


def test_detect_scam_empty_string_wallet():
    """Empty wallet string returns 0 and []."""
    out = detect_scam_interactions("")
    assert out["scam_interactions"] == 0
    assert out["scam_programs"] == []


def test_detect_scam_tx_value_none_skipped():
    """Tx with get_transaction returning None value is skipped (no crash)."""
    mock_sig = MagicMock(signature="sig1")
    with patch(
        "backend_blockid.analytics.scam_detector._load_scam_blacklist",
        return_value={FAKE_SCAM_1},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            client = mock_client.return_value
            client.get_signatures_for_address.return_value = MagicMock(value=[mock_sig])
            client.get_transaction.return_value = MagicMock(value=None)
            out = detect_scam_interactions(VALID_PUBKEY)
    assert out["scam_interactions"] == 0
    assert out["scam_programs"] == []
