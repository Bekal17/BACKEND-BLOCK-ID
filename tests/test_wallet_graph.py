"""
Tests for wallet graph / cluster detection (wallet_graph.detect_wallet_cluster).

Uses mocked RPC: get_signatures_for_address and get_transaction with
dict-style tx payloads containing message.accountKeys; scam_wallets blacklist mocked.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend_blockid.analytics.wallet_graph import (
    DEFAULT_SCAM_WALLETS_PATH,
    _account_keys_from_tx,
    _build_cluster_id,
    _load_scam_wallets,
    _neighbors_from_txs,
    detect_wallet_cluster,
)

SCAM_WALLET_1 = "ScamWallet11111111111111111111111111111111"
LEGIT_WALLET = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"
OTHER_WALLET = "So11111111111111111111111111111111111111112"
VALID_PUBKEY = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"


def _tx_value(account_keys: list[str]) -> dict:
    """Build tx.value-like dict with message.accountKeys."""
    return {
        "transaction": {
            "message": {
                "accountKeys": account_keys,
            }
        }
    }


def test_load_scam_wallets_default_path():
    """Default path points to oracle/scam_wallets.json."""
    assert "oracle" in str(DEFAULT_SCAM_WALLETS_PATH)
    assert DEFAULT_SCAM_WALLETS_PATH.name == "scam_wallets.json"


def test_load_scam_wallets_from_file(tmp_path):
    """Blacklist loads from JSON array."""
    path = tmp_path / "scam_wallets.json"
    path.write_text(json.dumps([SCAM_WALLET_1]), encoding="utf-8")
    with patch.dict("os.environ", {"SCAM_WALLETS_PATH": str(path)}):
        loaded = _load_scam_wallets()
    assert SCAM_WALLET_1 in loaded


def test_account_keys_from_tx():
    """_account_keys_from_tx extracts accountKeys (list of strings)."""
    tx = _tx_value([LEGIT_WALLET, OTHER_WALLET, "11111111111111111111111111111111"])
    keys = _account_keys_from_tx(tx)
    assert LEGIT_WALLET in keys
    assert OTHER_WALLET in keys
    assert "11111111111111111111111111111111" in keys


def test_account_keys_from_tx_dict_pubkey():
    """_account_keys_from_tx handles accountKeys as list of {pubkey: string}."""
    tx = {
        "transaction": {
            "message": {
                "accountKeys": [
                    {"pubkey": LEGIT_WALLET},
                    {"pubkey": OTHER_WALLET},
                ]
            }
        }
    }
    keys = _account_keys_from_tx(tx)
    assert LEGIT_WALLET in keys
    assert OTHER_WALLET in keys


def test_neighbors_from_txs_excludes_self_and_programs():
    """_neighbors_from_txs returns only counterparties (excludes wallet and known programs)."""
    txs = [
        _tx_value([LEGIT_WALLET, OTHER_WALLET, "11111111111111111111111111111111"]),
        _tx_value([LEGIT_WALLET, "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]),
    ]
    neighbors = _neighbors_from_txs(txs, LEGIT_WALLET)
    assert OTHER_WALLET in neighbors
    assert LEGIT_WALLET not in neighbors
    assert "11111111111111111111111111111111" not in neighbors


def test_build_cluster_id():
    """_build_cluster_id is deterministic and prefixed."""
    nodes = {LEGIT_WALLET, OTHER_WALLET}
    cid = _build_cluster_id(nodes)
    assert cid.startswith("cluster_")
    assert len(cid) == len("cluster_") + 12
    assert _build_cluster_id(nodes) == cid
    assert _build_cluster_id(set()) == "cluster_0"


def test_detect_wallet_cluster_empty_sigs():
    """No signatures returns cluster_size 1, cluster_risk LOW, cluster_id deterministic."""
    with patch("solana.rpc.api.Client") as mock_client:
        mock_client.return_value.get_signatures_for_address.return_value = MagicMock(value=[])
        out = detect_wallet_cluster(VALID_PUBKEY)
    assert out["cluster_size"] == 1
    assert out["cluster_risk"] == "LOW"
    assert out["cluster_id"].startswith("cluster_")


def test_detect_wallet_cluster_scam_counterparty():
    """If a counterparty is in scam_wallets, cluster_risk is HIGH."""
    tx_val = _tx_value([VALID_PUBKEY, SCAM_WALLET_1])
    with patch(
        "backend_blockid.analytics.wallet_graph._load_scam_wallets",
        return_value={SCAM_WALLET_1},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            with patch("solders.signature.Signature.from_string", return_value=MagicMock()):
                client = mock_client.return_value
                client.get_signatures_for_address.return_value = MagicMock(
                    value=[MagicMock(signature="sig1")]
                )
                client.get_transaction.return_value = MagicMock(value=tx_val)
                out = detect_wallet_cluster(VALID_PUBKEY)
    assert out["cluster_risk"] == "HIGH"
    assert out["cluster_size"] >= 2
    assert out["cluster_id"].startswith("cluster_")


def test_detect_wallet_cluster_no_scam():
    """Only legit counterparties -> cluster_risk LOW."""
    tx_val = _tx_value([VALID_PUBKEY, OTHER_WALLET])
    with patch(
        "backend_blockid.analytics.wallet_graph._load_scam_wallets",
        return_value={SCAM_WALLET_1},
    ):
        with patch("solana.rpc.api.Client") as mock_client:
            with patch("solders.signature.Signature.from_string", return_value=MagicMock()):
                client = mock_client.return_value
                client.get_signatures_for_address.return_value = MagicMock(
                    value=[MagicMock(signature="sig1")]
                )
                client.get_transaction.return_value = MagicMock(value=tx_val)
                out = detect_wallet_cluster(VALID_PUBKEY)
    assert out["cluster_risk"] == "LOW"
    assert out["cluster_size"] == 2


def test_detect_wallet_cluster_invalid_wallet():
    """Invalid pubkey returns cluster_0, size 1, LOW."""
    with patch(
        "backend_blockid.analytics.wallet_graph._load_scam_wallets",
        return_value=set(),
    ):
        out = detect_wallet_cluster("not-a-pubkey")
    assert out["cluster_id"] == "cluster_0"
    assert out["cluster_size"] == 1
    assert out["cluster_risk"] == "LOW"


def test_detect_wallet_cluster_empty_string():
    """Empty wallet string returns default."""
    out = detect_wallet_cluster("")
    assert out["cluster_id"] == "cluster_0"
    assert out["cluster_size"] == 1
    assert out["cluster_risk"] == "LOW"
