"""
Pytest tests for BlockID Step 3 Analytics (wallet_scanner, risk_engine, trust_engine, pipeline).

RPC and external calls are mocked so tests run without Solana RPC.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

VALID_WALLET = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"


# --- Risk engine ---


def test_risk_engine_rules():
    """Risk flags and level: new_wallet, low_activity, inactive, suspicious_distribution; 0->LOW, 1-2->MEDIUM, 3+->HIGH."""
    from backend_blockid.analytics.risk_engine import (
        FLAG_INACTIVE,
        FLAG_LOW_ACTIVITY,
        FLAG_NEW_WALLET,
        FLAG_SUSPICIOUS_DISTRIBUTION,
        RISK_HIGH,
        RISK_LOW,
        RISK_MEDIUM,
        calculate_risk,
    )

    # 0 flags -> LOW
    r = calculate_risk({"wallet_age_days": 30, "tx_count": 20, "unique_programs": 5, "token_accounts": 3})
    assert r["risk_level"] == RISK_LOW
    assert len(r["flags"]) == 0

    # new_wallet (< 3 days)
    r = calculate_risk({"wallet_age_days": 2, "tx_count": 20, "unique_programs": 5, "token_accounts": 3})
    assert FLAG_NEW_WALLET in r["flags"]
    assert r["risk_level"] == RISK_MEDIUM

    # low_activity (< 5 txs)
    r = calculate_risk({"wallet_age_days": 30, "tx_count": 3, "unique_programs": 5, "token_accounts": 3})
    assert FLAG_LOW_ACTIVITY in r["flags"]

    # inactive (0 programs)
    r = calculate_risk({"wallet_age_days": 30, "tx_count": 20, "unique_programs": 0, "token_accounts": 3})
    assert FLAG_INACTIVE in r["flags"]

    # suspicious_distribution (> 100 token accounts)
    r = calculate_risk({"wallet_age_days": 30, "tx_count": 20, "unique_programs": 5, "token_accounts": 150})
    assert FLAG_SUSPICIOUS_DISTRIBUTION in r["flags"]

    # 3+ flags -> HIGH
    r = calculate_risk({
        "wallet_age_days": 1,
        "tx_count": 2,
        "unique_programs": 0,
        "token_accounts": 200,
    })
    assert len(r["flags"]) >= 3
    assert r["risk_level"] == RISK_HIGH


# --- Trust engine ---


def test_trust_engine_score():
    """Trust score is clamped 0-100; formula uses tx_count, age, programs, and flag penalty."""
    from backend_blockid.analytics.trust_engine import calculate_trust

    metrics = {"wallet": "w", "tx_count": 10, "wallet_age_days": 14, "unique_programs": 2, "token_accounts": 1}
    risk = {"flags": [], "risk_level": "LOW"}
    score, label, reason_codes = calculate_trust(metrics, risk)
    assert 0 <= score <= 100
    assert label == "LOW"
    assert reason_codes == []
    # base 40 + min(10//10,30)=1 + min(14//30,20)=0 + min(2*2,20)=4 - 0 = 45
    assert score == 45

    risk2 = {"flags": ["new_wallet", "low_activity"], "risk_level": "MEDIUM"}
    score2, label2, codes2 = calculate_trust(metrics, risk2)
    assert label2 == "MEDIUM"
    assert score2 == 45 - 30  # 15
    assert "NEW_WALLET" in codes2 and "LOW_ACTIVITY" in codes2

    # Clamp: large penalty
    risk3 = {"flags": ["a", "b", "c", "d", "e"], "risk_level": "HIGH"}
    score3, _, _ = calculate_trust(metrics, risk3)
    assert score3 == 0


def test_trust_engine_scam_penalty():
    """If scam_interactions > 0, score is reduced by 40 and risk_label is HIGH."""
    from backend_blockid.analytics.trust_engine import calculate_trust

    metrics = {"wallet": "w", "tx_count": 10, "wallet_age_days": 14, "unique_programs": 2, "token_accounts": 1}
    risk = {"flags": [], "risk_level": "LOW"}
    score_no_scam, _, _ = calculate_trust(metrics, risk)
    score_scam, label_scam, codes_scam = calculate_trust(metrics, risk, scam_interactions=1)
    assert label_scam == "HIGH"
    assert score_scam == score_no_scam - 40
    assert "KNOWN_SCAM_PROGRAM" in codes_scam


def test_trust_engine_nft_scam_scammer_penalty():
    """If nft_scam_role is 'scammer', score is reduced and risk_label is HIGH; victim/participant not penalized."""
    from backend_blockid.analytics.trust_engine import calculate_trust

    metrics = {"wallet": "w", "tx_count": 10, "wallet_age_days": 14, "unique_programs": 2, "token_accounts": 1}
    risk = {"flags": [], "risk_level": "LOW"}
    score_base, _, _ = calculate_trust(metrics, risk)
    score_scammer, label_scammer, codes_scammer = calculate_trust(metrics, risk, nft_scam_role="scammer")
    assert label_scammer == "HIGH"
    assert score_scammer == score_base - 35
    assert "SCAM_NFT_CREATOR" in codes_scammer
    nft_scam_victim = {"received_scam_nft": 1, "minted_scam_nft": 0, "is_creator": False, "role": "victim"}
    score_victim, label_victim, codes_victim = calculate_trust(
        metrics, risk, nft_scam_role="victim", nft_scam=nft_scam_victim
    )
    assert label_victim == "LOW"
    assert score_victim == score_base
    assert "SCAM_NFT_RECEIVED" in codes_victim


def test_trust_engine_rugpull_penalty():
    """If rugpull_interactions > 0, score is reduced by 30 and RUG_PULL_TOKEN is in reason_codes."""
    from backend_blockid.analytics.trust_engine import (
        REASON_RUG_PULL_TOKEN,
        calculate_trust,
    )

    metrics = {"wallet": "w", "tx_count": 10, "wallet_age_days": 14, "unique_programs": 2, "token_accounts": 1}
    risk = {"flags": [], "risk_level": "LOW"}
    score_base, _, _ = calculate_trust(metrics, risk)
    score_rug, label_rug, codes_rug = calculate_trust(metrics, risk, rugpull_interactions=1)
    assert score_rug == score_base - 30
    assert REASON_RUG_PULL_TOKEN in codes_rug


def test_trust_engine_scam_cluster_penalty():
    """If in_scam_cluster is True, score is reduced by 50 and SCAM_CLUSTER is in reason_codes, risk HIGH."""
    from backend_blockid.analytics.trust_engine import (
        REASON_SCAM_CLUSTER,
        calculate_trust,
    )

    metrics = {"wallet": "w", "tx_count": 10, "wallet_age_days": 14, "unique_programs": 2, "token_accounts": 1}
    risk = {"flags": [], "risk_level": "LOW"}
    score_base, _, _ = calculate_trust(metrics, risk)
    score_cluster, label_cluster, codes_cluster = calculate_trust(metrics, risk, in_scam_cluster=True)
    assert score_cluster == max(0, score_base - 50)
    assert REASON_SCAM_CLUSTER in codes_cluster
    assert label_cluster == "HIGH"


def test_trust_engine_reason_codes():
    """Reason codes: NEW_WALLET, LOW_ACTIVITY, KNOWN_SCAM_PROGRAM, SCAM_NFT_*, SERVICE_WALLET, COLD_WALLET."""
    from backend_blockid.analytics.trust_engine import (
        REASON_COLD_WALLET,
        REASON_KNOWN_SCAM_PROGRAM,
        REASON_LOW_ACTIVITY,
        REASON_NEW_WALLET,
        REASON_SCAM_NFT_CREATOR,
        REASON_SCAM_NFT_RECEIVED,
        REASON_SERVICE_WALLET,
        calculate_trust,
    )

    metrics = {"wallet": "w", "tx_count": 10, "wallet_age_days": 14, "unique_programs": 2, "token_accounts": 1}
    risk = {"flags": [], "risk_level": "LOW"}
    _, _, codes = calculate_trust(metrics, risk)
    assert codes == []

    risk_new = {"flags": ["new_wallet"], "risk_level": "MEDIUM"}
    _, _, codes_new = calculate_trust(metrics, risk_new)
    assert REASON_NEW_WALLET in codes_new

    risk_low = {"flags": ["low_activity"], "risk_level": "MEDIUM"}
    _, _, codes_low = calculate_trust(metrics, risk_low)
    assert REASON_LOW_ACTIVITY in codes_low

    _, _, codes_scam = calculate_trust(metrics, risk, scam_interactions=1)
    assert REASON_KNOWN_SCAM_PROGRAM in codes_scam

    nft_received = {"received_scam_nft": 2, "is_creator": False, "role": "victim"}
    _, _, codes_nft = calculate_trust(metrics, risk, nft_scam_role="victim", nft_scam=nft_received)
    assert REASON_SCAM_NFT_RECEIVED in codes_nft

    nft_creator = {"received_scam_nft": 1, "is_creator": True, "role": "scammer"}
    _, _, codes_creator = calculate_trust(metrics, risk, nft_scam_role="scammer", nft_scam=nft_creator)
    assert REASON_SCAM_NFT_CREATOR in codes_creator

    _, _, codes_svc = calculate_trust(metrics, risk, wallet_type="service_wallet")
    assert REASON_SERVICE_WALLET in codes_svc

    _, _, codes_cold = calculate_trust(metrics, risk, wallet_type="cold_wallet")
    assert REASON_COLD_WALLET in codes_cold

    from backend_blockid.analytics.trust_engine import REASON_SCAM_CLUSTER
    _, _, codes_cluster = calculate_trust(metrics, risk, in_scam_cluster=True)
    assert REASON_SCAM_CLUSTER in codes_cluster


# --- Wallet scanner (mocked) ---


def test_wallet_scanner_mocked():
    """scan_wallet returns metrics dict; RPC calls are mocked."""
    from backend_blockid.analytics.wallet_scanner import scan_wallet

    mock_sigs = [
        {"signature": "sig1", "blockTime": 1700000000, "err": None},
        {"signature": "sig2", "blockTime": 1700001000, "err": None},
    ]
    mock_token_list = [
        {"account": {"data": {"parsed": {"info": {"mint": "Mint1"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "Mint2"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "Mint3"}}}}},
    ]

    with patch("solana.rpc.api.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client

        def get_sigs(*args, **kwargs):
            resp = MagicMock()
            resp.value = mock_sigs
            return resp

        def get_token(*args, **kwargs):
            resp = MagicMock()
            resp.value = mock_token_list
            return resp

        mock_client.get_signatures_for_address.side_effect = get_sigs
        mock_client.get_token_accounts_by_owner_json_parsed.side_effect = get_token
        mock_client.get_transaction.return_value = MagicMock(value=None)

        result = scan_wallet(VALID_WALLET)

    assert result["wallet"] == VALID_WALLET
    assert result["tx_count"] == 2
    assert result["wallet_age_days"] >= 0
    assert result["token_accounts"] == 3
    assert "unique_programs" in result


def test_wallet_age_calculation():
    """Wallet age uses OLDEST transaction blockTime; (now - oldest_ts) / 86400."""
    import time
    from backend_blockid.analytics.wallet_scanner import scan_wallet

    # Oldest tx 400 days ago
    now = int(time.time())
    oldest_ts = now - (400 * 86400)
    mock_sigs = [
        {"signature": "new", "blockTime": now - 100, "err": None},
        {"signature": "old", "blockTime": oldest_ts, "err": None},
    ]

    with patch("solana.rpc.api.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_signatures_for_address.return_value = MagicMock(value=mock_sigs)
        mock_client.get_token_accounts_by_owner_json_parsed.return_value = MagicMock(value=[])
        mock_client.get_transaction.return_value = MagicMock(value=None)

        result = scan_wallet(VALID_WALLET)

    assert result["wallet_age_days"] >= 395
    assert result["wallet_age_days"] <= 405
    assert result["tx_count"] == 2


def test_unique_program_count():
    """Unique programs parsed from message.instructions[].programId."""
    from backend_blockid.analytics.wallet_scanner import scan_wallet

    # Valid base58 signatures (64 bytes) so Signature.from_string does not raise
    valid_sig = "5VERv8NMvzbJMEkV8xnrLkEaWRtSz9CosKDYjCJjBRnbJLgp8uirBgmQpjKhoR4tjF3ZpRzrFmBV6UjKdiSZkQUW"
    mock_sigs = [
        {"signature": valid_sig, "blockTime": 1700000000, "err": None},
        {"signature": valid_sig, "blockTime": 1700001000, "err": None},
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

    assert result["unique_programs"] == 2
    assert result["tx_count"] == 2


def test_token_account_parsing():
    """Token account count uses safe dict parsing (account.data.parsed.info); no .mint attribute."""
    from backend_blockid.analytics.wallet_scanner import scan_wallet, _parse_token_accounts_safe

    # Direct helper test
    token_val = [
        {"account": {"data": {"parsed": {"info": {"mint": "M1"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "M2"}}}}},
    ]
    count, _ = _parse_token_accounts_safe(token_val, "w")
    assert count == 2

    # Full scan with dict-style token response
    mock_sigs = [{"signature": "s", "blockTime": 1700000000, "err": None}]
    token_list = [
        {"account": {"data": {"parsed": {"info": {"mint": "MintA"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "MintB"}}}}},
        {"account": {"data": {"parsed": {"info": {"mint": "MintC"}}}}},
    ]

    with patch("solana.rpc.api.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_client.get_signatures_for_address.return_value = MagicMock(value=mock_sigs)
        mock_client.get_token_accounts_by_owner_json_parsed.return_value = MagicMock(value=token_list)
        mock_client.get_transaction.return_value = MagicMock(value=None)

        result = scan_wallet(VALID_WALLET)

    assert result["token_accounts"] == 3


def test_trust_score_realistic():
    """New formula: base 40 + tx//10 + age//30 + programs*2 (capped) - flags*15; clamp 0-100."""
    from backend_blockid.analytics.trust_engine import calculate_trust

    # Strong wallet: 200 txs, 180 days, 8 programs, 0 flags
    metrics = {"tx_count": 200, "wallet_age_days": 180, "unique_programs": 8, "token_accounts": 5}
    risk = {"flags": [], "risk_level": "LOW"}
    score, label, _ = calculate_trust(metrics, risk)
    assert 0 <= score <= 100
    assert label == "LOW"
    # 40 + min(20,30)=20 + min(6,20)=6 + min(16,20)=16 - 0 = 82
    assert score == 82

    # Weak: 5 txs, 2 days, 0 programs, 2 flags
    metrics2 = {"tx_count": 5, "wallet_age_days": 2, "unique_programs": 0, "token_accounts": 1}
    risk2 = {"flags": ["new_wallet", "low_activity"], "risk_level": "MEDIUM"}
    score2, label2, _ = calculate_trust(metrics2, risk2)
    assert label2 == "MEDIUM"
    # 40 + 0 + 0 + 0 - 30 = 10
    assert score2 == 10


# --- Pipeline (mocked) ---


def test_pipeline_mocked():
    """run_wallet_analysis returns wallet, metrics, risk, score, risk_label; scan_wallet mocked."""
    from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis

    fake_metrics = {
        "wallet": VALID_WALLET,
        "tx_count": 25,
        "wallet_age_days": 60,
        "unique_programs": 8,
        "token_accounts": 4,
    }

    with patch("backend_blockid.analytics.analytics_pipeline.scan_wallet", return_value=fake_metrics):
        result = run_wallet_analysis(VALID_WALLET)

    assert result["wallet"] == VALID_WALLET
    assert result["metrics"] == fake_metrics
    assert "risk" in result
    assert result["risk"]["flags"] is not None
    assert result["risk"]["risk_level"] in ("LOW", "MEDIUM", "HIGH")
    assert "score" in result
    assert 0 <= result["score"] <= 100
    assert result["risk_label"] in ("LOW", "MEDIUM", "HIGH")
    assert "reason_codes" in result
    assert isinstance(result["reason_codes"], list)