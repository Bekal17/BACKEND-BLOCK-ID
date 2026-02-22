"""
Tests for feature_builder normalization and invalid-wallet handling.
"""

from __future__ import annotations

import pytest

import numpy as np

from backend_blockid.ml.feature_builder import (
    FEATURE_NAMES,
    build_features,
    get_feature_names,
    is_valid_metrics,
)


def test_normal_wallet():
    """Normal wallet produces normalized features in 0–5 range."""
    analytics = {
        "wallet": "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka",
        "wallet_type": "trader_wallet",
        "metrics": {
            "wallet_age_days": 365,
            "tx_count": 500,
            "unique_programs": 10,
            "token_accounts": 4,
        },
        "scam": {"scam_interactions": 0},
        "rugpull": {"rugpull_interactions": 0},
        "wallet_cluster": {"cluster_size": 5},
        "nft_scam": {"received_scam_nft": 0},
    }
    x = build_features(analytics)
    assert isinstance(x, np.ndarray)
    assert x.dtype == np.float64
    assert x.ndim == 1
    assert x.shape[0] == len(FEATURE_NAMES)
    assert np.all(x >= 0) and np.all(x <= 5)
    # wallet_age 365/365 = 1, tx 500/1000 = 0.5, etc.
    assert x[0] == 1.0
    assert x[1] == 0.5
    assert x[8] == 2.0  # trader_wallet index


def test_wallet_with_none_metrics():
    """Wallet with some None optional metrics still builds; None replaced with 0."""
    analytics = {
        "wallet": "Abc123",
        "wallet_type": "unknown",
        "metrics": {
            "wallet_age_days": 100,
            "tx_count": 50,
            "unique_programs": None,
            "token_accounts": None,
        },
        "scam": {"scam_interactions": None},
        "rugpull": {"rugpull_interactions": None},
        "wallet_cluster": {},
    }
    x = build_features(analytics)
    assert x is not None
    assert x[2] == 0.0
    assert x[3] == 0.0
    assert x[5] == 0.0
    assert x[6] == 0.0
    assert x[0] == min(100 / 365.0, 5.0)
    assert x[1] == min(50 / 1000.0, 5.0)


def test_invalid_wallet_missing_tx_count():
    """Invalid wallet (tx_count None) raises and is not built."""
    analytics = {
        "wallet": "Invalid1",
        "metrics": {
            "wallet_age_days": 30,
            "tx_count": None,
            "unique_programs": 5,
        },
    }
    with pytest.raises(ValueError, match="invalid wallet metrics"):
        build_features(analytics)


def test_invalid_wallet_missing_wallet_age():
    """Invalid wallet (wallet_age_days None) raises and is not built."""
    analytics = {
        "wallet": "Invalid2",
        "metrics": {
            "wallet_age_days": None,
            "tx_count": 10,
            "unique_programs": 5,
        },
    }
    with pytest.raises(ValueError, match="invalid wallet metrics"):
        build_features(analytics)


def test_invalid_wallet_no_metrics():
    """Missing or empty metrics dict is invalid."""
    analytics = {"wallet": "NoMetrics", "metrics": None}
    with pytest.raises(ValueError, match="invalid wallet metrics"):
        build_features(analytics)


def test_is_valid_metrics():
    """is_valid_metrics returns True only when tx_count and wallet_age_days are non-None."""
    assert is_valid_metrics({"wallet_age_days": 1, "tx_count": 1}) is True
    assert is_valid_metrics({"wallet_age_days": 0, "tx_count": 0}) is True
    assert is_valid_metrics({"wallet_age_days": None, "tx_count": 1}) is False
    assert is_valid_metrics({"wallet_age_days": 1, "tx_count": None}) is False
    assert is_valid_metrics(None) is False
    assert is_valid_metrics({}) is False


def test_scaling_caps_at_five():
    """Large raw values are capped at 5.0; all features stay in 0–5."""
    analytics = {
        "wallet": "Whale",
        "wallet_type": "cold_wallet",
        "metrics": {
            "wallet_age_days": 365 * 10,
            "tx_count": 100_000,
            "unique_programs": 100,
            "token_accounts": 50,
        },
        "scam": {"scam_interactions": 20},
        "rugpull": {"rugpull_interactions": 15},
        "wallet_cluster": {"cluster_size": 100},
    }
    x = build_features(analytics)
    assert np.all(x >= 0) and np.all(x <= 5.0)
    assert x[0] == 5.0   # wallet_age 3650/365
    assert x[1] == 5.0   # tx 100_000/1000
    assert x[2] == 5.0   # unique_programs 100/20
    assert x[3] == 2.5   # token_accounts 50/20
    assert x[5] == 2.0   # scam 20/10
    assert x[6] == 1.5   # rugpull 15/10
    assert x[7] == 5.0   # cluster 100/10 capped at 5


def test_get_feature_names_unchanged():
    """Feature names list is unchanged for compatibility."""
    names = get_feature_names()
    assert names == FEATURE_NAMES
    assert len(names) == len(FEATURE_NAMES)
