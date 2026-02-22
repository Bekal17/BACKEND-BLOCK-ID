"""
Tests for BlockID ML Trust Model v1: feature builder, training, prediction.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import numpy as np
import pytest

from backend_blockid.ml.feature_builder import (
    FEATURE_NAMES,
    build_features,
    get_feature_names,
)
from backend_blockid.ml.predict import (
    load_model_and_predict,
    proba_to_adjusted_score,
    predict_trust_proba,
)
from backend_blockid.ml.train_model import (
    load_training_data_from_json,
    train_model,
)


def _mock_analytics(
    wallet_age_days: int = 30,
    tx_count: int = 50,
    unique_programs: int = 5,
    token_accounts: int = 2,
    nft_count: int = 0,
    scam_interactions: int = 0,
    rugpull_interactions: int = 0,
    cluster_size: int = 3,
    wallet_type: str = "trader_wallet",
) -> dict:
    return {
        "wallet": "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka",
        "wallet_type": wallet_type,
        "metrics": {
            "wallet_age_days": wallet_age_days,
            "tx_count": tx_count,
            "unique_programs": unique_programs,
            "token_accounts": token_accounts,
        },
        "scam": {"scam_interactions": scam_interactions},
        "rugpull": {"rugpull_interactions": rugpull_interactions},
        "wallet_cluster": {"cluster_size": cluster_size},
        "nft_scam": {"received_scam_nft": nft_count if nft_count else 0},
    }


def test_build_features_shape():
    """build_features returns 1D array of 9 floats."""
    analytics = _mock_analytics()
    x = build_features(analytics)
    assert isinstance(x, np.ndarray)
    assert x.dtype == np.float64
    assert x.ndim == 1
    assert x.shape[0] == len(FEATURE_NAMES)


def test_build_features_values():
    """Feature values are normalized to 0–5 range."""
    analytics = _mock_analytics(
        wallet_age_days=100,
        tx_count=200,
        unique_programs=10,
        token_accounts=5,
        scam_interactions=1,
        rugpull_interactions=2,
        cluster_size=7,
        wallet_type="cold_wallet",
    )
    x = build_features(analytics)
    # Normalized: wallet_age/365, tx/1000, programs/20, token_acc/20, nft/20, scam/10, rugpull/10, cluster/10, type 0-5
    assert 0 <= x[0] <= 5 and abs(x[0] - 100 / 365) < 1e-6
    assert 0 <= x[1] <= 5 and abs(x[1] - 200 / 1000) < 1e-6
    assert 0 <= x[2] <= 5 and abs(x[2] - 10 / 20) < 1e-6
    assert 0 <= x[3] <= 5 and abs(x[3] - 5 / 20) < 1e-6
    assert 0 <= x[5] <= 5 and abs(x[5] - 1 / 10) < 1e-6
    assert 0 <= x[6] <= 5 and abs(x[6] - 2 / 10) < 1e-6
    assert 0 <= x[7] <= 5 and abs(x[7] - 7 / 10) < 1e-6
    assert x[8] == 0  # cold_wallet


def test_get_feature_names():
    """Feature names list has 9 elements and includes expected keys."""
    names = get_feature_names()
    assert len(names) == len(FEATURE_NAMES)
    assert "wallet_age_days" in names
    assert "tx_count" in names
    assert "cluster_size" in names
    assert "wallet_type_encoded" in names


def test_train_model_saves_pkl_and_config():
    """Training on mock data produces model.pkl and model_config.json."""
    training = [
        (_mock_analytics(tx_count=10, wallet_age_days=5), 25.0),
        (_mock_analytics(tx_count=100, wallet_age_days=100), 55.0),
        (_mock_analytics(tx_count=500, wallet_age_days=365), 85.0),
        (_mock_analytics(tx_count=50), 50.0),
        (_mock_analytics(tx_count=200, scam_interactions=1), 30.0),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        model_path = Path(tmp) / "model.pkl"
        config_path = Path(tmp) / "model_config.json"
        clf, X, y = train_model(
            training,
            model_path=model_path,
            config_path=config_path,
            n_estimators=10,
        )
        assert model_path.is_file()
        assert config_path.is_file()
        assert X.shape[0] == 5
        assert y.shape[0] == 5
        assert clf.n_estimators == 10


def test_load_training_data_from_json(tmp_path):
    """load_training_data_from_json parses JSON and returns (analytics, score) list."""
    data = [
        {"analytics": _mock_analytics(tx_count=10), "score": 30},
        {"analytics": _mock_analytics(tx_count=100), "score": 70},
    ]
    path = tmp_path / "train.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    loaded = load_training_data_from_json(path)
    assert len(loaded) == 2
    assert loaded[0][1] == 30.0
    assert loaded[1][1] == 70.0


def test_predict_trust_proba_no_model():
    """When no model.pkl, predict_trust_proba returns (None, None)."""
    analytics = _mock_analytics()
    with tempfile.TemporaryDirectory() as tmp:
        proba, clf = predict_trust_proba(analytics, model_path=Path(tmp) / "nonexistent.pkl")
    assert proba is None
    assert clf is None


def test_predict_trust_proba_with_model():
    """When model exists, predict_trust_proba returns (proba array, clf)."""
    training = [
        (_mock_analytics(tx_count=i * 50), 20.0 + i * 25) for i in range(5)
    ]
    with tempfile.TemporaryDirectory() as tmp:
        model_path = Path(tmp) / "model.pkl"
        config_path = Path(tmp) / "model_config.json"
        train_model(training, model_path=model_path, config_path=config_path, n_estimators=5)
        proba, clf = predict_trust_proba(_mock_analytics(tx_count=100), model_path=model_path)
    assert proba is not None
    assert len(proba) == 3
    assert np.isclose(proba.sum(), 1.0)
    assert clf is not None


def test_proba_to_adjusted_score():
    """proba_to_adjusted_score blends rule score with ML expected value."""
    proba = [0.2, 0.5, 0.3]
    rule = 50
    out = proba_to_adjusted_score(proba, rule, blend_weight=0.0)
    assert out == 50
    out_full_ml = proba_to_adjusted_score(proba, rule, blend_weight=1.0)
    expected_ml = 0.2 * 16.5 + 0.5 * 50 + 0.3 * 83.5
    assert out_full_ml == max(0, min(100, int(round(expected_ml))))


def test_load_model_and_predict_no_model():
    """load_model_and_predict returns model_loaded=False when no model."""
    analytics = _mock_analytics()
    with tempfile.TemporaryDirectory() as tmp:
        out = load_model_and_predict(analytics, model_path=Path(tmp) / "none.pkl")
    assert out["model_loaded"] is False
    assert out["ml_score"] is None


def test_load_model_and_predict_with_model():
    """load_model_and_predict returns proba and ml_score when model exists."""
    training = [
        (_mock_analytics(tx_count=10), 25.0),
        (_mock_analytics(tx_count=100), 75.0),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        model_path = Path(tmp) / "model.pkl"
        config_path = Path(tmp) / "config.json"
        train_model(training, model_path=model_path, config_path=config_path, n_estimators=5)
        out = load_model_and_predict(_mock_analytics(tx_count=50), model_path=model_path)
    assert out["model_loaded"] is True
    assert out["proba"] is not None
    assert len(out["proba"]) == 3
    assert out["ml_score"] is not None
    assert 0 <= out["ml_score"] <= 100
