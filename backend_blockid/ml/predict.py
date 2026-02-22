"""
Load trained BlockID ML Trust Model and predict trust score probability.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import numpy as np

from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.feature_builder import build_features

logger = get_logger(__name__)

DEFAULT_MODEL_PATH = Path(__file__).resolve().parent / "model.pkl"
DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent / "model_config.json"


def _load_model(path: str | Path | None = None):
    """Load pickled model from path. Returns (classifier, config_dict) or (None, None)."""
    import pickle

    path = Path(path or os.getenv("ML_MODEL_PATH") or DEFAULT_MODEL_PATH)
    if not path.is_file():
        logger.debug("predict_model_missing", path=str(path))
        return None, None
    try:
        with open(path, "rb") as f:
            clf = pickle.load(f)
    except Exception as e:
        logger.warning("predict_model_load_failed", path=str(path), error=str(e))
        return None, None
    config_path = Path(os.getenv("ML_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
    config: dict[str, Any] = {}
    if config_path.is_file():
        try:
            with open(config_path, encoding="utf-8") as f:
                config = json.load(f)
        except Exception:
            pass
    return clf, config


def predict_trust_proba(
    analytics_result: dict[str, Any],
    model_path: str | Path | None = None,
) -> tuple[np.ndarray | None, Any]:
    """
    Predict trust score class probabilities from analytics result.

    Returns (proba_array, classifier). proba_array is shape (3,) for classes
    low/medium/high, or None if model not found. classifier is the loaded model or None.
    """
    clf, _config = _load_model(model_path)
    if clf is None:
        return None, None
    try:
        x = build_features(analytics_result)
        X = x.reshape(1, -1)
        proba = clf.predict_proba(X)[0]
        return proba, clf
    except Exception as e:
        logger.warning("predict_trust_proba_failed", error=str(e))
        return None, clf


def proba_to_adjusted_score(
    proba: np.ndarray | list[float],
    rule_score: int,
    blend_weight: float = 0.5,
) -> int:
    """
    Blend rule-based score with ML expected score from class probabilities.

    proba: shape (3,) for classes low/medium/high (list or array). Expected score = proba[0]*16.5 + proba[1]*50 + proba[2]*83.5.
    rule_score: 0-100 from trust engine.
    blend_weight: weight for ML (0=rule only, 1=ML only). 0.5 = equal blend.
    Returns int 0-100.
    """
    proba = np.asarray(proba) if proba is not None else None
    if proba is None or len(proba) != 3:
        return rule_score
    mid = np.array([16.5, 50.0, 83.5], dtype=np.float64)
    ml_score = float(np.dot(proba, mid))
    blended = (1.0 - blend_weight) * rule_score + blend_weight * ml_score
    return max(0, min(100, int(round(blended))))


def load_model_and_predict(
    analytics_result: dict[str, Any],
    model_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Load model and return prediction result for pipeline integration.

    Returns dict: proba (list), proba_low, proba_medium, proba_high,
    ml_score (expected value 0-100), model_loaded (bool).
    """
    out: dict[str, Any] = {
        "proba": None,
        "proba_low": 0.0,
        "proba_medium": 0.0,
        "proba_high": 0.0,
        "ml_score": None,
        "model_loaded": False,
    }
    proba, clf = predict_trust_proba(analytics_result, model_path=model_path)
    if proba is None:
        return out
    out["model_loaded"] = True
    # Expand to 3 classes if model was trained with fewer (e.g. 2 classes)
    classes = getattr(clf, "classes_", np.arange(len(proba)))
    proba_3 = np.zeros(3)
    for i, c in enumerate(classes):
        if 0 <= c < 3:
            proba_3[int(c)] = proba[i]
    if proba_3.sum() > 0:
        proba_3 /= proba_3.sum()
    else:
        proba_3 = proba.tolist() if len(proba) >= 3 else [0.0, 1.0, 0.0]
        proba_3 = np.asarray(proba_3[:3])
    out["proba"] = proba_3.tolist()
    out["proba_low"] = float(proba_3[0])
    out["proba_medium"] = float(proba_3[1])
    out["proba_high"] = float(proba_3[2])
    mid = np.array([16.5, 50.0, 83.5])
    out["ml_score"] = max(0, min(100, int(round(float(np.dot(proba_3, mid))))))
    return out
