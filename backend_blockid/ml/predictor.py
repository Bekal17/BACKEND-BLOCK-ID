"""
ML prediction module for BlockID Trust Model.

Single entrypoint predict_wallet(wallet_analysis_data): build features,
load model, return score, risk_label, and class probabilities.
Designed for use from analytics_pipeline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.feature_builder import build_features
from backend_blockid.ml.predict import _load_model

logger = get_logger(__name__)

# Score bands for risk_label (same semantics as trust_engine: high score = low risk)
SCORE_TO_RISK = [(34, "HIGH"), (67, "MEDIUM"), (101, "LOW")]


def score_to_risk_label(score: int) -> str:
    """Map trust score 0-100 to risk_label LOW / MEDIUM / HIGH. Public for pipeline blend."""
    s = max(0, min(100, score))
    for threshold, label in SCORE_TO_RISK:
        if s < threshold:
            return label
    return "LOW"


def predict_wallet(
    wallet_analysis_data: dict[str, Any],
    model_path: str | Path | None = None,
) -> dict[str, Any]:
    """
    Run ML prediction on wallet analysis data.

    Flow: build_features(wallet_analysis_data) -> load model.pkl -> predict risk score.
    Returns dict: score (0-100), risk_label ("LOW"|"MEDIUM"|"HIGH"), probabilities.

    If model is missing or features are invalid, returns score/risk_label from
    wallet_analysis_data when present, with empty probabilities and model_loaded=False.
    """
    out: dict[str, Any] = {
        "score": None,
        "risk_label": None,
        "probabilities": {},
        "model_loaded": False,
    }

    # Fallback from rule-based result if present
    rule_score = wallet_analysis_data.get("score")
    if rule_score is not None:
        try:
            out["score"] = int(rule_score)
            out["risk_label"] = wallet_analysis_data.get("risk_label") or score_to_risk_label(out["score"])
        except (TypeError, ValueError):
            pass

    try:
        feats = build_features(wallet_analysis_data)
    except ValueError as e:
        logger.debug("predict_wallet_skip_invalid", error=str(e))
        return out

    clf, _config = _load_model(model_path)
    if clf is None:
        return out

    try:
        X = np.asarray(feats).reshape(1, -1)
        proba = clf.predict_proba(X)[0]
        classes = getattr(clf, "classes_", np.arange(len(proba)))
        # Map to 3 bands: low (0), medium (1), high (2) trust
        proba_3 = np.zeros(3)
        for i, c in enumerate(classes):
            if 0 <= c < 3:
                proba_3[int(c)] = proba[i]
        if proba_3.sum() > 0:
            proba_3 /= proba_3.sum()
        mid = np.array([16.5, 50.0, 83.5], dtype=np.float64)
        ml_score = max(0, min(100, int(round(float(np.dot(proba_3, mid))))))
        out["score"] = ml_score
        out["risk_label"] = score_to_risk_label(ml_score)
        out["probabilities"] = {
            "low": float(proba_3[0]),
            "medium": float(proba_3[1]),
            "high": float(proba_3[2]),
        }
        out["model_loaded"] = True
    except Exception as e:
        logger.warning("predict_wallet_failed", error=str(e))

    return out
