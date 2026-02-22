"""
BlockID ML Trust Model v1.

Feature building, training (RandomForest), and prediction for trust score.
"""

from backend_blockid.ml.feature_builder import build_features
from backend_blockid.ml.predict import load_model_and_predict, predict_trust_proba
from backend_blockid.ml.predictor import predict_wallet, score_to_risk_label

__all__ = [
    "build_features",
    "load_model_and_predict",
    "predict_trust_proba",
    "predict_wallet",
    "score_to_risk_label",
]
