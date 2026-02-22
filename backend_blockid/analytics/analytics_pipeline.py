"""
Analytics pipeline: run full wallet analysis (scan -> risk -> trust).

Single entrypoint for batch publisher and API: scan_wallet, calculate_risk,
calculate_trust; returns combined result with metrics, risk, score, risk_label.
"""

from __future__ import annotations

import os
from typing import Any

from backend_blockid.analytics.nft_scam_detector import detect_nft_scam_role
from backend_blockid.analytics.risk_engine import calculate_risk
from backend_blockid.analytics.rugpull_detector import detect_rugpull_tokens
from backend_blockid.analytics.scam_detector import detect_scam_interactions
from backend_blockid.analytics.trust_engine import calculate_trust
from backend_blockid.analytics.wallet_classifier import classify_wallet
from backend_blockid.analytics.wallet_graph import detect_wallet_cluster
from backend_blockid.analytics.wallet_scanner import scan_wallet
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)


def run_wallet_analysis(wallet: str) -> dict[str, Any]:
    """
    Run full analysis for one wallet: scan -> risk -> trust.

    Returns dict: wallet, metrics, risk, score, risk_label.
    Safe to call with invalid wallet; scanner returns zeros and risk/trust still run.
    """
    wallet = (wallet or "").strip()
    logger.info("analytics_pipeline_start", wallet=wallet[:16] + "..." if len(wallet) > 16 else wallet)

    metrics = scan_wallet(wallet)
    wallet_type = classify_wallet(metrics)
    risk = calculate_risk(metrics, wallet_type=wallet_type)
    scam = detect_scam_interactions(wallet)
    scam_interactions = scam.get("scam_interactions") or 0
    scam_programs = scam.get("scam_programs") or []
    scam_flags = [f"scam_program:{pid}" for pid in scam_programs]

    nft_scam = detect_nft_scam_role(wallet)
    nft_scam_role = nft_scam.get("role") or "none"

    rugpull = detect_rugpull_tokens(wallet)
    rugpull_interactions = rugpull.get("rugpull_interactions") or 0

    wallet_cluster = detect_wallet_cluster(wallet)
    in_scam_cluster = (wallet_cluster.get("cluster_risk") or "LOW") == "HIGH"

    score, risk_label, reason_codes = calculate_trust(
        metrics,
        risk,
        scam_interactions=scam_interactions,
        rugpull_interactions=rugpull_interactions,
        in_scam_cluster=in_scam_cluster,
        nft_scam_role=nft_scam_role,
        wallet_type=wallet_type,
        nft_scam=nft_scam,
    )

    result = {
        "wallet": wallet,
        "wallet_type": wallet_type,
        "metrics": metrics,
        "risk": risk,
        "scam": scam,
        "scam_flags": scam_flags,
        "nft_scam": nft_scam,
        "rugpull": rugpull,
        "wallet_cluster": wallet_cluster,
        "score": score,
        "risk_label": risk_label,
        "reason_codes": reason_codes,
    }

    try:
        from backend_blockid.ml.predictor import predict_wallet, score_to_risk_label

        pred = predict_wallet(result)
        result["ml_trust"] = pred
        result["probabilities"] = pred.get("probabilities") or {}
        if pred.get("model_loaded") and pred.get("score") is not None:
            blend = float(os.getenv("ML_SCORE_BLEND_WEIGHT", "0.3").strip() or "0.3")
            blend = min(1.0, max(0.0, blend))
            ml_score = pred["score"]
            adjusted = int(round((1.0 - blend) * score + blend * ml_score))
            adjusted = max(0, min(100, adjusted))
            result["score"] = adjusted
            result["risk_label"] = score_to_risk_label(adjusted)
            result["score_rule"] = score
    except Exception as e:
        logger.debug("analytics_pipeline_ml_skip", error=str(e))
        result["ml_trust"] = {"model_loaded": False}
        result["probabilities"] = {}
    logger.info(
        "analytics_pipeline_done",
        wallet=wallet[:16] + "..." if len(wallet) > 16 else wallet,
        score=score,
        risk_label=risk_label,
    )
    return result
