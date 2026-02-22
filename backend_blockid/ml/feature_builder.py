"""
Feature builder for BlockID ML Trust Model.

Builds a fixed-size numeric feature vector from analytics result
(run_wallet_analysis output) for training and inference.
Features are normalized to a small range (0–5) to avoid scale bias.
Invalid wallets (missing required metrics) are rejected.

Reputation signals (avg_tx_value, dex/lp counts, unique_counterparties,
cluster_size_estimate, scam_cluster_flag) are read from analytics_result when
present; missing values default to 0 and are logged.
"""

from __future__ import annotations

from typing import Any

import numpy as np

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# Wallet type to integer encoding (fixed order for reproducibility); kept in 0–5 range
WALLET_TYPE_ORDER = (
    "cold_wallet",
    "service_wallet",
    "trader_wallet",
    "nft_wallet",
    "inactive_wallet",
    "unknown",
)
WALLET_TYPE_TO_IDX = {t: i for i, t in enumerate(WALLET_TYPE_ORDER)}

# Original 9 features (backward compatible) + 6 new reputation signals
FEATURE_NAMES = [
    "wallet_age_days",
    "tx_count",
    "unique_programs",
    "token_accounts",
    "nft_count",
    "scam_interactions",
    "rugpull_interactions",
    "cluster_size",
    "wallet_type_encoded",
    "avg_tx_value",
    "dex_interaction_count",
    "lp_interaction_count",
    "unique_counterparties",
    "cluster_size_estimate",
    "scam_cluster_flag",
]


def is_valid_metrics(metrics: dict[str, Any] | None) -> bool:
    """
    Return False if required metrics are missing (None), so we do not build features for invalid wallets.
    """
    if metrics is None:
        return False
    if metrics.get("tx_count") is None:
        return False
    if metrics.get("wallet_age_days") is None:
        return False
    return True


def _safe_int(value: Any, default: int = 0) -> int:
    """Coerce to int; return default on None or failure."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    """Coerce to float; return default on None or failure."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _log_missing(wallet: str, name: str) -> None:
    """Log when a reputation signal is missing (debug to avoid spam)."""
    logger.debug("feature_builder_missing_signal", wallet=wallet[:20] + "..." if len(wallet) > 20 else wallet, feature=name)


def build_features(analytics_result: dict[str, Any]) -> np.ndarray:
    """
    Build normalized feature vector from analytics result (run_wallet_analysis output).

    All numeric features are scaled to 0–5. Invalid wallets (missing tx_count or
    wallet_age_days) are skipped: logs a warning and raises ValueError.
    Features: wallet_age_days, tx_count, unique_programs, token_accounts,
    nft_count, scam_interactions, rugpull_interactions, cluster_size, wallet_type_encoded.
    Returns 1D numpy array of shape (n_features,) float64.
    """
    metrics = analytics_result.get("metrics") or {}
    if not is_valid_metrics(metrics):
        logger.warning(
            "feature_builder_skip_invalid_wallet",
            wallet=analytics_result.get("wallet") or metrics.get("wallet"),
        )
        raise ValueError("invalid wallet metrics: tx_count and wallet_age_days required")

    scam = analytics_result.get("scam") or {}
    rugpull = analytics_result.get("rugpull") or {}
    wallet_cluster = analytics_result.get("wallet_cluster") or {}
    wallet_type = (analytics_result.get("wallet_type") or "unknown").strip() or "unknown"

    # Replace None with 0 for optional metrics
    wallet_age_days = metrics.get("wallet_age_days")
    wallet_age_days = 0 if wallet_age_days is None else _safe_int(wallet_age_days)
    tx_count = metrics.get("tx_count")
    tx_count = 0 if tx_count is None else _safe_int(tx_count)
    unique_programs = _safe_int(metrics.get("unique_programs"))
    token_accounts = _safe_int(metrics.get("token_accounts"))
    nft_count_raw = _safe_int(metrics.get("nft_count")) or _safe_int(
        (analytics_result.get("nft_scam") or {}).get("received_scam_nft")
    )
    if nft_count_raw == 0 and token_accounts > 0:
        nft_count_raw = token_accounts
    scam_interactions = _safe_int(scam.get("scam_interactions"))
    rugpull_interactions = _safe_int(rugpull.get("rugpull_interactions"))
    cluster_size = _safe_int(wallet_cluster.get("cluster_size"), 1)
    wallet_type_encoded = WALLET_TYPE_TO_IDX.get(wallet_type, len(WALLET_TYPE_ORDER) - 1)

    # Normalize to 0–5 range (and handle None as 0)
    wallet_age = min((wallet_age_days or 0) / 365.0, 5.0)
    tx = min((tx_count or 0) / 1000.0, 5.0)
    unique_prog = min((unique_programs or 0) / 20.0, 5.0)
    token_acc = min((token_accounts or 0) / 20.0, 5.0)
    scam_int = min((scam_interactions or 0) / 10.0, 5.0)
    rugpull_int = min((rugpull_interactions or 0) / 10.0, 5.0)
    cluster = min((cluster_size or 1) / 10.0, 5.0)
    # wallet_type_encoded already 0–5

    # Advanced ML features: from metrics (wallet_scanner) or reputation; default 0 if missing
    wallet_id = (analytics_result.get("wallet") or metrics.get("wallet") or "")
    rep = analytics_result.get("reputation") or {}
    wallet_short = (wallet_id[:20] + "...") if len(wallet_id) > 20 else wallet_id

    avg_tx_val = _safe_float(metrics.get("avg_tx_value") or rep.get("avg_tx_value"))
    if metrics.get("avg_tx_value") is None and rep.get("avg_tx_value") is None:
        _log_missing(wallet_short, "avg_tx_value")
    avg_tx_value_norm = min((avg_tx_val or 0.0) / 10.0, 1.0)

    nft_count_val = nft_count_raw
    nft_norm = min((nft_count_val or 0) / 50.0, 1.0)

    dex_count = _safe_int(
        metrics.get("dex_interactions") or metrics.get("dex_interaction_count")
        or rep.get("dex_interactions") or rep.get("dex_interaction_count")
    )
    if metrics.get("dex_interactions") is None and metrics.get("dex_interaction_count") is None:
        _log_missing(wallet_short, "dex_interactions")
    dex_norm = min((dex_count or 0) / 100.0, 1.0)

    lp_count = _safe_int(
        metrics.get("lp_interactions") or metrics.get("lp_interaction_count")
        or rep.get("lp_interactions") or rep.get("lp_interaction_count")
    )
    if metrics.get("lp_interactions") is None and metrics.get("lp_interaction_count") is None:
        _log_missing(wallet_short, "lp_interactions")
    lp_norm = min((lp_count or 0) / 20.0, 1.0)

    if "unique_counterparties" not in metrics and "unique_counterparties" not in rep:
        _log_missing(wallet_short, "unique_counterparties")
    unique_cp = _safe_int(metrics.get("unique_counterparties") or rep.get("unique_counterparties"))
    unique_cp_norm = min((unique_cp or 0) / 100.0, 5.0)

    cluster_est = _safe_int(
        wallet_cluster.get("cluster_size_estimate") or wallet_cluster.get("cluster_size") or metrics.get("cluster_size"),
        1,
    )
    if "cluster_size_estimate" not in wallet_cluster and "cluster_size" not in wallet_cluster and "cluster_size" not in metrics:
        _log_missing(wallet_short, "cluster_size")
    cluster_norm = min((cluster_est or 1) / 50.0, 1.0)

    # scam_cluster_flag: from wallet_cluster.cluster_risk or metrics.scam_cluster_flag (from local scam wallet list)
    scam_cluster_flag = 1.0 if (
        (wallet_cluster.get("cluster_risk") or "").strip().upper() == "HIGH"
        or _safe_int(metrics.get("scam_cluster_flag")) == 1
    ) else 0.0

    vec = np.array(
        [
            float(wallet_age),
            float(tx),
            float(unique_prog),
            float(token_acc),
            float(nft_norm),
            float(scam_int),
            float(rugpull_int),
            float(cluster),
            float(wallet_type_encoded),
            float(avg_tx_value_norm),
            float(dex_norm),
            float(lp_norm),
            float(unique_cp_norm),
            float(cluster_norm),
            float(scam_cluster_flag),
        ],
        dtype=np.float64,
    )
    logger.info("feature_builder_features", wallet=wallet_short, features=vec.tolist())
    return vec


def get_feature_names() -> list[str]:
    """Return ordered feature names (for inspection and persistence)."""
    return list(FEATURE_NAMES)
