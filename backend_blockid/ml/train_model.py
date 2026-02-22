"""
Train BlockID ML Trust Model v1 (RandomForestClassifier).

Loads historical (analytics, score) data from JSON or DB, or feature CSV from
build_dataset; bins score into classes (or uses binary label); fits RandomForest,
saves model.pkl and config. Optional train/test split and evaluation metrics.
"""

from __future__ import annotations

import csv
import json
import joblib
import os
import pickle
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    precision_score,
    recall_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.feature_builder import build_features, get_feature_names
from backend_blockid.ml.save_model import save_model

logger = get_logger(__name__)

OUTPUT_DIR = Path(__file__).resolve().parent
MODELS_DIR = OUTPUT_DIR / "models"
DATA_DIR = OUTPUT_DIR.parent / "data"
DEFAULT_MODEL_PATH = OUTPUT_DIR / "model.pkl"
DEFAULT_CONFIG_PATH = OUTPUT_DIR / "model_config.json"
FLOW_FEATURES_PATH = DATA_DIR / "flow_features.csv"
DRAINER_FEATURES_PATH = DATA_DIR / "drainer_features.csv"

# Flow feature columns (from flow_features.py); join key is wallet
FLOW_FEATURE_COLUMNS = [
    "total_tx",
    "unique_destinations",
    "rapid_tx_count",
    "avg_tx_interval",
    "percent_to_new_wallets",
    "tx_chain_length_estimate",
]

# Drainer feature columns (from drainer_detection.py); join key is wallet
DRAINER_FEATURE_COLUMNS = [
    "approval_like_count",
    "rapid_outflow_count",
    "multi_victim_pattern",
    "new_contract_interaction_count",
    "swap_then_transfer_pattern",
    "percent_to_same_cluster",
]

# Score bins for classification: 0 = low (0-33), 1 = medium (34-66), 2 = high (67-100)
SCORE_BIN_EDGES = [0, 34, 67, 101]


def _score_to_class(score: float) -> int:
    """Bin score into 0 (low), 1 (medium), 2 (high)."""
    s = max(0, min(100, float(score)))
    if s < 34:
        return 0
    if s < 67:
        return 1
    return 2


def merge_flow_features(df: pd.DataFrame, flow_path: Path) -> pd.DataFrame:
    """
    Left-join flow_features.csv on wallet. Fills missing flow columns with 0.
    No-op if flow_path missing or df has no 'wallet' column (keeps compatibility).
    """
    if not flow_path.is_file():
        logger.debug("train_model_flow_features_missing", path=str(flow_path))
        return df
    if "wallet" not in df.columns:
        logger.debug("train_model_no_wallet_column_skip_flow_merge")
        return df
    try:
        flow = pd.read_csv(flow_path)
    except Exception as e:
        logger.warning("train_model_flow_load_failed", path=str(flow_path), error=str(e))
        return df
    if "wallet" not in flow.columns:
        return df
    flow_cols = [c for c in FLOW_FEATURE_COLUMNS if c in flow.columns]
    if not flow_cols:
        return df
    df = df.merge(flow[["wallet"] + flow_cols], on="wallet", how="left")
    for c in flow_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[flow_cols] = df[flow_cols].fillna(0)
    logger.info("train_model_flow_merged", flow_cols=flow_cols, rows=len(df))
    return df


def merge_drainer_features(df: pd.DataFrame, drainer_path: Path) -> pd.DataFrame:
    """
    Left-join drainer_features.csv on wallet. Fills missing drainer columns with 0.
    No-op if drainer_path missing or df has no 'wallet' column.
    """
    if not drainer_path.is_file():
        logger.debug("train_model_drainer_features_missing", path=str(drainer_path))
        return df
    if "wallet" not in df.columns:
        logger.debug("train_model_no_wallet_column_skip_drainer_merge")
        return df
    try:
        drainer = pd.read_csv(drainer_path)
    except Exception as e:
        logger.warning("train_model_drainer_load_failed", path=str(drainer_path), error=str(e))
        return df
    if "wallet" not in drainer.columns:
        return df
    drainer_cols = [c for c in DRAINER_FEATURE_COLUMNS if c in drainer.columns]
    if not drainer_cols:
        return df
    df = df.merge(drainer[["wallet"] + drainer_cols], on="wallet", how="left")
    for c in drainer_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df[drainer_cols] = df[drainer_cols].fillna(0)
    logger.info("train_model_drainer_merged", drainer_cols=drainer_cols, rows=len(df))
    return df


def print_feature_stats(X_df: pd.DataFrame, label_series: pd.Series) -> None:
    """Print summary stats for feature matrix (count, mean, std, nulls) and label distribution."""
    print("\nFEATURE STATS (training set)")
    print("-" * 50)
    stats = X_df.agg(["count", "mean", "std", "min", "max"]).T
    stats["nulls"] = X_df.isna().sum()
    print(stats.round(4).to_string())
    print("\nLABEL DISTRIBUTION")
    print("-" * 50)
    print(label_series.value_counts().sort_index().to_string())
    print()


def load_training_data_from_csv(path: str | Path) -> tuple[np.ndarray, np.ndarray]:
    """
    Load training data from dataset CSV (f1..fN, label).

    Returns (X, y) with X float and y int (0=scam, 1=good). Drops rows with NaN or invalid label.
    """
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Dataset CSV not found: {path}")
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ValueError("Dataset CSV has no header")
        rows = list(reader)
    n_features = len(header) - 1
    if n_features < 1:
        raise ValueError("Dataset CSV must have at least one feature column and label")
    X_list: list[list[float]] = []
    y_list: list[int] = []
    for row in rows:
        if len(row) <= n_features:
            continue
        try:
            label = int(float(row[n_features].strip()))
        except (ValueError, TypeError):
            continue
        if label not in (0, 1):
            continue
        feats: list[float] = []
        bad = False
        for i in range(n_features):
            try:
                v = float(row[i].strip())
            except (ValueError, TypeError):
                bad = True
                break
            feats.append(v)
        if bad or len(feats) != n_features:
            continue
        X_list.append(feats)
        y_list.append(label)
    if not X_list:
        raise ValueError("No valid rows in dataset CSV")
    X = np.array(X_list, dtype=np.float64)
    y = np.array(y_list, dtype=np.int64)
    logger.info("train_model_loaded_csv", path=str(path), n_samples=len(y))
    return X, y


def load_training_data_from_json(path: str | Path) -> list[tuple[dict[str, Any], float]]:
    """
    Load training data from JSON file.

    Expected format: list of {"analytics": <run_wallet_analysis result>, "score": int or float}.
    Returns list of (analytics_dict, score).
    """
    path = Path(path)
    if not path.is_file():
        logger.warning("train_model_json_missing", path=str(path))
        return []
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        return []
    out: list[tuple[dict[str, Any], float]] = []
    for i, row in enumerate(data):
        if not isinstance(row, dict):
            continue
        analytics = row.get("analytics")
        score = row.get("score")
        if analytics is None or score is None:
            continue
        try:
            out.append((analytics, float(score)))
        except (TypeError, ValueError):
            continue
    logger.info("train_model_loaded_json", path=str(path), count=len(out))
    return out


def load_training_data_from_db(db: Any, limit: int = 5000) -> list[tuple[dict[str, Any], float]]:
    """
    Load training data from Database trust_scores.

    Uses metadata_json as analytics snapshot when it contains metrics/scam/rugpull/etc.
    Falls back to minimal analytics from (wallet, score) when metadata is partial.
    """
    out: list[tuple[dict[str, Any], float]] = []
    try:
        backend = getattr(db, "_backend", db)
        with backend._cursor() as cur:
            cur.execute(
                "SELECT wallet, score, metadata_json FROM trust_scores ORDER BY computed_at DESC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
    except Exception as e:
        logger.warning("train_model_db_query_failed", error=str(e))
        return []
    for row in rows:
        score = float(row["score"]) if row.get("score") is not None else 0.0
        meta = row.get("metadata_json")
        if meta and isinstance(meta, str):
            try:
                analytics = json.loads(meta)
            except json.JSONDecodeError:
                analytics = _minimal_analytics(None, score)
        else:
            analytics = _minimal_analytics(row.get("wallet"), score)
        if isinstance(analytics, dict) and "metrics" in analytics:
            out.append((analytics, score))
        elif isinstance(analytics, dict):
            analytics.setdefault("metrics", {})
            analytics.setdefault("scam", {})
            analytics.setdefault("rugpull", {})
            analytics.setdefault("wallet_cluster", {})
            analytics.setdefault("wallet_type", "unknown")
            analytics.setdefault("nft_scam", {})
            out.append((analytics, score))
    logger.info("train_model_loaded_db", count=len(out))
    return out


def _minimal_analytics(wallet: str | None, score: float) -> dict[str, Any]:
    """Build minimal analytics dict for DB rows that lack full snapshot."""
    return {
        "wallet": wallet or "",
        "metrics": {
            "wallet_age_days": 0,
            "tx_count": 0,
            "unique_programs": 0,
            "token_accounts": 0,
        },
        "scam": {"scam_interactions": 0},
        "rugpull": {"rugpull_interactions": 0},
        "wallet_cluster": {"cluster_size": 1},
        "wallet_type": "unknown",
        "nft_scam": {"received_scam_nft": 0},
    }


def train_model(
    training_data: list[tuple[dict[str, Any], float]],
    *,
    model_path: str | Path | None = None,
    config_path: str | Path | None = None,
    n_estimators: int = 100,
    random_state: int = 42,
) -> tuple[RandomForestClassifier, np.ndarray, np.ndarray]:
    """
    Train RandomForestClassifier on (analytics, score) pairs.

    training_data: list of (analytics_dict, score).
    Saves model to model_path and config (feature names, bin edges) to config_path.
    Returns (fitted classifier, X, y).
    """
    model_path = Path(model_path or os.getenv("ML_MODEL_PATH") or DEFAULT_MODEL_PATH)
    config_path = Path(config_path or os.getenv("ML_CONFIG_PATH") or DEFAULT_CONFIG_PATH)

    if not training_data:
        raise ValueError("training_data is empty; need at least one (analytics, score) pair")

    X_list: list[np.ndarray] = []
    y_list: list[int] = []
    for analytics, score in training_data:
        try:
            x = build_features(analytics)
            X_list.append(x)
            y_list.append(_score_to_class(score))
        except Exception as e:
            logger.debug("train_model_skip_row", error=str(e))
            continue

    if not X_list:
        raise ValueError("No valid feature vectors from training_data")

    X = np.stack(X_list)
    y = np.array(y_list, dtype=np.int64)

    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
        max_depth=10,
        min_samples_leaf=2,
    )
    clf.fit(X, y)

    model_path.parent.mkdir(parents=True, exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(clf, f)
    logger.info("train_model_saved", path=str(model_path))

    config = {
        "feature_names": get_feature_names(),
        "score_bin_edges": SCORE_BIN_EDGES,
        "n_estimators": n_estimators,
        "random_state": random_state,
    }
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    logger.info("train_model_config_saved", path=str(config_path))

    return clf, X, y


def train_and_evaluate(
    dataset_path: str | Path,
    *,
    model_path: str | Path | None = None,
    config_path: str | Path | None = None,
    test_size: float = 0.2,
    random_state: int = 42,
    n_estimators: int = 100,
) -> RandomForestClassifier:
    """
    Load dataset CSV with pandas, split 80/20, scale with StandardScaler,
    train RandomForestClassifier, save model + scaler with joblib, print accuracy.
    """
    path = Path(dataset_path)
    if not path.is_file():
        raise FileNotFoundError(f"Dataset CSV not found: {path}")

    df = pd.read_csv(path)
    if df.empty:
        raise Exception("Dataset empty")

    # Merge flow and drainer features when dataset has wallet column (left-join; missing -> 0)
    df = merge_flow_features(df, FLOW_FEATURES_PATH)
    df = merge_drainer_features(df, DRAINER_FEATURES_PATH)

    # Separate feature columns and label column; drop wallet so it is not used as a feature
    label_col = "label"
    if label_col not in df.columns:
        raise ValueError("Dataset must contain a 'label' column")
    drop_cols = [label_col]
    if "wallet" in df.columns:
        drop_cols.append("wallet")
    X_df = df.drop(columns=drop_cols)
    # Coerce to float and fill remaining NaN (e.g. from merge or bad values)
    X_df = X_df.apply(pd.to_numeric, errors="coerce").fillna(0)
    y = df[label_col].astype(int)
    mask = ~y.isna()
    X_df = X_df.loc[mask]
    y = y.loc[mask]
    if len(X_df) == 0:
        raise Exception("Dataset empty after dropping invalid rows")

    print_feature_stats(X_df, y)
    X = X_df.values.astype(np.float64)
    y = y.values

    stratify = y if len(np.unique(y)) > 1 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state, stratify=stratify
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    model = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
        max_depth=10,
        min_samples_leaf=2,
    )
    model.fit(X_train_scaled, y_train)
    y_pred = model.predict(X_test_scaled)

    # Feature importance (RandomForest; aligned to X_df column order)
    feature_names_used = X_df.columns.tolist()
    importances = model.feature_importances_
    importance_pairs = sorted(zip(feature_names_used, importances), key=lambda x: -x[1])
    print("\nFEATURE IMPORTANCE (train set)")
    print("-" * 50)
    for name, imp in importance_pairs:
        print(f"  {name}: {imp:.4f}")
    print()

    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred)

    print("\nEVALUATION (test set)")
    print("-" * 40)
    print(f"accuracy:  {acc:.4f}")
    print(f"precision: {prec:.4f}")
    print(f"recall:    {rec:.4f}")
    print("confusion matrix (rows=true, cols=pred):")
    print("  pred=0(scam)  pred=1(good)")
    print(cm)
    print()

    versioned_model_path, versioned_metadata_path, base_name = save_model(
        model,
        "trust_model",
        metrics={
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "dataset_size": len(X),
        },
        feature_list=feature_names_used,
        models_dir=MODELS_DIR,
    )
    # Save scaler with same timestamp for inference alignment
    scaler_path = MODELS_DIR / f"{base_name}_scaler.joblib"
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(scaler, scaler_path)
    logger.info("train_model_scaler_saved", path=str(scaler_path))

    model_path = Path(model_path or os.getenv("ML_MODEL_PATH") or DEFAULT_MODEL_PATH)
    config_path = Path(config_path or os.getenv("ML_CONFIG_PATH") or DEFAULT_CONFIG_PATH)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    logger.info("train_model_saved", path=str(model_path), versioned=str(versioned_model_path))

    # Persist actual feature order used (base + flow + drainer) so inference can align
    config = {
        "feature_names": feature_names_used,
        "score_bin_edges": SCORE_BIN_EDGES,
        "n_estimators": n_estimators,
        "random_state": random_state,
        "train_from_csv": True,
        "scaler_saved": True,
        "versioned_model_path": str(versioned_model_path),
        "versioned_scaler_path": str(scaler_path),
    }
    with open(config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
    logger.info("train_model_config_saved", path=str(config_path))

    return model


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Train BlockID Trust Model from dataset CSV (f1..f9, label); 80/20 split, print metrics, save model.",
    )
    parser.add_argument(
        "dataset_csv",
        type=Path,
        help="Path to dataset CSV produced by build_dataset.py",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=None,
        help=f"Output model path (default: {DEFAULT_MODEL_PATH})",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Fraction for test set (default: 0.2)",
    )
    args = parser.parse_args()
    try:
        train_and_evaluate(
            args.dataset_csv,
            model_path=args.model_path,
            test_size=args.test_size,
        )
    except (FileNotFoundError, ValueError) as e:
        print("ERROR:", e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
