"""
Retrain BlockID RandomForest scam detection model using merged behavioral fingerprint features.

Loads wallet_scores.csv (base: wallet, risk_score, scam_probability, reason_code), then merges
cluster_features.csv, flow_features.csv, drainer_features.csv on wallet. Derives label from
scam_probability (>= 0.5 -> scam=1). Trains RandomForestClassifier, prints metrics and top-20
feature importance, saves model and feature list to backend_blockid/ml/models/.

Example dataset format:
  - wallet_scores.csv: wallet, risk_score, scam_probability, reason_code
  - cluster_features.csv: wallet, neighbor_count, scam_neighbor_count, cluster_size, distance_to_scam
  - flow_features.csv: wallet, total_tx, unique_destinations, rapid_tx_count, ...
  - drainer_features.csv: wallet, approval_like_count, rapid_outflow_count, ...

Command to run (from project root):
  py backend_blockid/ml/train_blockid_model.py
"""

from __future__ import annotations

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

from backend_blockid.config.env import load_blockid_env, print_blockid_startup
from backend_blockid.ml.save_model import save_model

_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
_MODELS_DIR = _SCRIPT_DIR / "models"

WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"
CLUSTER_FEATURES_CSV = _DATA_DIR / "cluster_features.csv"
FLOW_FEATURES_CSV = _DATA_DIR / "flow_features.csv"
DRAINER_FEATURES_CSV = _DATA_DIR / "drainer_features.csv"
FEATURE_LIST_PATH = _MODELS_DIR / "feature_list.txt"

TEST_SIZE = 0.2
RANDOM_STATE = 42
N_ESTIMATORS = 200
TOP_K_IMPORTANCE = 20

# Columns to exclude from features (ids, targets, metadata)
NON_FEATURE_COLUMNS = frozenset({
    "wallet",
    "reason_code",
    "scam_probability",
    "risk_score",
})


def _log(msg: str, **kwargs: Any) -> None:
    parts = [f"[train_blockid] {msg}"]
    for k, v in kwargs.items():
        parts.append(f" {k}={v}")
    print("".join(parts))


def load_csv(path: Path) -> pd.DataFrame:
    """Load CSV; return empty DataFrame on missing or error."""
    if not path.is_file():
        _log("skip missing", path=str(path))
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        _log("loaded", path=str(path), rows=len(df), cols=len(df.columns))
        return df
    except Exception as e:
        _log("load failed", path=str(path), error=str(e))
        return pd.DataFrame()


def merge_all_on_wallet(
    base: pd.DataFrame,
    cluster: pd.DataFrame,
    flow: pd.DataFrame,
    drainer: pd.DataFrame,
) -> pd.DataFrame:
    """Left-join all feature sets on wallet; fill missing with 0."""
    df = base.copy()
    for name, frame in [
        ("cluster", cluster),
        ("flow", flow),
        ("drainer", drainer),
    ]:
        if frame.empty or "wallet" not in frame.columns:
            continue
        join_cols = [c for c in frame.columns if c != "wallet"]
        if not join_cols:
            continue
        df = df.merge(frame[["wallet"] + join_cols], on="wallet", how="left")
        df[join_cols] = df[join_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
        _log("merged", dataset=name, extra_cols=len(join_cols))
    return df


def build_X_y(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series] | None:
    """
    Derive label from scam_probability (>= 0.5 -> 1). Build X from numeric columns
    excluding wallet, reason_code, scam_probability, risk_score. Returns (X_df, y) or None.
    """
    if "scam_probability" not in df.columns:
        _log("ERROR: base dataset must have scam_probability column")
        return None
    prob = pd.to_numeric(df["scam_probability"], errors="coerce")
    valid = prob.notna()
    y = (prob >= 0.5).astype(int).loc[valid]
    feature_cols = [c for c in df.columns if c not in NON_FEATURE_COLUMNS and c in df.select_dtypes(include=[np.number]).columns]
    if not feature_cols:
        _log("ERROR: no feature columns after exclusions")
        return None
    X_df = df.loc[valid, feature_cols].copy()
    X_df = X_df.apply(pd.to_numeric, errors="coerce").fillna(0)
    _log("built X,y", n_samples=len(X_df), n_features=len(feature_cols))
    return X_df, y


def main() -> int:
    load_blockid_env()
    print_blockid_startup("train_blockid_model")

    _MODELS_DIR.mkdir(parents=True, exist_ok=True)

    base = load_csv(WALLET_SCORES_CSV)
    if base.empty or "wallet" not in base.columns:
        _log("ERROR: wallet_scores.csv (with wallet column) is required")
        return 1

    cluster = load_csv(CLUSTER_FEATURES_CSV)
    flow = load_csv(FLOW_FEATURES_CSV)
    drainer = load_csv(DRAINER_FEATURES_CSV)

    df = merge_all_on_wallet(base, cluster, flow, drainer)
    result = build_X_y(df)
    if result is None:
        return 1
    X_df, y = result
    X = X_df.values.astype(np.float64)
    y = y.values

    if len(np.unique(y)) < 2:
        _log("ERROR: need both classes in label; unique labels", unique=np.unique(y).tolist())
        return 1

    # Use stratify only if every class has at least 2 samples (else train_test_split raises)
    _, counts = np.unique(y, return_counts=True)
    use_stratify = counts.min() >= 2
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y if use_stratify else None
    )

    _log("training", n_estimators=N_ESTIMATORS)
    model = RandomForestClassifier(n_estimators=N_ESTIMATORS, random_state=RANDOM_STATE)
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred)

    print("\nMETRICS (test set)")
    print("-" * 40)
    print(f"accuracy:  {acc:.4f}")
    print(f"precision: {prec:.4f}")
    print(f"recall:    {rec:.4f}")
    print("confusion matrix (rows=true, cols=pred):")
    print(cm)
    print()

    names = X_df.columns.tolist()
    importances = model.feature_importances_
    ranked = sorted(zip(names, importances), key=lambda x: -x[1])[:TOP_K_IMPORTANCE]
    print(f"TOP {TOP_K_IMPORTANCE} FEATURE IMPORTANCE")
    print("-" * 50)
    for name, imp in ranked:
        print(f"  {name}: {imp:.4f}")
    print()

    model_path, metadata_path, _ = save_model(
        model,
        "blockid_model",
        metrics={
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "dataset_size": len(X),
        },
        feature_list=names,
    )
    with open(FEATURE_LIST_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(names))
    _log("saved", model=str(model_path), metadata=str(metadata_path), feature_list=str(FEATURE_LIST_PATH))
    return 0


if __name__ == "__main__":
    sys.exit(main())
