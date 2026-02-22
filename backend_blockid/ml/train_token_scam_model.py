"""
Train a RandomForest model to detect scam tokens and creators for BlockID.

Usage:
    py backend_blockid/ml/train_token_scam_model.py

Reads token_features.csv and scam_wallets.csv, builds labels (scam_flag or creator in scam_wallets),
trains RandomForestClassifier, prints metrics, saves model to backend_blockid/models/token_scam_model.joblib.
"""

from __future__ import annotations

import csv
from pathlib import Path

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

from backend_blockid.ml.save_model import save_model

# Paths: script in backend_blockid/ml/, data and models in backend_blockid/ml/models/
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
_MODELS_DIR = _SCRIPT_DIR / "models"
TOKEN_FEATURES_CSV = _DATA_DIR / "token_features.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

FEATURE_COLUMNS = [
    "mint_authority_exists",
    "freeze_authority_exists",
    "metadata_missing",
    "decimals",
    "supply",
]
TEST_SIZE = 0.2
RANDOM_STATE = 42
N_ESTIMATORS = 200


def _load_scam_wallets(path: Path) -> set[str]:
    """Load wallet addresses from scam_wallets.csv (first column)."""
    out: set[str] = set()
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
            if w:
                out.add(w)
    return out


def _parse_creator_wallets(raw: str) -> list[str]:
    """Parse creator_wallets: semicolon-separated or empty."""
    if not raw or not isinstance(raw, str):
        return []
    return [x.strip() for x in raw.strip().split(";") if x.strip()]


def _bool_to_int(val) -> int:
    """Convert bool or string bool to 0/1."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0
    if isinstance(val, bool):
        return 1 if val else 0
    s = str(val).strip().lower()
    if s in ("true", "1", "yes"):
        return 1
    return 0


def main() -> int:
    print("[ml] loading dataset")
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not TOKEN_FEATURES_CSV.exists():
        print("[ml] ERROR: token_features.csv not found:", TOKEN_FEATURES_CSV)
        return 1

    df = pd.read_csv(TOKEN_FEATURES_CSV)
    scam_wallets = _load_scam_wallets(SCAM_WALLETS_CSV)

    # Labels: scam_flag from CSV or any creator in scam_wallets → scam=1
    labels = []
    for _, row in df.iterrows():
        scam_flag = _bool_to_int(row.get("scam_flag"))
        creator_wallets = _parse_creator_wallets(str(row.get("creator_wallets") or ""))
        creator_in_scam = 1 if any(c in scam_wallets for c in creator_wallets) else 0
        label = 1 if (scam_flag or creator_in_scam) else 0
        labels.append(label)
    y = np.array(labels, dtype=int)

    # Features: derive mint_authority_exists, freeze_authority_exists; use metadata_missing, decimals, supply
    mint_authority_exists = (
        (df["mint_authority"].fillna("").astype(str).str.strip() != "").astype(int)
    )
    freeze_authority_exists = (
        (df["freeze_authority"].fillna("").astype(str).str.strip() != "").astype(int)
    )
    metadata_missing = df["metadata_missing"].map(_bool_to_int)
    decimals = pd.to_numeric(df["decimals"], errors="coerce").fillna(0).astype(int)
    supply = pd.to_numeric(df["supply"], errors="coerce").fillna(0).astype(np.int64)

    X = pd.DataFrame({
        "mint_authority_exists": mint_authority_exists,
        "freeze_authority_exists": freeze_authority_exists,
        "metadata_missing": metadata_missing,
        "decimals": decimals,
        "supply": supply,
    })

    # Drop rows with any NaN (shouldn't happen after fillna)
    valid = ~X.isna().any(axis=1)
    X = X.loc[valid].values.astype(np.float64)
    y = y[valid]

    if X.shape[0] < 2:
        print("[ml] ERROR: not enough samples to train (need at least 2)")
        return 1

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE, stratify=y if len(np.unique(y)) > 1 else None
    )

    print("[ml] training model")
    clf = RandomForestClassifier(n_estimators=N_ESTIMATORS, random_state=RANDOM_STATE)
    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    prec = precision_score(y_test, y_pred, zero_division=0)
    rec = recall_score(y_test, y_pred, zero_division=0)
    cm = confusion_matrix(y_test, y_pred)

    print("[ml] accuracy:", acc)
    print("[ml] precision:", prec)
    print("[ml] recall:", rec)
    print("[ml] confusion_matrix:")
    print(cm)

    _MODELS_DIR.mkdir(parents=True, exist_ok=True)
    print("[ml] saving model")
    model_path, metadata_path, _ = save_model(
        clf,
        "token_scam_model",
        metrics={
            "accuracy": acc,
            "precision": prec,
            "recall": rec,
            "dataset_size": len(X),
        },
        feature_list=FEATURE_COLUMNS,
    )
    print("[ml] saved", model_path, metadata_path)

    print("[ml] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
