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

import joblib
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
# Predict pipeline loads from backend_blockid/models/token_scam_model.joblib
_DEPLOY_MODEL_PATH = _SCRIPT_DIR.parent / "models" / "token_scam_model.joblib"
TOKEN_FEATURES_CSV = _DATA_DIR / "token_features.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

FEATURE_COLUMNS = [
    "mint_authority_exists",
    "freeze_authority_exists",
    "metadata_missing",
    "decimals",
    "supply",
    "is_mutable",
    "is_compressed",
    "has_unverified_creator",
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

    if "wallet" not in df.columns:
        print("[ml] ERROR: token_features.csv must have a wallet column for wallet-level aggregation")
        return 1

    # Labels: scam_flag from CSV or any creator in scam_wallets → scam=1 (per token row)
    labels = []
    for _, row in df.iterrows():
        scam_flag = _bool_to_int(row.get("scam_flag"))
        creator_wallets = _parse_creator_wallets(str(row.get("creator_wallets") or ""))
        creator_in_scam = 1 if any(c in scam_wallets for c in creator_wallets) else 0
        label = 1 if (scam_flag or creator_in_scam) else 0
        labels.append(label)
    df["_label"] = labels

    # Per-row features (same semantics as predict_wallet_score token dicts)
    if "mint_authority_exists" in df.columns:
        df["_mint"] = df["mint_authority_exists"].map(_bool_to_int)
    else:
        df["_mint"] = (
            (df["mint_authority"].fillna("").astype(str).str.strip() != "").astype(int)
        )
    if "freeze_authority_exists" in df.columns:
        df["_freeze"] = df["freeze_authority_exists"].map(_bool_to_int)
    else:
        df["_freeze"] = (
            (df["freeze_authority"].fillna("").astype(str).str.strip() != "").astype(int)
        )
    df["_meta"] = df["metadata_missing"].map(_bool_to_int)
    df["_dec"] = pd.to_numeric(df["decimals"], errors="coerce").fillna(0)
    df["_supply"] = pd.to_numeric(df["supply"], errors="coerce").fillna(0).astype(np.float64)
    df["_mut"] = pd.to_numeric(
        df.get("is_mutable", pd.Series([0] * len(df))), errors="coerce"
    ).fillna(0).astype(int)
    df["_cmp"] = pd.to_numeric(
        df.get("is_compressed", pd.Series([0] * len(df))), errors="coerce"
    ).fillna(0).astype(int)
    df["_unv"] = pd.to_numeric(
        df.get("has_unverified_creator", pd.Series([0] * len(df))), errors="coerce"
    ).fillna(0).astype(int)

    # Wallet-level aggregation — must match predict_wallet_score._feature_vector_from_tokens
    def _mut_ratio(s: pd.Series) -> float:
        return float((s.astype(int) == 1).sum()) / max(1, len(s))

    def _cmp_ratio(s: pd.Series) -> float:
        return float((s.astype(int) == 1).sum()) / max(1, len(s))

    wdf = df.groupby("wallet", sort=False).agg(
        mint_authority_exists=pd.NamedAgg(column="_mint", aggfunc="max"),
        freeze_authority_exists=pd.NamedAgg(column="_freeze", aggfunc="max"),
        metadata_missing=pd.NamedAgg(column="_meta", aggfunc="max"),
        decimals=pd.NamedAgg(column="_dec", aggfunc="mean"),
        supply=pd.NamedAgg(column="_supply", aggfunc="mean"),
        is_mutable=pd.NamedAgg(column="_mut", aggfunc=_mut_ratio),
        is_compressed=pd.NamedAgg(column="_cmp", aggfunc=_cmp_ratio),
        has_unverified_creator=pd.NamedAgg(column="_unv", aggfunc="max"),
        _label=pd.NamedAgg(column="_label", aggfunc="max"),
    )
    X = wdf[FEATURE_COLUMNS]
    y = wdf["_label"].to_numpy(dtype=int)

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
    clf = RandomForestClassifier(n_estimators=N_ESTIMATORS, random_state=RANDOM_STATE, )
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

    _DEPLOY_MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(clf, _DEPLOY_MODEL_PATH)
    print("[ml] deployed for inference:", _DEPLOY_MODEL_PATH)

    print("[ml] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
