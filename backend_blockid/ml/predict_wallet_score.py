"""
Predict trust/risk score for wallets using trained token_scam_model.joblib.

Merge ML predictions with reason penalties: final_score = ml_score - penalty (clamped 0–100).
Output: wallet_scores.csv with wallet, ml_score, penalty, final_score, risk_level.

Usage:
    py backend_blockid/ml/predict_wallet_score.py

Reads wallets from cluster_features.csv (valid Solana addresses only); loads reason_penalties.csv when present.
"""

from __future__ import annotations

import csv
import json
import sqlite3
from pathlib import Path

import joblib
import numpy as np

from backend_blockid.blockid_logging import get_logger
from backend_blockid.ml.predictor import score_to_risk_label
from backend_blockid.utils.wallet_utils import is_valid_wallet
from backend_blockid.database.config import DB_PATH
from backend_blockid.database.repositories import save_wallet_scores_from_csv

logger = get_logger(__name__)

# Paths: script in backend_blockid/ml/, data and models in backend_blockid/
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
_MODELS_DIR = _SCRIPT_DIR.parent / "models"
CLUSTER_FEATURES_CSV = _DATA_DIR / "cluster_features.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"
REASON_PENALTIES_CSV = _DATA_DIR / "reason_penalties.csv"
REASON_CODES_CSV = _DATA_DIR / "reason_codes.csv"
WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"
MODEL_PATH = _MODELS_DIR / "token_scam_model.joblib"

# Feature order must match train_token_scam_model.py
FEATURE_ORDER = [
    "mint_authority_exists",
    "freeze_authority_exists",
    "metadata_missing",
    "decimals",
    "supply",
]

def _load_valid_wallets_from_cluster(path: Path) -> list[str]:
    """Load valid Solana wallet addresses from cluster_features.csv."""
    out: list[str] = []
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            w = r.get("wallet", "").strip()
            if w and is_valid_wallet(w):
                out.append(w)
    return out


def _load_scam_wallets(path: Path) -> set[str]:
    """Load scam wallet addresses from CSV (first column)."""
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


def _get_token_history_mock(wallet: str) -> list[dict]:
    """
    Mock token history for a wallet. Returns list of feature dicts per token.
    Replace with Helius (e.g. getAssetsByOwner or creator-based) later.
    """
    # One synthetic token per wallet: no red flags (all zeros)
    return [
        {
            "mint_authority_exists": 0,
            "freeze_authority_exists": 0,
            "metadata_missing": 0,
            "decimals": 9,
            "supply": 0,
        }
    ]


def _feature_vector_from_tokens(tokens: list[dict]) -> np.ndarray:
    """Aggregate token features into one vector (max of binary flags, mean of numeric)."""
    if not tokens:
        return np.array([[0, 0, 0, 0, 0]], dtype=np.float64)
    mint_max = max(t.get("mint_authority_exists", 0) for t in tokens)
    freeze_max = max(t.get("freeze_authority_exists", 0) for t in tokens)
    meta_max = max(t.get("metadata_missing", 0) for t in tokens)
    dec_mean = np.mean([t.get("decimals", 0) for t in tokens])
    supply_mean = np.mean([t.get("supply", 0) for t in tokens])
    return np.array([[mint_max, freeze_max, meta_max, dec_mean, supply_mean]], dtype=np.float64)


def _load_reason_penalties(path: Path) -> dict[str, int]:
    """Load reason_penalties.csv; return {wallet: penalty_score}. Missing wallets = 0."""
    out: dict[str, int] = {}
    if not path.exists():
        logger.debug("predict_wallet_score_penalties_missing", path=str(path))
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        penalty_col = "penalty_score"
        wallet_col = "wallet"
        for row in reader:
            w = (row.get(wallet_col) or "").strip()
            if not w:
                continue
            try:
                p = int(row.get(penalty_col, 0) or 0)
            except (TypeError, ValueError):
                p = 0
            out[w] = max(0, p)
    logger.info("predict_wallet_score_penalties_loaded", path=str(path), wallets=len(out))
    return out


def _load_reason_codes(path: Path) -> dict[str, tuple[list[str], list[str]]]:
    out: dict[str, tuple[list[str], list[str]]] = {}
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            w = r.get("wallet", "").strip()
            if not w:
                continue
            try:
                codes = json.loads(r.get("reason_codes", "[]"))
            except Exception:
                codes = []
            texts = [c.replace("_", " ").title() for c in codes]
            out[w] = (codes, texts)
    return out


def main() -> int:
    print("[predict_wallet_score] loading wallets from", CLUSTER_FEATURES_CSV)
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not CLUSTER_FEATURES_CSV.exists():
        print("[predict_wallet_score] ERROR: cluster_features.csv not found:", CLUSTER_FEATURES_CSV)
        return 1

    wallets = _load_valid_wallets_from_cluster(CLUSTER_FEATURES_CSV)
    print("[predict_wallet_score] valid wallets:", len(wallets))
    print("Sample wallets:", wallets[:10])
    print("Wallet length check:", [(w, len(w)) for w in wallets[:10]])
    scam_wallets = _load_scam_wallets(SCAM_WALLETS_CSV)
    penalty_map = _load_reason_penalties(REASON_PENALTIES_CSV)
    reason_map = _load_reason_codes(REASON_CODES_CSV)

    if not MODEL_PATH.exists():
        print("[predict_wallet_score] ERROR: model not found:", MODEL_PATH)
        return 1

    print("[predict_wallet_score] loading model", MODEL_PATH)
    model = joblib.load(MODEL_PATH)

    rows: list[dict] = []
    for wallet in wallets:
        if not is_valid_wallet(wallet):
            print("[predict_wallet_score] skip invalid wallet:", wallet)
            continue
        logger.debug("predict_wallet_score_processing", wallet=wallet[:16] + "...")
        tokens = _get_token_history_mock(wallet)
        X = _feature_vector_from_tokens(tokens)

        # Known scam creator → override probability to 1.0
        if wallet in scam_wallets:
            scam_prob = 1.0
        else:
            scam_prob = float(model.predict_proba(X)[0, 1])

        risk_score = round(scam_prob * 100)
        # ml_score: trust score 0–100 (higher = safer)
        ml_score = 100 - risk_score
        penalty = penalty_map.get(wallet, 0)
        final_score = max(0, min(100, ml_score - penalty))
        risk_level = score_to_risk_label(final_score)

        from backend_blockid.ai_engine.positive_reasons import default_positive_reason
        from backend_blockid.database.repositories import insert_wallet_reason

        reason_codes, reason_text = reason_map.get(wallet, ([], []))
        if not reason_codes:
            positive = default_positive_reason()
            try:
                insert_wallet_reason(
                    wallet,
                    positive["code"],
                    positive["weight"],
                    confidence=positive["confidence"],
                    tx_hash=None,
                    tx_link=None,
                )
                logger.info(
                    "positive_reason_added_to_ml",
                    wallet=wallet,
                    reason=positive["code"],
                )
            except Exception:
                logger.exception("positive_reason_insert_failed", wallet=wallet[:16] + "...")

            final_score = min(100, max(0, final_score + positive["weight"]))
            risk_level = score_to_risk_label(final_score)
            reason_codes = [positive["code"]]
            reason_text = ["No suspicious behavior detected."]

        rows.append({
            "wallet": wallet,
            "ml_score": ml_score,
            "penalty": penalty,
            "final_score": final_score,
            "risk_level": risk_level,
            "reason_codes": json.dumps(reason_codes),
            "reason_text": json.dumps(reason_text),
        })

    if len(rows) == 0:
        print("[predict_wallet_score] No valid wallets → skip writing wallet_scores.csv")
        return 0

    with open(WALLET_SCORES_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "wallet",
                "ml_score",
                "penalty",
                "final_score",
                "risk_level",
                "reason_codes",
                "reason_text",
            ],
        )
        w.writeheader()
        w.writerows(rows)

    logger.info("predict_wallet_score_done", path=str(WALLET_SCORES_CSV), rows=len(rows))
    print("[predict_wallet_score] wrote", len(rows), "rows to", WALLET_SCORES_CSV)

    try:
        inserted = save_wallet_scores_from_csv(str(WALLET_SCORES_CSV))
        print("[predict_wallet_score] saved to DB:", inserted, "rows")
    except Exception as e:
        logger.exception("predict_wallet_score_save_db_error", path=str(WALLET_SCORES_CSV), error=str(e))
        print("[predict_wallet_score] ERROR saving to DB:", e)

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT wallet FROM wallet_reasons WHERE wallet IS NOT NULL")
        reason_wallets = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT wallet FROM trust_scores WHERE wallet IS NOT NULL")
        scored_wallets = {row[0] for row in cur.fetchall()}
        missing_wallets = sorted(reason_wallets - scored_wallets)
        inserted_missing = 0
        for wallet in missing_wallets:
            cur.execute(
                "INSERT OR IGNORE INTO trust_scores(wallet, score) VALUES (?, ?)",
                (wallet, 100),
            )
            inserted_missing += 1
        conn.commit()
        conn.close()
        if inserted_missing:
            logger.info(
                "predict_wallet_score_missing_trust_scores_added",
                count=inserted_missing,
            )
    except Exception as e:
        logger.exception("predict_wallet_score_missing_scores_error", error=str(e))

    print("[predict_wallet_score] done")
    return 0

    # after scoring wallets
    insert missing wallets with score 100

    
if __name__ == "__main__":
    raise SystemExit(main())
