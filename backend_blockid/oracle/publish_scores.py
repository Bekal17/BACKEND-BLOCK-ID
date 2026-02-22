"""
STEP 5: Publish ML risk scores to BlockID Anchor PDA (TrustScoreAccount).

Loads blockid_model.joblib and feature list, reads wallet features CSV (or builds from
cluster/flow/drainer CSVs), predicts scam probability, converts to trust score 0–100,
and publishes each score to the on-chain PDA via update_trust_score.

Env: SOLANA_RPC_URL, ORACLE_PRIVATE_KEY, ORACLE_PROGRAM_ID.
Batch mode: --batch reads wallets from backend_blockid/data/wallets.csv.

Usage:
  py backend_blockid/oracle/publish_scores.py [--batch] [--features PATH] [--dry-run]
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Any

# Run from project root so backend_blockid is importable
if __name__ == "__main__" and __package__ is None:
    _root = Path(__file__).resolve().parents[2]
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import joblib
import numpy as np
import pandas as pd

from solders.pubkey import Pubkey

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.env import get_oracle_program_id, load_blockid_env, print_blockid_startup
from backend_blockid.oracle.solana_publisher import (
    SYS_PROGRAM_ID_STR,
    _default_rpc_url,
    _load_keypair,
    _score_to_risk_level,
    build_update_trust_score_instruction,
    get_trust_score_pda,
)

logger = get_logger(__name__)

_ORACLE_DIR = Path(__file__).resolve().parent
_DATA_DIR = _ORACLE_DIR.parent / "data"
_ML_DIR = _ORACLE_DIR.parent / "ml"
_MODELS_DIR = _ML_DIR / "models"

MODEL_PATH = _MODELS_DIR / "blockid_model.joblib"
FEATURE_LIST_PATH = _MODELS_DIR / "feature_list.txt"
DEFAULT_FEATURES_CSV = _DATA_DIR / "wallet_features.csv"
BATCH_WALLETS_CSV = _DATA_DIR / "wallets.csv"

CLUSTER_FEATURES_CSV = _DATA_DIR / "cluster_features.csv"
FLOW_FEATURES_CSV = _DATA_DIR / "flow_features.csv"
DRAINER_FEATURES_CSV = _DATA_DIR / "drainer_features.csv"

DEFAULT_RETRY_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SEC = 2.0


def _log(msg: str, **kwargs: Any) -> None:
    parts = [f"[publish_scores] {msg}"]
    for k, v in kwargs.items():
        parts.append(f" {k}={v}")
    print("".join(parts))
    logger.info(msg, extra=kwargs)


def load_model_and_feature_list():
    """Load RandomForest model and ordered feature names. Raises FileNotFoundError if missing."""
    if not MODEL_PATH.is_file():
        raise FileNotFoundError(f"Model not found: {MODEL_PATH}. Run train_blockid_model.py first.")
    if not FEATURE_LIST_PATH.is_file():
        raise FileNotFoundError(f"Feature list not found: {FEATURE_LIST_PATH}. Run train_blockid_model.py first.")
    model = joblib.load(MODEL_PATH)
    with open(FEATURE_LIST_PATH, encoding="utf-8") as f:
        feature_list = [line.strip() for line in f if line.strip()]
    return model, feature_list


def load_wallet_features_csv(path: Path) -> pd.DataFrame:
    """Load CSV with wallet column and feature columns. Fill missing numeric with 0."""
    if not path.is_file():
        return pd.DataFrame()
    df = pd.read_csv(path)
    for c in df.columns:
        if c == "wallet":
            continue
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    return df


def build_features_from_merge(wallet_list: list[str]) -> pd.DataFrame:
    """
    Build feature matrix from cluster/flow/drainer CSVs merged on wallet.
    Wallets not present get all 0 features. Returns DataFrame with wallet + feature columns.
    """
    dfs = []
    for name, p in [
        ("cluster", CLUSTER_FEATURES_CSV),
        ("flow", FLOW_FEATURES_CSV),
        ("drainer", DRAINER_FEATURES_CSV),
    ]:
        if p.is_file():
            try:
                dfs.append(pd.read_csv(p))
            except Exception as e:
                _log("skip_csv", path=str(p), error=str(e))
    if not dfs:
        # No feature CSVs: return one row per wallet with no feature columns (will use feature_list from model)
        return pd.DataFrame({"wallet": wallet_list})
    base = dfs[0].copy()
    for d in dfs[1:]:
        if "wallet" in d.columns:
            join_cols = [c for c in d.columns if c != "wallet"]
            if join_cols:
                base = base.merge(d[["wallet"] + join_cols], on="wallet", how="outer")
    for c in base.columns:
        if c == "wallet":
            continue
        base[c] = pd.to_numeric(base[c], errors="coerce").fillna(0)
    # Ensure all requested wallets have a row
    base_wallets = set(base["wallet"].astype(str).str.strip())
    missing = [w for w in wallet_list if w not in base_wallets]
    if missing:
        zero_row = {c: 0 for c in base.columns if c != "wallet"}
        for w in missing:
            base = pd.concat([base, pd.DataFrame([{"wallet": w, **zero_row}])], ignore_index=True)
    return base


def predict_trust_scores(
    model: Any,
    feature_list: list[str],
    df: pd.DataFrame,
) -> list[tuple[str, float, float]]:
    """
    For each row in df, build X in feature_list order, predict scam probability, convert to trust score.
    Returns list of (wallet, trust_score_0_100, scam_probability).
    """
    results = []
    for _, row in df.iterrows():
        wallet = str(row.get("wallet", "")).strip()
        if not wallet:
            continue
        X = np.zeros((1, len(feature_list)), dtype=np.float64)
        for i, name in enumerate(feature_list):
            if name in row.index:
                try:
                    X[0, i] = float(row[name])
                except (TypeError, ValueError):
                    pass
        proba = model.predict_proba(X)[0]
        # Assume class 1 is scam (index 1 if binary)
        if proba.ndim == 0:
            scam_prob = float(proba)
        else:
            scam_prob = float(proba[1]) if len(proba) > 1 else float(proba[0])
        trust_score = max(0.0, min(100.0, (1.0 - scam_prob) * 100.0))
        results.append((wallet, round(trust_score, 2), scam_prob))
    return results


def send_update_trust_score(
    rpc_url: str,
    keypair: Any,
    program_id: Any,
    wallet_pubkey: Any,
    trust_score_u8: int,
    risk_level_u8: int,
    sys_program_id: Any,
) -> str | None:
    """Build and send update_trust_score transaction. Returns signature or None on failure."""
    from solders.pubkey import Pubkey
    from solana.rpc.api import Client
    from solana.transaction import Transaction

    oracle_pubkey = keypair.pubkey()
    ix, _ = build_update_trust_score_instruction(
        program_id,
        oracle_pubkey,
        wallet_pubkey,
        trust_score_u8,
        risk_level_u8,
        sys_program_id,
    )
    client = Client(rpc_url)
    for attempt in range(DEFAULT_RETRY_ATTEMPTS):
        try:
            resp = client.get_latest_blockhash()
            blockhash_val = getattr(resp, "value", None) or (
                getattr(resp.result, "value", None) if hasattr(resp, "result") else None
            )
            if not blockhash_val:
                raise RuntimeError("No blockhash")
            blockhash = getattr(blockhash_val, "blockhash", blockhash_val)
            tx = Transaction(recent_blockhash=blockhash, fee_payer=oracle_pubkey)
            tx.add(ix)
            result = client.send_transaction(tx, keypair)
            sig_val = getattr(result, "value", None) or (
                getattr(result.result, "value", None) if hasattr(result, "result") else None
            )
            if sig_val:
                return str(sig_val)
            err = getattr(result, "error", None) or getattr(result, "value", result)
            raise RuntimeError(str(err))
        except Exception as e:
            _log("tx_failed", attempt=attempt + 1, error=str(e))
            if attempt < DEFAULT_RETRY_ATTEMPTS - 1:
                time.sleep(DEFAULT_RETRY_BACKOFF_SEC * (attempt + 1))
    return None


def main() -> int:
    load_blockid_env()
    print_blockid_startup("publish_scores")

    parser = argparse.ArgumentParser(description="Publish ML trust scores to BlockID PDA.")
    parser.add_argument("--batch", action="store_true", help=f"Read wallet list from {BATCH_WALLETS_CSV.name}")
    parser.add_argument("--features", type=Path, default=None, help="Wallet features CSV (wallet + feature columns). Default: wallet_features.csv or build from cluster/flow/drainer.")
    parser.add_argument("--dry-run", action="store_true", help="Predict and log only; do not send transactions.")
    parser.add_argument("wallet", nargs="?", help="Single wallet to publish (ignored if --batch).")
    args = parser.parse_args()

    try:
        model, feature_list = load_model_and_feature_list()
    except FileNotFoundError as e:
        _log("ERROR", message=str(e))
        return 1

    _log("model_loaded", model=str(MODEL_PATH), n_features=len(feature_list))

    # Resolve wallet list and feature DataFrame
    features_path = args.features or Path(os.getenv("WALLET_FEATURES_CSV", "")).strip() or DEFAULT_FEATURES_CSV
    if args.batch:
        if not BATCH_WALLETS_CSV.is_file():
            _log("ERROR", message=f"Batch mode requires {BATCH_WALLETS_CSV}")
            return 1
        wallet_list = pd.read_csv(BATCH_WALLETS_CSV)["wallet"].astype(str).str.strip().tolist()
        wallet_list = [w for w in wallet_list if w]
        if not wallet_list:
            _log("ERROR", message="wallets.csv is empty or has no wallet column")
            return 1
        _log("batch_mode", n_wallets=len(wallet_list), path=str(BATCH_WALLETS_CSV))
        df = load_wallet_features_csv(features_path)
        if df.empty:
            df = build_features_from_merge(wallet_list)
            # Restrict to batch wallets
            df = df[df["wallet"].astype(str).str.strip().isin(wallet_list)]
        else:
            df = df[df["wallet"].astype(str).str.strip().isin(wallet_list)]
    else:
        single = (args.wallet or os.getenv("WALLET", "")).strip()
        if not single:
            _log("ERROR", message="Provide wallet as argument or set WALLET env, or use --batch")
            return 1
        wallet_list = [single]
        df = load_wallet_features_csv(features_path)
        if df.empty:
            df = build_features_from_merge(wallet_list)
        else:
            df = df[df["wallet"].astype(str).str.strip() == single]
        if df.empty:
            df = build_features_from_merge(wallet_list)

    if df.empty:
        _log("ERROR", message="No rows to score")
        return 1

    predictions = predict_trust_scores(model, feature_list, df)
    if not predictions:
        _log("ERROR", message="No predictions produced")
        return 1

    rpc_url = _default_rpc_url()
    keypair = None
    program_id = None
    sys_program_id = None
    if not args.dry_run:
        key_str = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
        prog_str = get_oracle_program_id()
        if not key_str:
            _log("ERROR", message="ORACLE_PRIVATE_KEY required when not --dry-run")
            return 1
        keypair = _load_keypair(key_str)
        from solders.pubkey import Pubkey
        program_id = Pubkey.from_string(prog_str)
        sys_program_id = Pubkey.from_string(SYS_PROGRAM_ID_STR)

    for wallet, trust_score, scam_prob in predictions:
        trust_u8 = max(0, min(100, int(round(trust_score))))
        risk_u8 = _score_to_risk_level(trust_score)
        if args.dry_run:
            _log("wallet", wallet=wallet, score=trust_score, scam_probability=round(scam_prob, 4), tx_signature="dry_run")
            print(f"wallet={wallet} score={trust_score} transaction_signature=dry_run")
            continue
        try:
            wallet_pubkey = Pubkey.from_string(wallet)
        except Exception as e:
            _log("skip_invalid_wallet", wallet=wallet[:16] + "...", error=str(e))
            continue
        sig = send_update_trust_score(
            rpc_url, keypair, program_id, wallet_pubkey, trust_u8, risk_u8, sys_program_id
        )
        if sig:
            _log("wallet", wallet=wallet, score=trust_score, transaction_signature=sig)
            print(f"wallet={wallet} score={trust_score} transaction_signature={sig}")
        else:
            _log("wallet", wallet=wallet, score=trust_score, transaction_signature="failed")
            print(f"wallet={wallet} score={trust_score} transaction_signature=failed")

    _log("done", published=len(predictions))
    return 0


if __name__ == "__main__":
    sys.exit(main())
