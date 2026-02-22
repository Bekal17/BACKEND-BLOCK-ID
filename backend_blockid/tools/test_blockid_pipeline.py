"""
Automatic pipeline test script for BlockID STEP 0-5.

Checks required inputs, runs each step via subprocess, verifies outputs,
validates CSVs, and prints a summary. Does not modify existing pipeline scripts.

Usage (from project root):
    py -m backend_blockid.tools.test_blockid_pipeline
    python -m backend_blockid.tools.test_blockid_pipeline

Works on Windows PowerShell and WSL/Linux.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd

# Paths
_TOOLS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _TOOLS_DIR.parent
_ROOT = _BACKEND_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
_MODELS_DIR = _BACKEND_DIR / "ml" / "models"

REQUIRED_INPUTS = [
    _DATA_DIR / "transactions.csv",
    _DATA_DIR / "scam_wallets.csv",
]

# Ensure project root is on path for imports
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _log(msg: str) -> None:
    print(f"[test_pipeline] {msg}")


def check_required_inputs() -> bool:
    """Check required input files exist. Return True if OK."""
    ok = True
    for p in REQUIRED_INPUTS:
        if not p.exists():
            _log(f"ERROR: required input missing: {p}")
            ok = False
    return ok


def ensure_wallets_csv() -> bool:
    """Create wallets.csv from transactions.csv if missing. Return True if OK."""
    wallets_csv = _DATA_DIR / "wallets.csv"
    if wallets_csv.exists():
        return True
    tx_csv = _DATA_DIR / "transactions.csv"
    if not tx_csv.exists():
        return False
    try:
        df = pd.read_csv(tx_csv)
        from_col = "from" if "from" in df.columns else df.columns[0]
        to_col = "to" if "to" in df.columns else (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        wallets = set()
        for col in [from_col, to_col]:
            wallets.update(df[col].dropna().astype(str).str.strip())
        wallets = sorted(w for w in wallets if w and w.lower() != "nan")
        if not wallets:
            _log("ERROR: no wallets in transactions.csv; cannot create wallets.csv")
            return False
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        out = pd.DataFrame({"wallet": wallets})
        out.to_csv(wallets_csv, index=False)
        _log(f"created wallets.csv with {len(wallets)} wallets from transactions.csv")
        return True
    except Exception as e:
        _log(f"ERROR: failed to create wallets.csv: {e}")
        return False


def ensure_cluster_features_for_train() -> bool:
    """Copy graph_cluster_features.csv to cluster_features.csv if needed for STEP 4."""
    src = _DATA_DIR / "graph_cluster_features.csv"
    dst = _DATA_DIR / "cluster_features.csv"
    if dst.exists():
        return True
    if not src.exists():
        return False
    try:
        shutil.copy2(src, dst)
        _log("copied graph_cluster_features.csv -> cluster_features.csv for STEP 4")
        return True
    except Exception as e:
        _log(f"ERROR: failed to copy cluster features: {e}")
        return False


def ensure_wallet_scores_for_train() -> bool:
    """Create minimal wallet_scores.csv from feature CSVs if missing. Return True if OK."""
    dst = _DATA_DIR / "wallet_scores.csv"
    if dst.exists():
        return True
    wallets = set()
    for name in ["graph_cluster_features.csv", "cluster_features.csv", "flow_features.csv", "drainer_features.csv"]:
        p = _DATA_DIR / name
        if p.exists():
            try:
                df = pd.read_csv(p)
                if "wallet" in df.columns:
                    wallets.update(df["wallet"].dropna().astype(str).str.strip())
            except Exception:
                pass
    wallets = sorted(w for w in wallets if w and w.lower() != "nan")
    if len(wallets) < 4:
        _log("ERROR: need wallet_scores.csv or enough wallets in feature CSVs (>=4) for training")
        return False
    # Mix labels so both classes exist (required by train_blockid_model)
    rows = []
    for i, w in enumerate(wallets):
        scam_prob = 1.0 if i % 2 == 0 else 0.0  # alternating for both classes
        risk = int(100 * scam_prob)
        rows.append({"wallet": w, "risk_score": risk, "scam_probability": scam_prob, "reason_code": "TEST"})
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(dst, index=False)
    _log(f"created minimal wallet_scores.csv with {len(rows)} wallets for STEP 4")
    return True


def run_step(step_num: int, module: str, *args: str) -> tuple[bool, str]:
    """Run a pipeline step. Return (success, message)."""
    cmd = [sys.executable, "-m", module] + list(args)
    _log(f"STEP {step_num}: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(_ROOT),
            capture_output=True,
            text=True,
            timeout=600,
        )
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()
        if result.returncode != 0:
            return False, f"exit code {result.returncode}\n{err}\n{out}"
        return True, "OK"
    except subprocess.TimeoutExpired:
        return False, "timeout (600s)"
    except Exception as e:
        return False, str(e)


def verify_file(path: Path, desc: str) -> bool:
    """Verify file exists and has content. Return True if OK."""
    if not path.exists():
        _log(f"  FAIL: {desc} not found: {path}")
        return False
    if path.stat().st_size == 0:
        _log(f"  FAIL: {desc} is empty: {path}")
        return False
    return True


def validate_csv(path: Path, require_wallet: bool = True, require_score: bool = False) -> tuple[bool, str]:
    """
    Validate CSV: row count > 0, no empty wallet column, optional score 0-100.
    Return (ok, message).
    """
    try:
        df = pd.read_csv(path)
        if df.empty:
            return False, "row count = 0"
        if require_wallet and "wallet" not in df.columns:
            return False, "missing 'wallet' column"
        if require_wallet and "wallet" in df.columns:
            empty = df["wallet"].isna() | (df["wallet"].astype(str).str.strip() == "")
            if empty.any():
                return False, f"empty wallet in {empty.sum()} row(s)"
        if require_score:
            if "risk_score" in df.columns:
                r = pd.to_numeric(df["risk_score"], errors="coerce")
                if r.isna().any() or (r < 0).any() or (r > 100).any():
                    return False, "risk_score must be 0-100"
            if "scam_probability" in df.columns:
                p = pd.to_numeric(df["scam_probability"], errors="coerce")
                if p.isna().any() or (p < 0).any() or (p > 1).any():
                    return False, "scam_probability must be 0-1"
        return True, f"rows={len(df)}"
    except Exception as e:
        return False, str(e)


def find_latest_model() -> Path | None:
    """Find latest blockid_model*.joblib. Return None if not found."""
    if not _MODELS_DIR.exists():
        return None
    candidates = list(_MODELS_DIR.glob("blockid_model*.joblib"))
    candidates = [p for p in candidates if "scaler" not in p.stem.lower()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def main() -> int:
    _log("BlockID pipeline test starting")
    _log(f"project root: {_ROOT}")

    if not check_required_inputs():
        _log("ABORT: required inputs missing")
        return 1

    results: list[tuple[int, bool, str]] = []

    # STEP 1: graph_clustering
    ok, msg = run_step(1, "backend_blockid.oracle.graph_clustering")
    if not ok:
        results.append((1, False, msg))
    else:
        out_path = _DATA_DIR / "graph_cluster_features.csv"
        v_ok = verify_file(out_path, "graph_cluster_features.csv")
        if v_ok:
            cv_ok, cv_msg = validate_csv(out_path, require_wallet=True, require_score=False)
            v_ok = v_ok and cv_ok
            if not cv_ok:
                msg = cv_msg
        results.append((1, v_ok, msg if not v_ok else "OK"))

    # Prepare for STEP 2, 3, 4
    if not ensure_wallets_csv():
        results.append((2, False, "wallets.csv missing and could not create"))
        results.append((3, False, "wallets.csv missing"))
    else:
        # STEP 2: flow_features (requires HELIUS_API_KEY / RPC)
        ok, msg = run_step(2, "backend_blockid.oracle.flow_features")
        if not ok:
            results.append((2, False, msg))
        else:
            out_path = _DATA_DIR / "flow_features.csv"
            v_ok = verify_file(out_path, "flow_features.csv")
            if v_ok:
                cv_ok, cv_msg = validate_csv(out_path, require_wallet=True, require_score=False)
                v_ok = v_ok and cv_ok
                if not cv_ok:
                    msg = cv_msg
            results.append((2, v_ok, msg if not v_ok else "OK"))

        # STEP 3: drainer_detection
        ok, msg = run_step(3, "backend_blockid.oracle.drainer_detection")
        if not ok:
            results.append((3, False, msg))
        else:
            out_path = _DATA_DIR / "drainer_features.csv"
            v_ok = verify_file(out_path, "drainer_features.csv")
            if v_ok:
                cv_ok, cv_msg = validate_csv(out_path, require_wallet=True, require_score=False)
                v_ok = v_ok and cv_ok
                if not cv_ok:
                    msg = cv_msg
            results.append((3, v_ok, msg if not v_ok else "OK"))

    # Prepare cluster_features and wallet_scores for STEP 4
    if not ensure_cluster_features_for_train():
        if 2 not in [r[0] for r in results] and 3 not in [r[0] for r in results]:
            results.append((4, False, "cluster_features.csv missing (copy from graph_cluster_features)"))
        else:
            results.append((4, False, "cluster_features.csv missing"))
    elif not ensure_wallet_scores_for_train():
        results.append((4, False, "wallet_scores.csv missing and could not create"))
    else:
        # STEP 4: train_blockid_model
        ok, msg = run_step(4, "backend_blockid.ml.train_blockid_model")
        if not ok:
            results.append((4, False, msg))
        else:
            model_path = find_latest_model()
            v_ok = model_path is not None
            if not v_ok:
                msg = "no blockid_model*.joblib in ml/models/"
            results.append((4, v_ok, msg if not v_ok else "OK"))

    # STEP 5: publish_scores --dry-run (only if STEP 4 passed)
    step4_ok = any(r[0] == 4 and r[1] for r in results)
    if not step4_ok:
        results.append((5, False, "skipped: STEP 4 failed"))
    else:
        ok, msg = run_step(5, "backend_blockid.oracle.publish_scores", "--dry-run")
        if not ok:
            results.append((5, False, msg))
        else:
            # Dry run does not write wallet_scores; validate wallet_scores if it exists
            ws_path = _DATA_DIR / "wallet_scores.csv"
            v_ok = True
            if ws_path.exists():
                cv_ok, cv_msg = validate_csv(ws_path, require_wallet=True, require_score=True)
                v_ok = cv_ok
                if not cv_ok:
                    msg = cv_msg
            results.append((5, v_ok, msg if not v_ok else "OK"))

    # Print summary
    print()
    print("PIPELINE TEST RESULT")
    print("-" * 40)
    for step_num, passed, msg in sorted(results, key=lambda x: x[0]):
        status = "PASS" if passed else "FAIL"
        print(f"STEP {step_num} {status}")
        if not passed and msg:
            for line in msg.strip().split("\n")[:5]:
                print(f"  {line}")
    total_pass = sum(1 for _, p, _ in results if p)
    total_steps = len(results)
    print("-" * 40)
    print(f"TOTAL {total_pass}/{total_steps} PASS")
    print()

    return 0 if total_pass == total_steps else 1


if __name__ == "__main__":
    raise SystemExit(main())
