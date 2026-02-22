"""
BlockID full pipeline — run all steps in order with logging and file checks.

Steps (in order):
  1. backend_blockid.oracle.graph_clustering
  2. backend_blockid.oracle.flow_features
  3. backend_blockid.oracle.drainer_detection
  4. backend_blockid.oracle.auto_evidence_collector
  5. backend_blockid.oracle.reason_aggregator
  6. backend_blockid.oracle.reason_weight_engine
  7. backend_blockid.ml.predict_wallet_score
  8. backend_blockid.oracle.batch_publish

Requirements:
  - Print START / OK / FAIL per step
  - Stop pipeline if any step fails
  - Print final summary
  - Use subprocess to call python -m module
  - Log project root path
  - Catch exceptions and print stacktrace
  - Print which dataset is used

Usage:
  py -m backend_blockid.tools.run_full_pipeline
"""

from __future__ import annotations

import subprocess
import sys
import traceback
from pathlib import Path

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.env import (
    get_devnet_dummy_dir,
    load_blockid_env,
    use_devnet_dummy_data,
)

logger = get_logger(__name__)

_TOOLS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _TOOLS_DIR.parent
_ROOT = _BACKEND_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
_MODELS_DIR = _BACKEND_DIR / "models"

# Dataset paths (for reporting)
WALLET_SOURCES = [
    _DATA_DIR / "wallets.csv",
    _DATA_DIR / "test_wallets.csv",
    _DATA_DIR / "manual_wallets.csv",
    _DATA_DIR / "test_wallets_100.csv",
]

CLUSTER_FEATURES_CSV = _DATA_DIR / "cluster_features.csv"

# Files to check before each step (missing = stop pipeline)
REQUIRED_FILES: list[Path] = [
    _DATA_DIR / "test_wallets.csv",
    _DATA_DIR / "cluster_features.csv",
    _DATA_DIR / "reason_codes.csv",
    _DATA_DIR / "reason_penalties.csv",
    _DATA_DIR / "wallet_scores.csv",
]

STEPS: list[tuple[str, str, str]] = [
    ("graph_clustering", "backend_blockid.oracle.graph_clustering", "graph_cluster_features.csv"),
    ("flow_features", "backend_blockid.oracle.flow_features", "flow_features.csv"),
    ("drainer_detection", "backend_blockid.oracle.drainer_detection", "drainer_features.csv"),
    ("auto_evidence_collector", "backend_blockid.oracle.auto_evidence_collector", ""),
    ("reason_aggregator", "backend_blockid.oracle.reason_aggregator", "reason_codes.csv"),
    ("reason_weight_engine", "backend_blockid.oracle.reason_weight_engine", "reason_penalties.csv"),
    ("predict_wallet_score", "backend_blockid.ml.predict_wallet_score", "wallet_scores.csv"),
    ("batch_publish", "backend_blockid.oracle.batch_publish", ""),
]


_SEP = "=" * 60
_SEP_THIN = "-" * 60


def _log(msg: str) -> None:
    print(f"[run_full_pipeline] {msg}")


def ensure_cluster_features() -> bool:
    """If cluster_features.csv is missing, run build_cluster_features. Return True if OK."""
    if CLUSTER_FEATURES_CSV.exists():
        return True
    _log("cluster_features.csv missing; running build_cluster_features...")
    try:
        from backend_blockid.tools.build_cluster_features import build_cluster_features
        ok = build_cluster_features()
        if ok:
            logger.info("cluster_features_generated")
            _log("cluster_features.csv generated.")
        return ok
    except Exception as e:
        _log(f"build_cluster_features failed: {e}")
        logger.exception("cluster_features_build_failed", error=str(e))
        return False


def _print_dataset_info() -> None:
    """Print which dataset is used (dummy vs live, key files)."""
    load_blockid_env()
    use_dummy = use_devnet_dummy_data()

    _log(_SEP_THIN)
    _log("Dataset:")
    if use_dummy:
        dummy_dir = get_devnet_dummy_dir()
        _log(f"  mode: devnet_dummy (BLOCKID_USE_DUMMY_DATA=1)")
        _log(f"  dummy_dir: {dummy_dir}")
        if dummy_dir.exists():
            for name in ("transactions.csv", "wallets.csv", "flow_features.csv"):
                p = dummy_dir / name
                _log(f"  {name}: {'exists' if p.exists() else 'missing'}")
    else:
        _log("  mode: live (Helius/RPC)")

    _log("  wallet sources:")
    for p in WALLET_SOURCES:
        status = "exists" if p.exists() else "missing"
        _log(f"    {p.name}: {status}")

    tx_path = _DATA_DIR / "transactions.csv"
    _log(f"  transactions.csv: {'exists' if tx_path.exists() else 'missing'}")
    _log(_SEP_THIN)


def check_required_files() -> bool:
    """Check required files exist before pipeline. Return True if OK; print ERROR and return False if any missing."""
    missing: list[Path] = [p for p in REQUIRED_FILES if not p.exists()]
    if not missing:
        return True
    _log(_SEP_THIN)
    _log("ERROR: required file(s) missing")
    _log(_SEP_THIN)
    for p in missing:
        _log(f"  MISSING: {p}")
    _log("")
    _log("Pipeline stopped. Create missing files and try again.")
    _log(_SEP_THIN)
    logger.error("run_full_pipeline_missing_files", paths=[str(p) for p in missing])
    return False


def run_step(step_name: str, module: str, *args: str) -> tuple[bool, str]:
    """Run a pipeline step via subprocess. Return (success, message)."""
    cmd = [sys.executable, "-m", module] + list(args)
    _log(f"START: {step_name}")
    _log(f"  $ {' '.join(cmd)}")
    logger.info("pipeline_step_start", step=step_name, module=module)

    try:
        result = subprocess.run(
            cmd,
            cwd=str(_ROOT),
            capture_output=True,
            text=True,
            timeout=900,
        )
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()

        if result.returncode != 0:
            msg = f"exit code {result.returncode}"
            if err:
                msg += f"\n{err}"
            if out:
                msg += f"\n{out}"
            _log(f"FAIL: {step_name}")
            logger.error("pipeline_step_failed", step=step_name, returncode=result.returncode)
            return False, msg

        _log(f"OK: {step_name}")
        logger.info("pipeline_step_ok", step=step_name)
        return True, "OK"

    except subprocess.TimeoutExpired as e:
        _log(f"FAIL: {step_name}")
        logger.error("pipeline_step_timeout", step=step_name, timeout=900)
        return False, f"timeout (900s): {e}"

    except Exception as e:
        _log(f"FAIL: {step_name}")
        tb = traceback.format_exc()
        print(tb, file=sys.stderr)
        logger.exception("pipeline_step_error", step=step_name, error=str(e))
        return False, f"{e}\n{tb}"


def main() -> int:
    load_blockid_env()
    _log(_SEP)
    _log("BlockID full pipeline")
    _log(_SEP)
    _log(f"project root: {_ROOT}")
    _log("")
    _print_dataset_info()
    _log("")
    _log("Required files check:")
    logger.info("run_full_pipeline_start")

    if not ensure_cluster_features():
        _log("ERROR: Could not generate cluster_features.csv. Ensure transactions.csv exists.")
        return 1

    if not check_required_files():
        return 1

    _log("All required files present.")
    _log("")
    results: list[tuple[str, bool, str]] = []

    for i, (step_name, module, _) in enumerate(STEPS, start=1):
        _log(_SEP_THIN)
        _log(f"Step {i}/{len(STEPS)}: {step_name}")
        _log(_SEP_THIN)
        ok, msg = run_step(step_name, module)
        if ok:
            results.append((step_name, True, "OK"))
        else:
            results.append((step_name, False, msg))
            _log(f"  {msg}")
            _log("")
            _log("Pipeline stopped on first failure.")
            break
        _log("")

    # Verification step (post-run)
    try:
        from backend_blockid.tools.verify_pipeline_output import verify_pipeline_output
        if not verify_pipeline_output():
            _log("[verify] WARNING: pipeline verification failed")
    except Exception as e:
        _log(f"[verify] WARNING: verification error: {e}")

    # Final summary
    _log("")
    _log(_SEP)
    _log("SUMMARY")
    _log(_SEP)
    for step_name, ok, detail in results:
        status = "OK" if ok else "FAIL"
        _log(f"  {status}: {step_name}")
        if not ok and detail and detail != "OK":
            for line in detail.split("\n")[:8]:
                _log(f"    {line}")
    _log(_SEP)

    failed = sum(1 for _, ok, _ in results if not ok)
    if failed > 0:
        _log(f"RESULT: FAILED ({failed} step(s))")
        logger.warning("run_full_pipeline_failed", failed_steps=failed, steps=[r[0] for r in results])
        return 1

    _log("RESULT: PASS (all steps completed)")
    logger.info("run_full_pipeline_done", steps=len(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
