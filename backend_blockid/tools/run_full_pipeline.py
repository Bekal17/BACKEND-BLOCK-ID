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

import asyncio
import csv
import io
import os
import subprocess
import sys
import time
import traceback
from pathlib import Path

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
except Exception:
    pass

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.env import (
    get_devnet_dummy_dir,
    load_blockid_env,
    use_devnet_dummy_data,
)
from backend_blockid.database.repositories import get_all_active_clusters
from backend_blockid.oracle.incremental_wallet_meta_scanner import scan_wallet, scan_cluster
from backend_blockid.ai_engine.priority_wallets import (
    age_priorities,
    boost_active_wallets,
    get_wallets_with_budget,
    populate_priority_wallets,
    remove_old_wallets,
)
from backend_blockid.ai_engine.dynamic_risk_v2 import update_wallet_score_async
from backend_blockid.tools.blockid_logger import log_event
from backend_blockid.tools.telegram_alert import send_pipeline_summary
from backend_blockid.api_server.monitoring_api import record_pipeline_run_async
from backend_blockid.api_server.metrics import record_pipeline_run as metrics_record_pipeline_run

logger = get_logger(__name__)

BLOCKID_TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
print(f"[run_full_pipeline] TEST_MODE = {BLOCKID_TEST_MODE}")

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
    ("build_wallet_graph", "backend_blockid.tools.build_wallet_graph", "wallet_graph_clusters"),
    ("propagation_engine_v1", "backend_blockid.tools.propagation_engine_v1", ""),
    ("flow_features", "backend_blockid.oracle.flow_features", "flow_features.csv"),
    ("drainer_detection", "backend_blockid.oracle.drainer_detection", "drainer_features.csv"),
    ("auto_evidence_collector", "backend_blockid.oracle.auto_evidence_collector", ""),
    ("reason_aggregator", "backend_blockid.oracle.reason_aggregator", "reason_codes.csv"),
    ("reason_weight_engine", "backend_blockid.oracle.reason_weight_engine", "reason_penalties.csv"),
    ("predict_wallet_score", "backend_blockid.ml.predict_wallet_score", "wallet_scores.csv"),
    ("aggregate_reason_codes", "backend_blockid.tools.aggregate_reason_codes", ""),
    ("save_score_history", "backend_blockid.tools.save_score_history", ""),
    ("batch_publish", "backend_blockid.oracle.batch_publish", ""),
]


_SEP = "=" * 60
_SEP_THIN = "-" * 60


def _log(msg: str) -> None:
    print(f"[run_full_pipeline] {msg}")


def should_skip_step(step_name: str) -> bool:
    if BLOCKID_TEST_MODE and step_name in ["batch_publish"]:
        print(f"[run_full_pipeline] SKIP {step_name} (TEST_MODE)")
        return True
    return False


def test_wallet_limit() -> None:
    if BLOCKID_TEST_MODE:
        print("[run_full_pipeline] Using TEST wallets only")
        os.environ["BLOCKID_MAX_WALLETS"] = "50"


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


def _load_wallet_list() -> list[str]:
    """Aggregate unique wallets from configured wallet source CSVs."""
    wallets: set[str] = set()
    for path in WALLET_SOURCES:
        if not path.exists():
            continue
        try:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    w = (row.get("wallet") or "").strip()
                    if w:
                        wallets.add(w)
        except Exception as e:
            _log(f"WARNING: failed to read {path.name}: {e}")
            continue
    return sorted(wallets)


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
        print("Return code:", result.returncode)
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


async def main() -> int:
    load_blockid_env()
    run_start_ts = int(time.time())
    _log(_SEP)
    _log("BlockID full pipeline")
    _log(_SEP)
    _log(f"project root: {_ROOT}")
    from backend_blockid.tools.generate_cluster_features import generate_cluster_features
    print("[run_full_pipeline] cluster_features auto-generated if missing")
    generate_cluster_features()
    from backend_blockid.tools.generate_wallet_reason_codes import generate_wallet_reason_codes
    print("[run_full_pipeline] wallet_reason_codes auto-generated if missing")
    generate_wallet_reason_codes()
    _log("")
    _print_dataset_info()
    _log("")
    _log("Required files check:")
    logger.info("run_full_pipeline_start")

    if not ensure_cluster_features():
        _log("ERROR: Could not generate cluster_features.csv. Ensure transactions.csv exists.")
        await record_pipeline_run_async(run_start_ts, int(time.time()), False, 0, 1, 0, "cluster_features failed")
        metrics_record_pipeline_run(success=False, wallets_scanned=0)
        return 1

    if not check_required_files():
        await record_pipeline_run_async(run_start_ts, int(time.time()), False, 0, 1, 0, "missing required files")
        metrics_record_pipeline_run(success=False, wallets_scanned=0)
        return 1

    _log("All required files present.")
    _log("")

    # Priority wallet selection (top 100) before Helius fetch
    await remove_old_wallets(days=30)
    await populate_priority_wallets()
    aged = await age_priorities()
    boosted = await boost_active_wallets()
    wallet_list = await get_wallets_with_budget()
    _log(f"Aging applied to {aged} wallets")
    _log(f"Boosted active wallets: {boosted}")
    _log(f"Selected wallets for run: {len(wallet_list)}")
    logger.info(
        "wallet_selection",
        selected=len(wallet_list),
        limit=100,
        test_mode=BLOCKID_TEST_MODE,
    )
    _log(f"wallet_meta incremental scan: {len(wallet_list)} wallets (prioritized)")
    scan_latency_ms: int | None = None
    scan_start = time.time()
    try:
        for wallet in wallet_list:
            await scan_wallet(wallet)
        scan_latency_ms = int((time.time() - scan_start) * 1000)
        await log_event("helius_fetch", "ok", f"scanned_wallets={len(wallet_list)}", latency_ms=scan_latency_ms)
    except Exception as e:
        scan_latency_ms = int((time.time() - scan_start) * 1000)
        await log_event("helius_fetch", "error", str(e), latency_ms=scan_latency_ms)
        raise

    clusters = await get_all_active_clusters()
    _log(f"[SCHEDULER] scanning {len(clusters)} clusters")
    await log_event("graph_cluster", "ok", f"clusters_scanned={len(clusters)}")
    for cid in clusters:
        await scan_cluster(cid)

    test_wallet_limit()

    results: list[tuple[str, bool, str]] = []

    for i, (step_name, module, _) in enumerate(STEPS, start=1):
        if should_skip_step(step_name):
            continue
        _log(_SEP_THIN)
        _log(f"Step {i}/{len(STEPS)}: {step_name}")
        _log(_SEP_THIN)
        ok, msg = run_step(step_name, module)
        if ok:
            results.append((step_name, True, "OK"))
            if step_name == "predict_wallet_score":
                for wallet in wallet_list:
                    await update_wallet_score_async(wallet)
                wallet_list = await get_wallets_with_budget(limit=100)
                _log(f"Selected wallets for run: {len(wallet_list)}")
                await log_event("ml_scoring", "ok", f"wallets_scored={len(wallet_list)}")
                await log_event("dynamic_risk", "ok", f"wallets_updated={len(wallet_list)}")
            if step_name == "batch_publish":
                await log_event("pda_publish", "ok", f"wallets_published={len(wallet_list)}")
        else:
            results.append((step_name, False, msg))
            _log(f"  {msg}")
            _log("")
            _log("Pipeline stopped on first failure.")
            if step_name == "batch_publish":
                await log_event("pda_publish", "error", msg)
            break
        _log("")

    # Verification step (post-run)
    try:
        from backend_blockid.tools.verify_pipeline_output import verify_pipeline_output
        if not verify_pipeline_output():
            _log("[verify] WARNING: pipeline verification failed")
    except Exception as e:
        _log(f"[verify] WARNING: verification error: {e}")

    # Helius cost monitor (report + budget guard)
    _log(_SEP_THIN)
    _log("Helius cost monitor")
    _log(_SEP_THIN)
    try:
        code = subprocess.run(
            [sys.executable, "-m", "backend_blockid.tools.helius_cost_monitor"],
            cwd=str(_ROOT),
            capture_output=False,
            timeout=60,
        ).returncode
        if code != 0:
            results.append(("helius_cost_monitor", False, "Over daily budget limit"))
            _log("WARNING: Helius cost exceeds DAILY_LIMIT. Pipeline marked failed.")
            failed = sum(1 for _, ok, _ in results if not ok)
            _log(_SEP)
            _log("SUMMARY")
            _log(_SEP)
            for step_name, ok, detail in results:
                status = "OK" if ok else "FAIL"
                _log(f"  {status}: {step_name}")
            _log(_SEP)
            _log("RESULT: FAILED (Helius budget exceeded)")
            logger.warning("run_full_pipeline_helius_over_budget")
            await record_pipeline_run_async(
                run_start_ts, int(time.time()), False,
                len(wallet_list), 1, len(results), "Helius budget exceeded",
            )
            metrics_record_pipeline_run(success=False, wallets_scanned=len(wallet_list))
            return 1
        results.append(("helius_cost_monitor", True, "OK"))
        _log("OK: helius_cost_monitor")
    except Exception as e:
        _log(f"[helius_cost] WARNING: {e}")
        results.append(("helius_cost_monitor", True, "OK"))  # don't fail pipeline on monitor error
    _log("")

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
    pda_failures = sum(1 for step_name, ok, _ in results if step_name == "batch_publish" and not ok)
    if failed > 0:
        _log(f"RESULT: FAILED ({failed} step(s))")
        logger.warning("run_full_pipeline_failed", failed_steps=failed, steps=[r[0] for r in results])
        await log_event("pipeline", "error", f"failed_steps={failed}")
        await record_pipeline_run_async(
            run_start_ts, int(time.time()), False,
            len(wallet_list), failed, len(results), "pipeline step failed",
        )
        metrics_record_pipeline_run(success=False, wallets_scanned=len(wallet_list))
        send_pipeline_summary(
            {
                "wallets_scored": len(wallet_list),
                "rpc_latency_avg": scan_latency_ms,
                "pda_failures": pda_failures,
                "errors": failed,
            }
        )
        print("[run_full_pipeline] Pipeline finished")
        print(f"[run_full_pipeline] TEST_MODE={BLOCKID_TEST_MODE}")
        return 1

    _log("RESULT: PASS (all steps completed)")
    logger.info("run_full_pipeline_done", steps=len(results))
    await log_event("pipeline", "ok", f"steps_completed={len(results)}")
    await record_pipeline_run_async(
        run_start_ts, int(time.time()), True,
        len(wallet_list), 0, len(results), None,
    )
    metrics_record_pipeline_run(success=True, wallets_scanned=len(wallet_list))
    send_pipeline_summary(
        {
            "wallets_scored": len(wallet_list),
            "rpc_latency_avg": scan_latency_ms,
            "pda_failures": pda_failures,
            "errors": 0,
        }
    )
    print("[run_full_pipeline] Pipeline finished")
    print(f"[run_full_pipeline] TEST_MODE={BLOCKID_TEST_MODE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
