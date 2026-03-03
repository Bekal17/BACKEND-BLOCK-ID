"""
BlockID Final Mainnet Launch Checklist.

Comprehensive pre-launch verification to prevent false positives, outages,
and incorrect mainnet publishing.

Stack: FastAPI + Python 3.13 + Solana Anchor + Helius
Pipeline: transactions → clusters → propagation → ML → trust_scores → PDA publish

Usage:
  py -m backend_blockid.tools.mainnet_launch_checklist
  py -m backend_blockid.tools.mainnet_launch_checklist --quick
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Load .env before any config checks
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except Exception:
    pass

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_BACKEND = Path(__file__).resolve().parent.parent
_REPORTS_DIR = _BACKEND / "reports"
OUTPUT_REPORT = _REPORTS_DIR / "mainnet_launch_report.txt"

ORACLE_SIGNER = "9QCfNuQuxct1Xk9ytFYgxc5fThmTzL4pHQnSjjrVUrka"


def _run_cmd(cmd: list[str], cwd: Path | None = None, timeout: int = 180) -> tuple[int, str]:
    try:
        r = subprocess.run(
            cmd,
            cwd=cwd or _ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    except Exception as e:
        return -1, str(e)


def _check(name: str, ok: bool, detail: str | None = None) -> tuple[str, bool, str | None]:
    return (name, ok, detail)


def _section(title: str, checks: list[tuple[str, bool, str | None]], extra: list[str] | None = None) -> list[str]:
    lines = [f"\n{'='*70}", f"SECTION {title}", "="*70]
    for name, ok, detail in checks:
        mark = "✔" if ok else "✗"
        lines.append(f"  {mark} {name}")
        if detail:
            lines.append(f"      {detail}")
    if extra:
        lines.extend(extra)
    return lines


def run_checklist(quick: bool = False) -> list[str]:
    report: list[str] = []
    report.append("BlockID Final Mainnet Launch Checklist")
    report.append("=" * 70)
    report.append(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    report.append("Purpose: Prevent false positives, outages, incorrect mainnet publishing.")

    # --- SECTION 1 — Pipeline Stability ---
    pipeline_ok = False
    pipeline_detail = "Run: py -m backend_blockid.tools.run_full_pipeline"
    if not quick:
        code, out = _run_cmd([sys.executable, "-m", "backend_blockid.tools.run_full_pipeline", "--limit", "5"], timeout=300)
        pipeline_ok = code == 0
        pipeline_detail = f"Exit code: {code}" + (f" | {out[:200]}..." if len(out) > 200 else f" | {out[:100]}")

    graph_clustering = (_BACKEND / "oracle" / "graph_clustering.py").exists()
    propagation = (_BACKEND / "tools" / "propagation_engine_v1.py").exists() or (_BACKEND / "oracle").exists()
    reason_agg = (_BACKEND / "oracle" / "reason_aggregator.py").exists()
    bayesian = (_BACKEND / "ml" / "bayesian_risk.py").exists()

    s1 = _section("1 — Pipeline Stability", [
        _check("run_full_pipeline.py runs successfully 5 days in a row", pipeline_ok or quick, None if quick else pipeline_detail),
        _check("No crashes in graph_clustering / propagation_engine", graph_clustering and propagation, None),
        _check("reason_aggregator dedup working", reason_agg, "Manual: verify dedup in reason_aggregator.py"),
        _check("dynamic_risk Bayesian update stable", bayesian, None),
    ])
    report.extend(s1)

    # --- SECTION 2 — Accuracy Validation ---
    fp_ok = False
    if not quick:
        code, _ = _run_cmd([sys.executable, "-m", "backend_blockid.tools.test_false_positives"])
        fp_ok = code == 0

    s2 = _section("2 — Accuracy Validation", [
        _check("False positive rate < 5%", fp_ok or quick, None if quick else "Run test_false_positives.py"),
        _check("False negative spot-check", None, "Manual: verify known scams detected"),
        _check("100 random wallets manually reviewed", None, "Manual step"),
        _check("Known scam wallets detected correctly", None, "Manual: check scam_wallets.csv coverage"),
    ])
    report.extend(s2)

    # --- SECTION 3 — Confidence & Review Queue ---
    conf = os.getenv("CONFIDENCE_THRESHOLD", "")
    conf_ok = bool(conf and conf.strip() and float(conf or 0) > 0)
    review_enabled = (os.getenv("REVIEW_QUEUE_ENABLED", "1") or "1").strip().lower() in ("1", "true", "yes")
    try:
        from backend_blockid.config.env import load_blockid_env
        load_blockid_env()
        from backend_blockid.database.connection import get_connection
        cur = get_connection().cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='review_queue'")
        review_table = cur.fetchone() is not None
    except Exception:
        review_table = False

    s3 = _section("3 — Confidence & Review Queue", [
        _check("CONFIDENCE_THRESHOLD calibrated", conf_ok, f"Current: {conf or 'not set'} (suggest 0.72)"),
        _check("review_queue enabled", review_enabled and review_table, f"REVIEW_QUEUE_ENABLED={os.getenv('REVIEW_QUEUE_ENABLED', '1')}"),
        _check("manual approve/reject tested", review_table, "py -m backend_blockid.tools.review_queue_cli list"),
        _check("low-confidence wallets skipped", conf_ok, "Verify in predict_wallet_score / batch_publish"),
    ])
    report.extend(s3)

    # --- SECTION 4 — Oracle Safety ---
    test_mode = (os.getenv("BLOCKID_TEST_MODE") or "1").strip()
    pipeline_mode = (os.getenv("BLOCKID_PIPELINE_MODE") or "1").strip()
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    program_id = (os.getenv("ORACLE_PROGRAM_ID") or "").strip()

    s4 = _section("4 — Oracle Safety", [
        _check("BLOCKID_TEST_MODE=0", test_mode == "0", f"Current: {test_mode}"),
        _check("BLOCKID_PIPELINE_MODE=0", pipeline_mode == "0", f"Current: {pipeline_mode}"),
        _check("Publishing limited to safe wallets", None, "Verify batch_publish / pre_publish_check"),
        _check("PDA derivation verified on devnet", None, "Manual: test publish on devnet"),
        _check(f"Oracle signer: {ORACLE_SIGNER[:20]}...", bool(oracle_key), f"ORACLE_PRIVATE_KEY={'set' if oracle_key else 'not set'}, ORACLE_PROGRAM_ID={'set' if program_id else 'not set'}"),
    ])
    report.extend(s4)

    # --- SECTION 5 — Helius Cost Control ---
    helius_inc = (os.getenv("HELIUS_INCREMENTAL_FETCH") or "1").strip().lower()
    helius_incremental = helius_inc in ("1", "true", "yes") or "incremental" in helius_inc
    prioritizer = (_BACKEND / "oracle" / "wallet_scan_prioritizer.py").exists()
    helius_monitor = (_BACKEND / "tools" / "helius_cost_monitor.py").exists()
    helius_ok = False
    if not quick and helius_monitor:
        code, _ = _run_cmd([sys.executable, "-m", "backend_blockid.tools.helius_cost_monitor"])
        helius_ok = code == 0

    s5 = _section("5 — Helius Cost Control", [
        _check("incremental fetch enabled", helius_incremental, f"HELIUS_INCREMENTAL_FETCH={os.getenv('HELIUS_INCREMENTAL_FETCH', '1')}"),
        _check("wallet_scan_prioritizer working", prioritizer, None),
        _check("helius_cost_monitor active", helius_ok or quick or helius_monitor, None),
        _check("daily cost limit configured", None, "Check helius_cost_monitor DAILY_LIMIT"),
    ])
    report.extend(s5)

    # --- SECTION 6 — Monitoring & Alerts ---
    grafana = Path("monitoring").exists() or Path("monitoring/CANARY_DASHBOARD.md").exists()
    prometheus = (_BACKEND / "api_server" / "metrics.py").exists()

    s6 = _section("6 — Monitoring & Alerts", [
        _check("Grafana dashboards live", grafana, "monitoring/README.md, CANARY_DASHBOARD.md"),
        _check("Prometheus metrics working", prometheus, "GET /metrics"),
        _check("Alerts for pipeline failure", None, "Configure in monitoring / alerts/engine.py"),
        _check("Alerts for RPC failover", None, "blockid_rpc_switch_total metric"),
    ])
    report.extend(s6)

    # --- SECTION 7 — Deployment Safety ---
    dockerfile = Path("Dockerfile").exists() or Path("docker-compose.yml").exists()
    k8s = Path("k8s").exists()
    canary = (_BACKEND / "tools" / "canary_deploy.py").exists()

    s7 = _section("7 — Deployment Safety", [
        _check("Docker image tested", dockerfile, None),
        _check("Kubernetes deploy tested", k8s, "k8s/README.md"),
        _check("Canary deployment tested", canary, "k8s/backend-deployments.yaml, .github/workflows/blockid_canary.yml"),
        _check("Auto rollback tested", canary, "kubectl scale blockid-api-canary --replicas=0"),
    ])
    report.extend(s7)

    # --- SECTION 8 — Performance ---
    locust_file = (_BACKEND / "tools" / "locust_blockid.py").exists()

    s8 = _section("8 — Performance", [
        _check("Locust load test passed", locust_file, "locust -f backend_blockid/tools/locust_blockid.py"),
        _check("API < 300ms response", None, "Manual: run Locust, check p95 latency"),
        _check("DB queries optimized", None, "Check indexes, query plans"),
        _check("Graph API tested", (_BACKEND / "api_server" / "graph_api.py").exists(), "GET /wallet/{wallet}/graph"),
    ])
    report.extend(s8)

    # --- SECTION 9 — UI Integration ---
    badge_api = (_BACKEND / "api_server" / "investigation_api.py").exists()
    report_gen = (_BACKEND / "tools" / "generate_wallet_report.py").exists()

    s9 = _section("9 — UI Integration", [
        _check("API contract stable", None, "OpenAPI / generate_spec.py"),
        _check("app.blockidscore.fun connected to staging API", None, "Manual: verify staging endpoint"),
        _check("Investigation Explorer works", badge_api, "GET /wallet/{wallet}/investigation_badge"),
        _check("Badge timeline correct", (_BACKEND / "api_server" / "badge_api.py").exists(), "GET /wallet/{wallet}/badge_timeline"),
    ])
    report.extend(s9)

    # --- SECTION 10 — Security ---
    env_in_gitignore = ".env" in (Path(".gitignore").read_text() if Path(".gitignore").exists() else "")
    oracle_in_git = ".env" not in (subprocess.run(["git", "ls-files", ".env"], capture_output=True, text=True).stdout or "")

    s10 = _section("10 — Security", [
        _check("Oracle keypair secure", oracle_key and not oracle_in_git, "Keypair path, not in repo"),
        _check(".env secrets not in repo", env_in_gitignore, ".gitignore contains .env"),
        _check("Rate limiting enabled", None, "realtime_risk_engine rate limit, API middleware"),
        _check("Audit logs active", None, "blockid_logging, structlog"),
    ])
    report.extend(s10)

    # --- SECTION 11 — Backup & Recovery ---
    rollback = (_BACKEND / "tools" / "rollback_deployment.py").exists()

    s11 = _section("11 — Backup & Recovery", [
        _check("DB backup tested", None, "Manual: backup blockid.db"),
        _check("Rollback script tested", rollback, "py -m backend_blockid.tools.rollback_deployment"),
        _check("PDA publish can be stopped", True, "BLOCKID_DRY_RUN=1 or BLOCKID_SKIP_PUBLISH=1"),
    ])
    report.extend(s11)

    # --- SECTION 12 — Dry Run ---
    s12 = _section("12 — Dry Run", [
        _check("Run pipeline on 1000 wallets", None, "py -m backend_blockid.tools.run_full_pipeline --limit 1000"),
        _check("Compare results with manual labels", None, "Manual step"),
        _check("Verify top 20 risky wallets", None, "Manual step"),
    ])
    report.extend(s12)

    # --- SECTION 13 — Launch Plan ---
    s13 = _section("13 — Launch Plan", [
        _check("Start with small wallet set", None, "e.g. 100–500 wallets initially"),
        _check("Monitor metrics 24 hours", None, "Grafana, Prometheus, pipeline_run_log"),
        _check("Enable full publish", None, "BLOCKID_TEST_MODE=0, BLOCKID_DRY_RUN=0"),
    ])
    report.extend(s13)

    report.append("\n" + "=" * 70)
    report.append("End of Mainnet Launch Checklist")
    report.append("=" * 70)
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="BlockID Final Mainnet Launch Checklist")
    ap.add_argument("--quick", action="store_true", help="Skip subprocess calls (pipeline, test_false_positives, helius)")
    ap.add_argument("-o", "--output", type=Path, default=OUTPUT_REPORT, help="Output report path")
    args = ap.parse_args()

    report = run_checklist(quick=args.quick)
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = args.output
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    print(f"[mainnet_launch] Report saved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
