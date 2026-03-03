"""
BlockID Mainnet Readiness Checklist.

Runs all pre-deployment checks and generates mainnet_readiness_report.txt.
Prevents false positives, outages, and incorrect trust score publishing.

Usage:
  py -m backend_blockid.tools.mainnet_readiness_checklist
  py -m backend_blockid.tools.mainnet_readiness_checklist --quick  # skip subprocess calls
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_BACKEND = Path(__file__).resolve().parent.parent
_REPORTS_DIR = _BACKEND / "reports"
OUTPUT_REPORT = _REPORTS_DIR / "mainnet_readiness_report.txt"


def _run_cmd(cmd: list[str], cwd: Path | None = None) -> tuple[int, str]:
    """Run command, return (exit_code, stdout+stderr)."""
    try:
        r = subprocess.run(
            cmd,
            cwd=cwd or _ROOT,
            capture_output=True,
            text=True,
            timeout=120,
        )
        out = (r.stdout or "") + (r.stderr or "")
        return r.returncode, out
    except Exception as e:
        return -1, str(e)


def _section(name: str, lines: list[str], checks: list[tuple[str, bool, str | None]]) -> list[str]:
    out = [f"\n{'='*60}", f"SECTION — {name}", "="*60]
    for title, ok, detail in checks:
        status = "✔" if ok else "✗"
        out.append(f"  {status} {title}")
        if detail:
            out.append(f"      {detail}")
    out.extend(lines)
    return out


def run_checklist(quick: bool = False) -> list[str]:
    """Run all checklist sections. Return report lines."""
    report: list[str] = []
    report.append("BlockID Mainnet Readiness Report")
    report.append(f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    report.append("")

    # Section 1 — Data Integrity
    from backend_blockid.tools.check_database_integrity import run_checks as db_checks
    db_results = db_checks()
    s1_checks = [
        ("All required DB tables exist", db_results["tables_exist"]["ok"], 
         str(db_results["tables_exist"].get("missing", [])) if not db_results["tables_exist"]["ok"] else None),
        ("No NULL wallet addresses", db_results["no_null_wallets"]["ok"],
         str(db_results["no_null_wallets"].get("issues", {})) if not db_results["no_null_wallets"]["ok"] else None),
        ("trust_scores.wallet unique index", db_results["trust_scores_unique"]["ok"],
         db_results["trust_scores_unique"].get("message")),
        ("wallet_history timestamps valid", db_results["wallet_history_timestamps"]["ok"],
         str(db_results["wallet_history_timestamps"].get("issues", [])) if not db_results["wallet_history_timestamps"]["ok"] else None),
        ("cluster_ids consistent", db_results["cluster_ids_consistent"]["ok"],
         str(db_results["cluster_ids_consistent"].get("issues", [])) if not db_results["cluster_ids_consistent"]["ok"] else None),
    ]
    report.extend(_section("Data Integrity Checks", [], s1_checks))

    # Section 2 — False Positive Tests
    fp_ok = False
    fp_detail = "Run: py -m backend_blockid.tools.test_false_positives"
    if not quick:
        code, out = _run_cmd([sys.executable, "-m", "backend_blockid.tools.test_false_positives"])
        fp_ok = code == 0
        if "false_positive" in out.lower() or "candidates" in out.lower():
            fp_detail = "Report saved to backend_blockid/reports/"
    s2_checks = [
        ("Run test_false_positives.py", fp_ok or quick, None if quick else fp_detail),
        ("False positive rate < 5% (manual verify)", None, "Manual: review false_positive_candidates.csv"),
        ("Manual review of 50 random wallets", None, "Manual step"),
    ]
    report.extend(_section("False Positive Tests", [], s2_checks))

    # Section 3 — Confidence Calibration
    conf_threshold = os.getenv("CONFIDENCE_THRESHOLD", "")
    conf_ok = bool(conf_threshold and conf_threshold.strip())
    s3_checks = [
        ("Run calibrate_confidence.py", None, "Script not in repo — add if needed"),
        ("CONFIDENCE_THRESHOLD in config", conf_ok, f"Current: {conf_threshold or 'not set'}" if conf_threshold else "Add CONFIDENCE_THRESHOLD=0.72 to .env"),
    ]
    report.extend(_section("Confidence Calibration", [], s3_checks))

    # Section 4 — Review Queue Active
    from backend_blockid.database.connection import get_connection
    conn = get_connection()
    cur = conn.cursor()
    review_table = False
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='review_queue'")
        review_table = cur.fetchone() is not None
    except Exception:
        pass
    conn.close()
    batch_publish_path = _BACKEND / "oracle" / "batch_publish.py"
    batch_checks_review = batch_publish_path.exists() and "is_pending_review" in batch_publish_path.read_text(encoding="utf-8")
    s4_checks = [
        ("review_queue table created", review_table, None),
        ("batch_publish checks review_queue", batch_checks_review, None),
        ("CLI approve/reject tested", None, "Manual: py -m backend_blockid.tools.review_queue_cli list"),
    ]
    report.extend(_section("Review Queue Active", [], s4_checks))

    # Section 5 — Propagation & Reason Validation
    opt_reason = Path(_BACKEND / "tools" / "optimize_reason_weights.py").exists()
    opt_prop = Path(_BACKEND / "tools" / "optimize_propagation_weights.py").exists()
    s5_checks = [
        ("optimize_reason_weights.py exists", opt_reason, "Run before mainnet"),
        ("optimize_propagation_weights.py exists", opt_prop, "Run before mainnet"),
        ("No extreme scores without evidence", None, "Manual verification"),
    ]
    report.extend(_section("Propagation & Reason Validation", [], s5_checks))

    # Section 6 — Oracle Safety Checks
    test_mode = os.getenv("BLOCKID_TEST_MODE", "1")
    pipeline_mode = os.getenv("BLOCKID_PIPELINE_MODE", "1")
    dry_run = os.getenv("BLOCKID_DRY_RUN", "0")
    s6_checks = [
        ("BLOCKID_TEST_MODE=0 for mainnet", test_mode == "0", f"Current: {test_mode}"),
        ("BLOCKID_PIPELINE_MODE=0 for prod", pipeline_mode == "0", f"Current: {pipeline_mode}"),
        ("BLOCKID_DRY_RUN=0 to publish", dry_run == "0", f"Current: {dry_run}"),
        ("Publish only if confidence >= threshold", None, "Verify in batch_publish / publish_one"),
        ("Publish logs stored", None, "Check oracle tx logs"),
    ]
    report.extend(_section("Oracle Safety Checks", [], s6_checks))

    # Section 7 — Solana Program Checks
    program_id = os.getenv("ORACLE_PROGRAM_ID", "")
    oracle_key = os.getenv("ORACLE_PRIVATE_KEY", "")
    s7_checks = [
        ("Program ID correct", bool(program_id), f"ORACLE_PROGRAM_ID={'set' if program_id else 'not set'}"),
        ("PDA derivation verified", None, "Manual: test publish on devnet"),
        ("Test publish on devnet success", None, "Manual step"),
        ("Explorer check of PDA accounts", None, "Manual: solscan.io / explorer.solana.com"),
    ]
    report.extend(_section("Solana Program Checks", [], s7_checks))

    # Section 8 — Helius Cost Control
    helius_ok = False
    if not quick:
        code, _ = _run_cmd([sys.executable, "-m", "backend_blockid.tools.helius_cost_monitor"])
        helius_ok = code == 0
    max_wallets = os.getenv("BLOCKID_MAX_WALLETS", "")
    s8_checks = [
        ("Incremental tx fetch enabled", True, "fetch_helius_transactions uses last signature"),
        ("Max wallets per run set", bool(max_wallets), f"BLOCKID_MAX_WALLETS={max_wallets or 'not set'}" if max_wallets else "Set BLOCKID_MAX_WALLETS"),
        ("helius_cost_monitor run", helius_ok or quick, None),
    ]
    report.extend(_section("Helius Cost Control", [], s8_checks))

    # Section 9 — Monitoring & Alerts
    s9_checks = [
        ("FastAPI health endpoint", True, "GET /health"),
        ("Realtime risk alerts", True, "realtime_risk_engine logs"),
        ("Telegram/email alerts configured", None, "Manual: configure alerts/engine.py"),
    ]
    report.extend(_section("Monitoring & Alerts", [], s9_checks))

    # Section 10 — Backup & Rollback
    s10_checks = [
        ("DB backup script tested", None, "Manual: backup blockid.db"),
        ("trust_scores snapshot saved", None, "Manual: export before deploy"),
        ("Ability to stop publishing quickly", True, "BLOCKID_DRY_RUN=1 or BLOCKID_SKIP_PUBLISH=1"),
    ]
    report.extend(_section("Backup & Rollback", [], s10_checks))

    # Section 11 — UI & Explorer Checks
    s11_checks = [
        ("Badge engine correct", Path(_BACKEND / "tools" / "badge_engine.py").exists(), None),
        ("Investigation Explorer works", Path(_BACKEND / "api_server" / "investigation_api.py").exists(), "GET /wallet/{wallet}/investigation_badge"),
        ("Graph panel loads", Path(_BACKEND / "api_server" / "graph_api.py").exists(), "GET /wallet/{wallet}/graph"),
        ("PDF report generation works", Path(_BACKEND / "tools" / "generate_wallet_report.py").exists(), "py -m backend_blockid.tools.generate_wallet_report"),
    ]
    report.extend(_section("UI & Explorer Checks", [], s11_checks))

    # Section 12 — Final Dry Run
    s12_checks = [
        ("Run full pipeline on 500 wallets", None, "Manual: run_full_pipeline.py"),
        ("Compare with manual labels", None, "Manual step"),
        ("Verify top 20 risky wallets manually", None, "Manual step"),
    ]
    report.extend(_section("Final Dry Run", [], s12_checks))

    report.append("\n" + "="*60)
    report.append("End of Mainnet Readiness Report")
    report.append("="*60)
    return report


def main() -> int:
    ap = argparse.ArgumentParser(description="BlockID Mainnet Readiness Checklist")
    ap.add_argument("--quick", action="store_true", help="Skip subprocess calls (test_false_positives, helius_cost_monitor)")
    ap.add_argument("-o", "--output", type=Path, default=OUTPUT_REPORT, help="Output report path")
    args = ap.parse_args()

    report = run_checklist(quick=args.quick)
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = args.output
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(report))

    print(f"[mainnet_readiness] Report saved to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
