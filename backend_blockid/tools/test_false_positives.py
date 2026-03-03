"""
BlockID False Positive Test Script.

Evaluates scoring accuracy and detects wrongly flagged wallets.
Output: backend_blockid/reports/false_positive_candidates.csv

Usage:
  py -m backend_blockid.tools.test_false_positives

Future upgrades:
* Compare with known scam wallet list
* ROC curve evaluation
* Precision / recall metrics
* Auto tuning of reason weights
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
_REPORTS_DIR = _BACKEND_DIR / "reports"
OUTPUT_CSV = _REPORTS_DIR / "false_positive_candidates.csv"
MANUAL_WALLETS_CSV = _DATA_DIR / "manual_wallets.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"
LIMIT = 200


def _load_scam_wallets() -> set[str]:
    """Load scam wallets from DB and CSV."""
    out: set[str] = set()
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scam_wallets'")
        if cur.fetchone():
            cur.execute("SELECT wallet FROM scam_wallets WHERE wallet IS NOT NULL")
            for r in cur.fetchall():
                w = (r[0] or "").strip()
                if w:
                    out.add(w)
    except Exception:
        pass
    conn.close()

    if SCAM_WALLETS_CSV.exists():
        with open(SCAM_WALLETS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
                if w:
                    out.add(w)
    return out


def _load_manual_safe_wallets() -> set[str]:
    """Load manual safe list (wallet column, exclude is_test_wallet=1 if present)."""
    out: set[str] = set()
    if not MANUAL_WALLETS_CSV.exists():
        return out
    with open(MANUAL_WALLETS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            w = (row.get("wallet") or "").strip()
            is_test = (row.get("is_test_wallet") or "").strip() in ("1", "true", "True", "yes")
            if w and not is_test:
                out.add(w)
    return out


def _get_tx_to_scam(conn, cur, wallet: str, scam_set: set[str]) -> tuple[int, float]:
    """Return (tx_count_to_scam, total_flow_sol_to_scam)."""
    tx_count = 0
    flow_sol = 0.0
    scam_list = list(scam_set)
    if not scam_list:
        return 0, 0.0

    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}

    if "from_wallet" in cols and "to_wallet" in cols:
        placeholders = ",".join("?" * len(scam_list))
        cur.execute(
            f"""
            SELECT COUNT(*), COALESCE(SUM(amount), 0)
            FROM transactions
            WHERE from_wallet = ? AND to_wallet IN ({placeholders})
            """,
            (wallet,) + tuple(scam_list),
        )
        row = cur.fetchone()
        if row:
            tx_count = row[0] or 0
            flow_sol = float(row[1] or 0)
    else:
        # sender/receiver/amount_lamports
        placeholders = ",".join("?" * len(scam_list))
        cur.execute(
            f"""
            SELECT COUNT(*), COALESCE(SUM(amount_lamports), 0)
            FROM transactions
            WHERE sender = ? AND receiver IN ({placeholders})
            """,
            (wallet,) + tuple(scam_list),
        )
        row = cur.fetchone()
        if row:
            tx_count = row[0] or 0
            flow_sol = float(row[1] or 0) / 1e9

    return tx_count, flow_sol


def _get_cluster_size(cur, wallet: str) -> int:
    for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
            if not cur.fetchone():
                continue
            cur.execute(f"SELECT cluster_id FROM {tbl} WHERE wallet = ? LIMIT 1", (wallet,))
            r = cur.fetchone()
            if r:
                cid = r[0]
                cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE cluster_id = ?", (cid,))
                return cur.fetchone()[0] or 0
        except Exception:
            continue
    return 0


def _get_scam_ratio_in_cluster(cur, wallet: str, scam_set: set[str]) -> float:
    """Ratio of scam wallets in same cluster. 0 if no cluster."""
    size = _get_cluster_size(cur, wallet)
    if size <= 0 or not scam_set:
        return 0.0
    for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
            if not cur.fetchone():
                continue
            cur.execute(f"SELECT cluster_id FROM {tbl} WHERE wallet = ? LIMIT 1", (wallet,))
            r = cur.fetchone()
            if not r:
                continue
            cid = r[0]
            cur.execute(f"SELECT wallet FROM {tbl} WHERE cluster_id = ?", (cid,))
            members = {row[0] for row in cur.fetchall() if row[0]}
            scam_in = len(members & scam_set)
            return scam_in / max(size, 1)
        except Exception:
            continue
    return 0.0


def _get_graph_distance(cur, wallet: str) -> int:
    cur.execute(
        "SELECT graph_distance FROM trust_scores WHERE wallet = ? LIMIT 1",
        (wallet,),
    )
    r = cur.fetchone()
    if r and r[0] is not None:
        try:
            return int(r[0])
        except (TypeError, ValueError):
            pass
    return 999


def _get_reasons(cur, wallet: str) -> str:
    cur.execute(
        "SELECT reason_codes FROM trust_scores WHERE wallet = ? LIMIT 1",
        (wallet,),
    )
    r = cur.fetchone()
    if r and r[0]:
        return str(r[0])
    cur.execute(
        "SELECT GROUP_CONCAT(reason_code) FROM wallet_reasons WHERE wallet = ?",
        (wallet,),
    )
    r = cur.fetchone()
    if r and r[0]:
        return str(r[0])
    return ""


def main() -> int:
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_connection()
    cur = conn.cursor()

    scam_set = _load_scam_wallets()
    manual_safe = _load_manual_safe_wallets()

    cur.execute(
        """
        SELECT wallet, score, risk_level
        FROM trust_scores
        WHERE score < 20 AND wallet IS NOT NULL
        LIMIT ?
        """,
        (LIMIT,),
    )
    flagged = cur.fetchall()

    candidates: list[dict] = []
    for row in flagged:
        wallet = (row[0] or "").strip()
        score = float(row[1] or 0)
        risk = str(row[2] or "1")

        tx_count, flow_sol = _get_tx_to_scam(conn, cur, wallet, scam_set)
        cluster_size = _get_cluster_size(cur, wallet)
        scam_ratio = _get_scam_ratio_in_cluster(cur, wallet, scam_set)
        graph_distance = _get_graph_distance(cur, wallet)
        reasons = _get_reasons(cur, wallet)

        weak_evidence = tx_count <= 1 and flow_sol < 0.1
        in_manual_safe = wallet in manual_safe

        if weak_evidence or in_manual_safe:
            if weak_evidence:
                print(f"[false_positive] wallet={wallet[:16]}... score={score} tx={tx_count} flow={flow_sol:.2f}")
            if in_manual_safe:
                print(f"[false_positive] manual_safe_flagged wallet={wallet[:16]}... score={score}")
            candidates.append({
                "wallet": wallet,
                "score": score,
                "risk": risk,
                "tx_count": tx_count,
                "flow_to_scam": round(flow_sol, 4),
                "cluster_size": cluster_size,
                "scam_ratio": round(scam_ratio, 4),
                "reasons": reasons,
                "manual_safe": 1 if in_manual_safe else 0,
            })

    conn.close()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["wallet", "score", "risk", "tx_count", "flow_to_scam", "cluster_size", "scam_ratio", "reasons", "manual_safe"]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(candidates)

    total = len(flagged)
    fp_count = len(candidates)
    rate = (fp_count / total * 100) if total else 0

    print()
    print("=" * 50)
    print("FALSE POSITIVE TEST SUMMARY")
    print("=" * 50)
    print(f"Total flagged wallets (score < 20): {total}")
    print(f"Potential false positives:          {fp_count}")
    print(f"False positive rate:                {rate:.1f}%")
    print("=" * 50)
    print(f"Output: {OUTPUT_CSV}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
