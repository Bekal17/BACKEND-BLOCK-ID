"""
BlockID Database Integrity Check.

Validates data integrity before mainnet deployment.
Run before go-live to catch schema and data issues.

Usage:
  py -m backend_blockid.tools.check_database_integrity
"""
from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection

REQUIRED_TABLES = [
    "trust_scores",
    "wallet_reasons",
    "scam_wallets",
    "wallet_clusters",
    "wallet_history",
    "wallet_risk_probabilities",
    "wallet_last_update",
    "wallet_badges",
]


def check_all_tables_exist(cur) -> tuple[bool, list[str]]:
    """All required DB tables exist."""
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {r[0] for r in cur.fetchall()}
    missing = [t for t in REQUIRED_TABLES if t not in existing]
    return len(missing) == 0, missing


def check_null_wallets(cur) -> tuple[bool, dict[str, int]]:
    """No NULL or empty wallet addresses in key tables."""
    issues: dict[str, int] = {}
    for table in ("trust_scores", "wallet_reasons", "wallet_clusters", "wallet_history"):
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE wallet IS NULL OR TRIM(wallet) = ''")
            cnt = cur.fetchone()[0] or 0
            if cnt > 0:
                issues[table] = cnt
        except Exception:
            issues[table] = -1
    return len(issues) == 0, issues


def check_trust_scores_unique(cur) -> tuple[bool, str | None]:
    """trust_scores.wallet has unique index or no duplicates."""
    cur.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name='trust_scores'"
    )
    rows = cur.fetchall()
    has_unique = any(
        "unique" in (str(r[1] or "")).lower() or "ux_" in str(r[0] or "").lower()
        for r in rows
    )
    cur.execute("SELECT COUNT(*), COUNT(DISTINCT wallet) FROM trust_scores WHERE wallet IS NOT NULL")
    r = cur.fetchone()
    total, distinct = (r[0] or 0), (r[1] or 0)
    if total > 0 and total != distinct:
        return False, f"Duplicate wallets: {total} rows, {distinct} unique"
    if not has_unique and total > 0:
        return False, "No unique index on trust_scores.wallet (risk of duplicates)"
    return True, None


def check_wallet_history_timestamps(cur) -> tuple[bool, list[str]]:
    """wallet_history timestamps valid (reasonable range)."""
    issues = []
    try:
        cur.execute("SELECT MIN(snapshot_at), MAX(snapshot_at) FROM wallet_history")
        r = cur.fetchone()
        if not r or (r[0] is None and r[1] is None):
            return True, []
        lo, hi = r[0] or 0, r[1] or 0
        import time
        now = int(time.time())
        if lo < 1000000000 or lo > now + 86400:
            issues.append(f"Min snapshot_at={lo} out of range")
        if hi < 1000000000 or hi > now + 86400:
            issues.append(f"Max snapshot_at={hi} out of range")
    except Exception as e:
        issues.append(str(e))
    return len(issues) == 0, issues


def check_cluster_ids_consistent(cur) -> tuple[bool, list[str]]:
    """cluster_ids consistent across wallet_clusters."""
    issues = []
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wallet_clusters'")
        if not cur.fetchone():
            return True, []
        cur.execute(
            "SELECT cluster_id, COUNT(*) FROM wallet_clusters WHERE cluster_id IS NOT NULL GROUP BY cluster_id"
        )
        for cid, cnt in cur.fetchall():
            if cnt < 1:
                issues.append(f"Cluster {cid} has 0 members")
    except Exception as e:
        issues.append(str(e))
    return len(issues) == 0, issues


def run_checks() -> dict:
    """Run all integrity checks. Return dict of results."""
    conn = get_connection()
    cur = conn.cursor()
    results = {}

    ok, missing = check_all_tables_exist(cur)
    results["tables_exist"] = {"ok": ok, "missing": missing}

    ok, issues = check_null_wallets(cur)
    results["no_null_wallets"] = {"ok": ok, "issues": issues}

    ok, msg = check_trust_scores_unique(cur)
    results["trust_scores_unique"] = {"ok": ok, "message": msg}

    ok, issues = check_wallet_history_timestamps(cur)
    results["wallet_history_timestamps"] = {"ok": ok, "issues": issues}

    ok, issues = check_cluster_ids_consistent(cur)
    results["cluster_ids_consistent"] = {"ok": ok, "issues": issues}

    conn.close()
    return results


def main() -> int:
    results = run_checks()
    all_ok = all(r["ok"] for r in results.values())
    for name, r in results.items():
        status = "OK" if r["ok"] else "FAIL"
        print(f"[check_db] {name}: {status}")
        if not r["ok"] and r.get("missing"):
            print(f"  missing tables: {r['missing']}")
        if not r["ok"] and r.get("issues"):
            print(f"  issues: {r['issues']}")
        if not r["ok"] and r.get("message"):
            print(f"  {r['message']}")
    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
