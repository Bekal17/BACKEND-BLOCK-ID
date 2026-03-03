"""
BlockID Helius API Cost Monitor.

Tracks API usage via helius_usage table and enforces budget guard.
Prints daily report, saves CSV, and exits with code 1 if over DAILY_LIMIT.

Future upgrades:
  - Real Helius billing API integration
  - Per-token cost tracking
  - Cost prediction model
  - Auto wallet prioritization

Usage:
  py -m backend_blockid.tools.helius_cost_monitor
  py -m backend_blockid.tools.helius_cost_monitor --max-wallets 100
  py -m backend_blockid.tools.helius_cost_monitor --check-only
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection

_REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
CSV_REPORT = _REPORTS_DIR / "helius_cost_report.csv"
DAILY_LIMIT = float(os.getenv("HELIUS_DAILY_LIMIT_USD", "5").strip() or "5")
DEFAULT_MAX_WALLETS = int(os.getenv("BLOCKID_MAX_WALLETS", "200").strip() or "200")


def _today_start_ts() -> int:
    """Unix timestamp at start of UTC day."""
    now = time.gmtime(time.time())
    return int(time.mktime((now.tm_year, now.tm_mon, now.tm_mday, 0, 0, 0, 0, 0, 0)))


def get_today_stats() -> tuple[int, float]:
    """Return (total_calls_today, estimated_cost_today)."""
    conn = get_connection()
    cur = conn.cursor()
    cutoff = _today_start_ts()
    try:
        cur.execute(
            "SELECT COALESCE(SUM(request_count), 0), COALESCE(SUM(estimated_cost), 0) FROM helius_usage WHERE timestamp >= ?",
            (cutoff,),
        )
        row = cur.fetchone()
        total_calls = int(row[0] or 0)
        total_cost = float(row[1] or 0.0)
    except Exception:
        total_calls, total_cost = 0, 0.0
    conn.close()
    return total_calls, total_cost


def get_top_wallets_today(limit: int = 10) -> list[tuple[str, int, float]]:
    """Return [(wallet, calls, cost), ...] sorted by cost desc."""
    conn = get_connection()
    cur = conn.cursor()
    cutoff = _today_start_ts()
    rows: list[tuple[str, int, float]] = []
    try:
        cur.execute(
            """
            SELECT wallet, SUM(request_count) AS calls, SUM(estimated_cost) AS cost
            FROM helius_usage WHERE timestamp >= ?
            GROUP BY wallet ORDER BY cost DESC LIMIT ?
            """,
            (cutoff, limit),
        )
        for r in cur.fetchall():
            w = (r[0] if hasattr(r, "keys") else r[0]) or ""
            c = int(r[1] if hasattr(r, "keys") else r[1] or 0)
            cost = float(r[2] if hasattr(r, "keys") else r[2] or 0.0)
            rows.append((w, c, cost))
    except Exception:
        pass
    conn.close()
    return rows


def save_csv_report(date_str: str, total_calls: int, estimated_cost: float) -> None:
    """Save/update helius_cost_report.csv (one row per date, update if today exists)."""
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    rows: list[list[str | int]] = [["date", "total_calls", "estimated_cost"]]
    seen_today = False
    if CSV_REPORT.exists():
        with open(CSV_REPORT, "r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            header = next(r, None)
            if header and header[0] == "date":
                for row in r:
                    if row and row[0] == date_str:
                        rows.append([date_str, total_calls, round(estimated_cost, 6)])
                        seen_today = True
                    else:
                        rows.append(row)
    if not seen_today:
        rows.append([date_str, total_calls, round(estimated_cost, 6)])
    with open(CSV_REPORT, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows(rows)


def run_report(max_wallets: int | None = None) -> dict:
    """Generate daily usage report."""
    total_calls, cost_today = get_today_stats()
    top_wallets = get_top_wallets_today(10)
    cost_monthly = cost_today * 30  # simple projection
    max_w = max_wallets or DEFAULT_MAX_WALLETS
    return {
        "total_calls_today": total_calls,
        "estimated_cost_today_usd": round(cost_today, 6),
        "estimated_cost_monthly_usd": round(cost_monthly, 4),
        "top_10_wallets": [{"wallet": w, "calls": c, "cost_usd": round(cost, 6)} for w, c, cost in top_wallets],
        "daily_limit_usd": DAILY_LIMIT,
        "over_budget": cost_today > DAILY_LIMIT,
        "max_wallets_per_run": max_w,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Helius API cost monitor for BlockID")
    ap.add_argument("--max-wallets", type=int, default=None, help="Max wallets per run")
    ap.add_argument("--check-only", action="store_true", help="Only check budget, minimal output")
    args = ap.parse_args()

    r = run_report(args.max_wallets)

    # Print report
    if args.check_only:
        if r["over_budget"]:
            print(f"[helius_cost] OVER_BUDGET: ${r['estimated_cost_today_usd']:.4f} > ${DAILY_LIMIT}")
            return 1
        return 0

    print("[helius_cost_monitor] Usage report:")
    print(f"  Total API calls today: {r['total_calls_today']}")
    print(f"  Estimated cost today: ${r['estimated_cost_today_usd']:.6f} USD")
    print(f"  Estimated cost monthly: ${r['estimated_cost_monthly_usd']:.4f} USD")
    print(f"  Daily limit: ${r['daily_limit_usd']} USD")
    print("  Top 10 expensive wallets:")
    for item in r["top_10_wallets"]:
        print(f"    wallet={item['wallet'][:16]}... calls={item['calls']} cost=${item['cost_usd']:.6f}")
    print(f"  within_limit: {not r['over_budget']}")

    # Save CSV
    date_str = time.strftime("%Y-%m-%d", time.gmtime())
    save_csv_report(date_str, r["total_calls_today"], r["estimated_cost_today_usd"])
    print(f"  report: {CSV_REPORT}")

    # Budget guard
    if r["over_budget"]:
        print("")
        print("WARNING: Estimated cost today exceeds DAILY_LIMIT. Pipeline stopped.")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
