"""
BlockID Load Testing with Locust.

Stress test FastAPI endpoints before mainnet.
Server: http://localhost:8000 (override with --host)

Usage:
  locust -f backend_blockid/tools/locust_blockid.py
  locust -f backend_blockid/tools/locust_blockid.py --headless -u 100 -r 10 -t 60s
  locust -f backend_blockid/tools/locust_blockid.py --host http://localhost:8000

Open http://localhost:8089 for UI. Suggested: Users 50→500, Spawn rate 10/sec.

Metrics to observe: response time, error rate, DB query time, CPU/RAM, Helius calls.

Future upgrades: distributed Locust workers, Kubernetes load test, Phantom plugin simulation.
"""

from __future__ import annotations

import csv
import os
import random
from pathlib import Path

from locust import HttpUser, task, between, events

# -----------------------------------------------------------------------------
# Wallet list from test_wallets.csv
# -----------------------------------------------------------------------------

WALLET_CSV = Path(__file__).resolve().parent.parent / "data" / "test_wallets.csv"
REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"
LOAD_TEST_CSV = REPORTS_DIR / "load_test_results.csv"

_wallets: list[str] = []


def _load_wallets() -> list[str]:
    global _wallets
    if _wallets:
        return _wallets
    path = WALLET_CSV
    if not path.exists():
        return []
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
            if w and len(w) >= 32:
                _wallets.append(w)
    return _wallets


def random_wallet() -> str:
    """Pick a random wallet from test_wallets.csv."""
    wallets = _load_wallets()
    if not wallets:
        return "Bz2tW98VhBJYUba7xr2bQnkzgSvRfiAQHtaKHzBDijdm"  # fallback
    return random.choice(wallets)


# -----------------------------------------------------------------------------
# Report saving
# -----------------------------------------------------------------------------


@events.quitting.add_listener
def _save_report(environment, **kwargs):
    """Save load test summary to backend_blockid/reports/load_test_results.csv."""
    try:
        stats = getattr(environment, "stats", None)
        if not stats:
            return
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        rows = []
        entries = getattr(stats, "entries", {})
        for key, stat in entries.items():
            if isinstance(key, tuple):
                name, method = key[0], key[1] if len(key) > 1 else "GET"
            else:
                name, method = str(key), getattr(stat, "method", "GET") or "GET"
            if stat.num_requests == 0:
                continue
            rows.append({
                "endpoint": name,
                "method": method,
                "requests": stat.num_requests,
                "failures": stat.num_failures,
                "median_response_ms": round(stat.median_response_time or 0, 1),
                "avg_response_ms": round(stat.avg_response_time or 0, 1),
                "min_ms": round(stat.min_response_time or 0, 1),
                "max_ms": round(stat.max_response_time or 0, 1),
                "rps": round(getattr(stat, "total_rps", 0) or 0, 2),
            })
        if rows:
            with open(LOAD_TEST_CSV, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                writer.writeheader()
                writer.writerows(rows)
            print(f"[locust] Saved report to {LOAD_TEST_CSV}")
    except Exception as e:
        print(f"[locust] Report save failed: {e}")


# -----------------------------------------------------------------------------
# BlockID user
# -----------------------------------------------------------------------------


class BlockIDUser(HttpUser):
    """Load test BlockID API endpoints."""

    wait_time = between(1, 3)
    host = "http://localhost:8000"  # Override: locust --host http://your-server:8000

    @task(5)
    def wallet_profile(self):
        """GET /wallet/{wallet} — trust score and flags."""
        wallet = random_wallet()
        self.client.get(f"/wallet/{wallet}", name="/wallet/{wallet}")

    @task(2)
    def badge(self):
        """GET /wallet/{wallet}/investigation_badge."""
        wallet = random_wallet()
        self.client.get(
            f"/wallet/{wallet}/investigation_badge",
            name="/wallet/{wallet}/investigation_badge",
        )

    @task(2)
    def graph(self):
        """GET /wallet/{wallet}/graph."""
        wallet = random_wallet()
        self.client.get(
            f"/wallet/{wallet}/graph",
            name="/wallet/{wallet}/graph",
        )

    @task(1)
    def report(self):
        """GET /wallet/{wallet}/report — PDF report."""
        wallet = random_wallet()
        self.client.get(
            f"/wallet/{wallet}/report",
            name="/wallet/{wallet}/report",
        )

    @task(3)
    def realtime_update(self):
        """POST /realtime/update_wallet/{wallet} — trigger risk update."""
        wallet = random_wallet()
        self.client.post(
            f"/realtime/update_wallet/{wallet}",
            name="/realtime/update_wallet/{wallet}",
        )

    @task(1)
    def pipeline_batch_update(self):
        """
        Pipeline load test: run realtime risk update for multiple wallets.
        Updates up to 20 wallets per task (scale users to reach 100+ total).
        """
        wallets = _load_wallets()
        batch_size = min(20, len(wallets) or 1)
        selected = random.sample(wallets, batch_size) if len(wallets) >= batch_size else (wallets or [random_wallet()])
        for wallet in selected:
            self.client.post(
                f"/realtime/update_wallet/{wallet}",
                name="/realtime/update_wallet/{wallet}",
            )
