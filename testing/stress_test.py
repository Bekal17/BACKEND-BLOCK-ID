"""
Stress test for the analysis pipeline: concurrent agent-style workload on mock data.

- Simulates 100 wallets and 1000 transactions in an in-memory DB.
- Runs the full analysis path (features → anomaly → trust score → alerts) concurrently.
- Measures latency (percentiles) and memory (tracemalloc; RSS via psutil if available).
- Production-safe: uses an isolated temp SQLite DB and mock data only; never touches production DB.

Run: python -m testing.stress_test
"""

from __future__ import annotations

import argparse
import random
import sys
import tempfile
import time
import tracemalloc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

# Ensure project root is on path when run as __main__
if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

# Base58 alphabet (Solana-style addresses)
_B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _random_base58(length: int = 44, rng: random.Random | None = None) -> str:
    rng = rng or random
    return "".join(rng.choices(_B58, k=length))


def generate_mock_wallets(n: int, seed: int = 42) -> list[str]:
    """Generate n distinct base58-like wallet addresses (deterministic with seed)."""
    rng = random.Random(seed)
    seen: set[str] = set()
    out: list[str] = []
    while len(out) < n:
        addr = _random_base58(44, rng)
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out


def generate_mock_transactions(
    total: int,
    wallet_addresses: list[str],
    seed: int = 43,
) -> list[tuple[str, tuple[str, str, str, int, int | None, int | None]]]:
    """
    Generate `total` mock transaction records distributed across wallets.
    Returns list of (wallet, (signature, sender, receiver, amount_lamports, timestamp, slot)).
    """
    rng = random.Random(seed)
    base_ts = int(time.time()) - 86400 * 90  # ~90 days ago
    records: list[tuple[str, tuple[str, str, str, int, int | None, int | None]]] = []
    sig_counter = 0
    for _ in range(total):
        wallet = rng.choice(wallet_addresses)
        sig_counter += 1
        sig = _random_base58(88, rng)  # signature-like length
        sender = wallet if rng.random() > 0.5 else _random_base58(44, rng)
        receiver = wallet if sender != wallet else _random_base58(44, rng)
        amount = rng.randint(1_000_000, 500_000_000_000)  # 0.001–500 SOL in lamports
        timestamp = base_ts + rng.randint(0, 86400 * 90)
        slot = rng.randint(100_000, 300_000_000)
        records.append(
            (
                wallet,
                (sig, sender, receiver, amount, timestamp, slot),
            )
        )
    return records


def seed_memory_db(
    db: Any,
    wallet_addresses: list[str],
    transactions_by_wallet: dict[str, list[tuple[str, str, str, int, int | None, int | None]]],
) -> None:
    """Insert mock wallets and transactions into the given DB. No production tables touched."""
    for wallet in wallet_addresses:
        db.add_tracked_wallet(wallet)
    for wallet, records in transactions_by_wallet.items():
        if records:
            db.insert_transactions(wallet, records)


def run_single_analysis(
    wallet: str,
    db: Any,
    anomaly_config: Any,
    alert_config: Any,
    max_history: int,
) -> float:
    """
    Run the same analysis path as the periodic agent for one wallet.
    Returns latency in seconds.
    """
    from backend_blockid.agent_worker.runner import _analyze_and_save_wallet

    t0 = time.perf_counter()
    _analyze_and_save_wallet(wallet, db, anomaly_config, alert_config, max_history)
    return time.perf_counter() - t0


def percentile(sorted_values: list[float], p: float) -> float:
    """Linear interpolation percentile (e.g. p50, p95, p99)."""
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * (p / 100.0)
    f = int(k)
    c = f + 1 if f + 1 < len(sorted_values) else f
    return sorted_values[f] + (k - f) * (sorted_values[c] - sorted_values[f])


def run_stress(
    num_wallets: int = 100,
    num_transactions: int = 1000,
    concurrency: int = 16,
    warmup_runs: int = 5,
    seed: int = 42,
) -> dict[str, Any]:
    """
    Execute stress test: in-memory DB, mock data, concurrent analysis runs.
    Returns dict with latency stats, memory stats, and counts.
    """
    from backend_blockid.alerts.engine import AlertConfig
    from backend_blockid.analysis_engine.anomaly import AnomalyConfig
    from backend_blockid.database import get_database

    # Isolated DB: temp file so all threads share same DB; deleted after run. Never touches production.
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        db = get_database(db_path)
        wallets = generate_mock_wallets(num_wallets, seed=seed)
        flat_tx = generate_mock_transactions(num_transactions, wallets, seed=seed + 1)
        by_wallet: dict[str, list[tuple[str, str, str, int, int | None, int | None]]] = {}
        for w, rec in flat_tx:
            by_wallet.setdefault(w, []).append(rec)
        seed_memory_db(db, wallets, by_wallet)

        anomaly_config = AnomalyConfig()
        alert_config = AlertConfig()
        max_history = 500

        for i in range(min(warmup_runs, len(wallets))):
            run_single_analysis(wallets[i], db, anomaly_config, alert_config, max_history)

        tracemalloc.start()
        try:
            start_mem_current, start_mem_peak = tracemalloc.get_traced_memory()
            latencies: list[float] = []
            errors = 0
            t_start = time.perf_counter()
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {
                    executor.submit(
                        run_single_analysis,
                        w,
                        db,
                        anomaly_config,
                        alert_config,
                        max_history,
                    ): w
                    for w in wallets
                }
                for fut in as_completed(futures):
                    try:
                        lat = fut.result()
                        latencies.append(lat)
                    except Exception:
                        errors += 1
            t_end = time.perf_counter()
            end_mem_current, end_mem_peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()

        total_wall_s = t_end - t_start
        latencies.sort()
        n = len(latencies)
        return {
            "num_wallets": num_wallets,
            "num_transactions": num_transactions,
            "concurrency": concurrency,
            "completed": n,
            "errors": errors,
            "wall_sec": total_wall_s,
            "throughput_wallets_per_sec": n / total_wall_s if total_wall_s > 0 else 0,
            "latency_sec": {
                "min": latencies[0] if latencies else 0,
                "max": latencies[-1] if latencies else 0,
                "mean": sum(latencies) / n if n else 0,
                "p50": percentile(latencies, 50),
                "p95": percentile(latencies, 95),
                "p99": percentile(latencies, 99),
            },
            "memory_tracemalloc": {
                "start_current_mb": start_mem_current / (1024 * 1024),
                "end_current_mb": end_mem_current / (1024 * 1024),
                "peak_mb": end_mem_peak / (1024 * 1024),
            },
        }
    finally:
        Path(db_path).unlink(missing_ok=True)


def get_rss_mb() -> float | None:
    """RSS in MB if psutil is available."""
    try:
        import psutil
        return psutil.Process().memory_info().rss / (1024 * 1024)
    except ImportError:
        return None


def print_report(results: dict[str, Any], include_rss: bool = True) -> None:
    """Print a clear performance report to stdout."""
    rss = get_rss_mb() if include_rss else None
    lat = results["latency_sec"]
    mem = results["memory_tracemalloc"]

    lines = [
        "",
        "=" * 60,
        "STRESS TEST PERFORMANCE REPORT",
        "=" * 60,
        f"  Wallets simulated:     {results['num_wallets']}",
        f"  Transactions total:   {results['num_transactions']}",
        f"  Concurrency:          {results['concurrency']}",
        f"  Completed:            {results['completed']}",
        f"  Errors:               {results['errors']}",
        "",
        "  Latency (per wallet analysis)",
        "  --------------------------------",
        f"    Min:    {lat['min']:.4f} s",
        f"    Mean:   {lat['mean']:.4f} s",
        f"    P50:    {lat['p50']:.4f} s",
        f"    P95:    {lat['p95']:.4f} s",
        f"    P99:    {lat['p99']:.4f} s",
        f"    Max:    {lat['max']:.4f} s",
        "",
        "  Throughput",
        "  --------------------------------",
        f"    Wall time:           {results['wall_sec']:.2f} s",
        f"    Wallets / second:    {results['throughput_wallets_per_sec']:.2f}",
        "",
        "  Memory (tracemalloc)",
        "  --------------------------------",
        f"    Start (current):     {mem['start_current_mb']:.2f} MB",
        f"    End (current):       {mem['end_current_mb']:.2f} MB",
        f"    Peak:                {mem['peak_mb']:.2f} MB",
    ]
    if rss is not None:
        lines.append(f"    Process RSS:          {rss:.2f} MB")
    lines.extend(["", "  Production DB: NOT USED (isolated temp DB)", "=" * 60, ""])
    print("\n".join(lines))


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Stress test analysis pipeline with mock data (production-safe)."
    )
    parser.add_argument(
        "--wallets",
        type=int,
        default=100,
        help="Number of mock wallets (default: 100)",
    )
    parser.add_argument(
        "--transactions",
        type=int,
        default=1000,
        help="Total mock transactions (default: 1000)",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=16,
        help="Concurrent analysis workers (default: 16)",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=5,
        help="Warmup runs before timed phase (default: 5)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--no-rss",
        action="store_true",
        help="Skip RSS memory (psutil) in report",
    )
    args = parser.parse_args()

    if args.wallets < 1 or args.transactions < 1:
        print("error: --wallets and --transactions must be >= 1", file=sys.stderr)
        return 1
    if args.concurrency < 1:
        print("error: --concurrency must be >= 1", file=sys.stderr)
        return 1

    results = run_stress(
        num_wallets=args.wallets,
        num_transactions=args.transactions,
        concurrency=args.concurrency,
        warmup_runs=args.warmup,
        seed=args.seed,
    )
    print_report(results, include_rss=not args.no_rss)
    return 0 if results["errors"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
