#!/usr/bin/env python3
"""
Test RPC endpoints and print latency ranking.

Usage:
  python -m backend_blockid.tools.test_rpc_endpoints

Shows each endpoint's health status and latency (ms).
TEST_MODE=1 or SOLANA_NETWORK=devnet uses devnet endpoints only.
"""

from __future__ import annotations

import sys

from backend_blockid.config.env import load_blockid_env
from backend_blockid.config.rpc_endpoints import get_rpc_endpoints
from backend_blockid.oracle.rpc_health import check_rpc_health

load_blockid_env()


def _safe_url(url: str) -> str:
    if "api-key=" in url:
        return url.split("?")[0] + "?api-key=***"
    return url


def main() -> int:
    endpoints = get_rpc_endpoints()
    if not endpoints:
        print("No RPC endpoints configured. Set HELIUS_API_KEY or RPC_ENDPOINTS.", file=sys.stderr)
        return 1

    results: list[tuple[str, bool, float | None]] = []
    for url in endpoints:
        ok, latency = check_rpc_health(url)
        results.append((url, ok, latency))

    results.sort(key=lambda x: (not x[1], (x[2] or float("inf"))))

    print("RPC endpoint latency ranking:\n")
    for i, (url, ok, latency) in enumerate(results, 1):
        status = "OK" if ok else "FAIL"
        lat_str = f"{latency * 1000:.0f} ms" if latency is not None else "N/A"
        print(f"  {i}. {status:4}  {lat_str:>10}  {_safe_url(url)}")
    print()
    healthy = sum(1 for _, ok, _ in results if ok)
    print(f"Healthy: {healthy}/{len(results)}")
    return 0 if healthy > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
