#!/usr/bin/env python3
"""
Generate test wallets with labels for BlockID testing.

Produces 20 wallets with labels:
  - 5 NEW_WALLET
  - 5 HIGH_OUTFLOW
  - 5 DRAINER_INTERACTION
  - 5 SCAM_CLUSTER_MEMBER

Usage:
  python backend_blockid/tools/generate_test_wallets.py

Output: backend_blockid/data/test_wallets.csv
"""

from __future__ import annotations

import csv
from pathlib import Path

from solders.keypair import Keypair

_SCRIPT_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _SCRIPT_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
_OUTPUT_PATH = _DATA_DIR / "test_wallets.csv"

LABELS = [
    ("NEW_WALLET", 5),
    ("HIGH_OUTFLOW", 5),
    ("DRAINER_INTERACTION", 5),
    ("SCAM_CLUSTER_MEMBER", 5),
]


def generate_valid_solana_wallet() -> str:
    """Generate a valid Solana wallet address from a new keypair."""
    return str(Keypair().pubkey())


def main() -> int:
    seen: set[str] = set()
    rows: list[tuple[str, str]] = []

    for label, count in LABELS:
        for _ in range(count):
            while True:
                addr = generate_valid_solana_wallet()
                if addr not in seen:
                    seen.add(addr)
                    break
            rows.append((addr, label))

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(_OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "label"])
        writer.writerows(rows)

    print(f"Generated {len(rows)} wallets:")
    for addr, label in rows:
        print(f"  {addr}  {label}")
    print(f"Written to {_OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
