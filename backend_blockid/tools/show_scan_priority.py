"""
BlockID Scan Priority CLI.

Prints top wallets by scan priority score.

Usage:
  py -m backend_blockid.tools.show_scan_priority
  py -m backend_blockid.tools.show_scan_priority --limit 20
  py -m backend_blockid.tools.show_scan_priority --test-mode
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.oracle.wallet_scan_prioritizer import (
    get_prioritized_wallets_with_scores,
    MAX_WALLETS_PER_RUN,
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Show top wallets by scan priority")
    ap.add_argument("--limit", type=int, default=50, help="Number of wallets to show")
    ap.add_argument("--test-mode", action="store_true", help="Use only test_wallets.csv")
    ap.add_argument("--max-wallets", type=int, default=None, help="Max wallets to prioritize")
    args = ap.parse_args()

    if args.test_mode:
        os.environ["BLOCKID_TEST_MODE"] = "1"

    scored = get_prioritized_wallets_with_scores(
        max_wallets=args.max_wallets or MAX_WALLETS_PER_RUN,
        test_mode=args.test_mode,
    )

    n_show = min(args.limit, len(scored))
    print(f"[show_scan_priority] Top {n_show} wallets (of {len(scored)} prioritized)")
    print("-" * 70)
    for i, (w, score, reason) in enumerate(scored[: args.limit], 1):
        short = w[:16] + "..." if len(w) > 16 else w
        print(f"  {i:3}. {short}  score={score:.2f}  reason={reason}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
