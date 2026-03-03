"""
BlockID Review Queue CLI.

Commands:
  py -m backend_blockid.tools.review_queue_cli list
  py -m backend_blockid.tools.review_queue_cli approve WALLET
  py -m backend_blockid.tools.review_queue_cli reject WALLET
"""
from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root on path
_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.tools.review_queue_engine import approve, list_pending, reject


def main() -> int:
    args = sys.argv[1:]
    if not args:
        print("Usage: py -m backend_blockid.tools.review_queue_cli list | approve WALLET | reject WALLET")
        return 1

    cmd = args[0].lower()
    if cmd == "list":
        items = list_pending()
        if not items:
            print("[review_queue] No pending items")
            return 0
        print(f"[review_queue] {len(items)} pending:")
        for r in items:
            wallet = r.get("wallet", "?")
            score = r.get("score", "?")
            reasons = r.get("reasons", "[]")
            print(f"  {wallet[:20]}... score={score} reasons={reasons}")
        return 0

    if cmd == "approve":
        if len(args) < 2:
            print("Usage: review_queue_cli approve WALLET")
            return 1
        wallet = args[1].strip()
        if approve(wallet):
            print(f"[review_queue] approved {wallet[:20]}...")
            return 0
        print(f"[review_queue] wallet not found: {wallet[:20]}...")
        return 1

    if cmd == "reject":
        if len(args) < 2:
            print("Usage: review_queue_cli reject WALLET")
            return 1
        wallet = args[1].strip()
        if reject(wallet):
            print(f"[review_queue] rejected {wallet[:20]}...")
            return 0
        print(f"[review_queue] wallet not found: {wallet[:20]}...")
        return 1

    print("Unknown command:", cmd)
    return 1


if __name__ == "__main__":
    sys.exit(main())
