"""
Test wallet reasons API response.

Usage:
  py -m backend_blockid.tools.test_wallet_reasons_api WALLET
"""
from __future__ import annotations

import json
import sys

import requests


def main() -> int:
    wallet = sys.argv[1] if len(sys.argv) > 1 else ""
    if not wallet:
        print("[test_wallet_reasons_api] ERROR: wallet required")
        return 1

    url = f"http://127.0.0.1:8000/trust-score/{wallet}"
    r = requests.get(url, timeout=30)
    print("[test_wallet_reasons_api] status:", r.status_code)
    try:
        print(json.dumps(r.json(), indent=2))
    except Exception:
        print(r.text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
