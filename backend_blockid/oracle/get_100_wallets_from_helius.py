"""
Fetch 100 wallet addresses (token holders) from a token mint using Helius RPC getTokenAccounts.

Usage:
    py backend_blockid/oracle/get_100_wallets_from_helius.py [MINT]

Default mint: USDC mainnet.
Saves first 100 unique owner wallets to backend_blockid/data/test_wallets_100.csv.
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

# Paths: script in backend_blockid/oracle/, data in backend_blockid/data/
_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT = _SCRIPT_DIR.parents[2]
_DATA_DIR = _SCRIPT_DIR.parent / "data"
OUTPUT_CSV = _DATA_DIR / "test_wallets_100.csv"
TARGET_WALLET_COUNT = 100

# USDC mainnet default
DEFAULT_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

load_dotenv(_ROOT / ".env")


def _helius_rpc_url() -> str | None:
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return None


def get_token_accounts(mint: str, limit: int = 1000, cursor: str | None = None) -> dict[str, Any] | None:
    """
    Call Helius getTokenAccounts RPC for a mint. Returns result dict or None on error.
    Result has token_accounts (list of { address, mint, owner, amount, ... }).
    """
    rpc_url = _helius_rpc_url()
    if not rpc_url:
        print("[helius] ERROR: HELIUS_API_KEY or SOLANA_RPC_URL not set")
        return None

    params: dict[str, Any] = {"mint": mint, "limit": limit}
    if cursor:
        params["cursor"] = cursor

    payload = {
        "jsonrpc": "2.0",
        "id": "blockid-get-token-accounts",
        "method": "getTokenAccounts",
        "params": params,
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print("[helius] ERROR: RPC request failed:", e)
        return None
    except ValueError as e:
        print("[helius] ERROR: invalid JSON response:", e)
        return None

    err = data.get("error")
    if err:
        print("[helius] ERROR: RPC error:", err)
        return None

    return data.get("result")


def main() -> int:
    mint = (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_MINT).strip()
    if not mint:
        mint = DEFAULT_MINT

    print("[helius] fetching holders for mint:", mint)
    result = get_token_accounts(mint, limit=1000)
    if result is None:
        return 1

    token_accounts = result.get("token_accounts") or []
    if not isinstance(token_accounts, list):
        token_accounts = []

    seen: set[str] = set()
    wallets: list[str] = []
    for acc in token_accounts:
        if not isinstance(acc, dict):
            continue
        owner = (acc.get("owner") or "").strip()
        if owner and owner not in seen:
            seen.add(owner)
            wallets.append(owner)
            print("[helius] wallet found", owner)
        if len(wallets) >= TARGET_WALLET_COUNT:
            break

    # If first page had fewer than 100 unique owners, paginate
    cursor = result.get("cursor")
    while len(wallets) < TARGET_WALLET_COUNT and cursor:
        result = get_token_accounts(mint, limit=1000, cursor=cursor)
        if result is None:
            break
        token_accounts = result.get("token_accounts") or []
        if not isinstance(token_accounts, list):
            break
        for acc in token_accounts:
            if not isinstance(acc, dict):
                continue
            owner = (acc.get("owner") or "").strip()
            if owner and owner not in seen:
                seen.add(owner)
                wallets.append(owner)
                print("[helius] wallet found", owner)
            if len(wallets) >= TARGET_WALLET_COUNT:
                break
        cursor = result.get("cursor")
        if not cursor:
            break

    wallets = wallets[:TARGET_WALLET_COUNT]
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["wallet"])
        for waddr in wallets:
            w.writerow([waddr])

    print("[helius] saved", len(wallets), "wallets to", OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
