"""
BlockID – Scan 100 Real Tokens using Helius
Author: Bekal BlockID Project

Scans token mints via Helius getAsset, extracts fields, writes backend_blockid/data/token_features.csv.
Run from project root: py backend_blockid/oracle/scan_100_tokens_helius.py
"""

import os
import time
import csv
import requests
from pathlib import Path
from dotenv import load_dotenv

_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT = _SCRIPT_DIR.parents[1]
_DATA_DIR = _SCRIPT_DIR.parent / "data"
OUTPUT = _DATA_DIR / "token_features.csv"

load_dotenv(_ROOT / ".env")

API_KEY = os.getenv("HELIUS_API_KEY")
RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={API_KEY}" if API_KEY else os.getenv("SOLANA_RPC_URL", "").strip()


# ============================
# SAMPLE REAL TOKENS
# ============================
# Popular mainnet tokens (BONK mint corrected)
TOKENS = [
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "So11111111111111111111111111111111111111112",   # SOL wrapped
    "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",  # BONK
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",   # USDT
]

# Duplicate until 100 for testing
TOKENS = (TOKENS * 30)[:100]


# ============================
# FETCH ASSET
# ============================
def fetch_asset(mint):
    payload = {
        "jsonrpc": "2.0",
        "id": "blockid-scan",
        "method": "getAsset",
        "params": {"id": mint},
    }

    r = requests.post(RPC_URL, json=payload, timeout=20)
    data = r.json()

    if "error" in data:
        print("[helius_scan] error:", mint, data["error"])
        return None

    return data.get("result")


# ============================
# EXTRACT FIELDS
# ============================
def extract(asset, mint):
    content = asset.get("content") or {}
    metadata = content.get("metadata") or {}
    token_info = asset.get("token_info") or content.get("token_info") or {}

    name = metadata.get("name")
    symbol = metadata.get("symbol")

    mint_auth = token_info.get("mint_authority")
    freeze_auth = token_info.get("freeze_authority")
    supply = token_info.get("supply")
    decimals = token_info.get("decimals")

    scam_flag = (
        mint_auth is not None
        or freeze_auth is not None
        or name is None
    )

    creators = asset.get("authorities") or []
    creator_wallets = [c.get("address") for c in creators if isinstance(c, dict) and c.get("address")]

    return [
        mint,
        name,
        symbol,
        mint_auth,
        freeze_auth,
        supply,
        decimals,
        name is None,
        scam_flag,
        str(creator_wallets),
    ]


# ============================
# MAIN
# ============================
def main():
    if not RPC_URL:
        print("[helius_scan] ERROR: HELIUS_API_KEY or SOLANA_RPC_URL not set")
        return 1

    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "mint",
            "name",
            "symbol",
            "mint_authority",
            "freeze_authority",
            "supply",
            "decimals",
            "metadata_missing",
            "scam_flag",
            "creator_wallets",
        ])

        for i, mint in enumerate(TOKENS, 1):
            print(f"[helius_scan] {i}/100 scanning {mint}")

            asset = fetch_asset(mint)
            if asset:
                row = extract(asset, mint)
                writer.writerow(row)

            time.sleep(0.2)

    print("\n[helius_scan] token_features.csv created at", OUTPUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
