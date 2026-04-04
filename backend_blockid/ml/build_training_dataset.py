"""
Build training dataset for BlockID token scam detection model.
Fetches real token data from Helius DAS API for labeled wallets,
extracts features, and saves to token_features.csv + scam_wallets.csv.

Usage:
    py backend_blockid/ml/build_training_dataset.py
"""

from __future__ import annotations

import asyncio
import csv
import os
from pathlib import Path

import httpx

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"

# Input: labeled wallet dataset
LABELED_WALLETS_CSV = _DATA_DIR / "labeled_wallets_dataset.csv"

# Output: training data
TOKEN_FEATURES_CSV = _DATA_DIR / "token_features.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"


async def fetch_wallet_tokens(wallet: str, client: httpx.AsyncClient) -> list[dict]:
    """Fetch token/asset data from Helius DAS API for a single wallet."""
    try:
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getAssetsByOwner",
            "params": {
                "ownerAddress": wallet,
                "page": 1,
                "limit": 100,
                "displayOptions": {
                    "showFungible": True,
                    "showNativeBalance": True,
                },
            },
        }
        resp = await client.post(HELIUS_RPC_URL, json=payload)
        if resp.status_code != 200:
            print(f"  [WARN] HTTP {resp.status_code} for {wallet[:16]}...")
            return []
        data = resp.json()

        if "error" in data or "result" not in data:
            print(f"  [WARN] DAS error for {wallet[:16]}...: {data.get('error')}")
            return []

        items = data["result"].get("items", [])
        tokens = []

        for item in items:
            token_info = item.get("token_info") or {}
            ownership = item.get("ownership") or {}
            authorities = item.get("authorities") or []
            creators = item.get("creators") or []
            content = item.get("content") or {}
            metadata = content.get("metadata") or {}
            compression = item.get("compression") or {}

            has_mint_authority = 0
            has_freeze_authority = 0
            creator_wallets = []

            for auth in authorities:
                scopes = auth.get("scopes", [])
                if isinstance(scopes, list):
                    if "full" in scopes:
                        has_mint_authority = 1
                    if "freeze" in scopes:
                        has_freeze_authority = 1

            if ownership.get("frozen", False):
                has_freeze_authority = 1

            for c in creators:
                if isinstance(c, dict):
                    addr = c.get("address", "")
                    if addr:
                        creator_wallets.append(addr)

            metadata_missing = 1 if not metadata.get("name") else 0
            is_mutable = 1 if item.get("mutable", False) else 0
            is_compressed = 1 if compression.get("compressed", False) else 0

            has_unverified_creator = 0
            for c in creators:
                if isinstance(c, dict) and not c.get("verified", False):
                    has_unverified_creator = 1
                    break

            decimals = token_info.get("decimals", 0)
            supply = token_info.get("supply", 0)

            tokens.append(
                {
                    "wallet": wallet,
                    "token_id": item.get("id", ""),
                    "interface": item.get("interface", ""),
                    "mint_authority_exists": has_mint_authority,
                    "freeze_authority_exists": has_freeze_authority,
                    "metadata_missing": metadata_missing,
                    "decimals": decimals if decimals is not None else 0,
                    "supply": supply if supply is not None else 0,
                    "is_mutable": is_mutable,
                    "is_compressed": is_compressed,
                    "has_unverified_creator": has_unverified_creator,
                    "creator_wallets": ";".join(creator_wallets),
                    "token_name": metadata.get("name", ""),
                    "token_symbol": metadata.get("symbol", ""),
                    "is_burnt": 1 if item.get("burnt", False) else 0,
                }
            )

        return tokens

    except Exception as e:
        print(f"  [ERROR] Failed for {wallet[:16]}...: {e}")
        return []


async def main() -> None:
    if not HELIUS_API_KEY:
        print("[ERROR] HELIUS_API_KEY not set")
        return

    if not LABELED_WALLETS_CSV.exists():
        print(f"[ERROR] {LABELED_WALLETS_CSV} not found")
        return

    wallets = []
    scam_wallets: set[str] = set()
    with open(LABELED_WALLETS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = row.get("wallet", "").strip()
            label = row.get("label", "").strip()
            if wallet and len(wallet) >= 32:
                wallets.append({"wallet": wallet, "label": label})
                if label in ("scam", "suspect"):
                    scam_wallets.add(wallet)

    print(f"[INFO] Loaded {len(wallets)} wallets ({len(scam_wallets)} scam/suspect)")

    all_token_rows: list[dict] = []
    async with httpx.AsyncClient(timeout=30) as client:
        for i, w in enumerate(wallets):
            wallet = w["wallet"]
            label = w["label"]
            print(f"[{i + 1}/{len(wallets)}] Fetching {wallet[:20]}... ({label})")

            tokens = await fetch_wallet_tokens(wallet, client)

            if tokens:
                scam_flag = 1 if label in ("scam", "suspect") else 0
                for t in tokens:
                    t["scam_flag"] = scam_flag
                all_token_rows.extend(tokens)
                print(f"  -> {len(tokens)} tokens found")
            else:
                all_token_rows.append(
                    {
                        "wallet": wallet,
                        "token_id": "",
                        "interface": "",
                        "mint_authority_exists": 0,
                        "freeze_authority_exists": 0,
                        "metadata_missing": 1,
                        "decimals": 0,
                        "supply": 0,
                        "is_mutable": 0,
                        "is_compressed": 0,
                        "has_unverified_creator": 0,
                        "creator_wallets": "",
                        "token_name": "",
                        "token_symbol": "",
                        "is_burnt": 0,
                        "scam_flag": 1 if label in ("scam", "suspect") else 0,
                    }
                )
                print("  -> 0 tokens (empty row added)")

            await asyncio.sleep(0.1)

    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "wallet",
        "token_id",
        "interface",
        "mint_authority_exists",
        "freeze_authority_exists",
        "metadata_missing",
        "decimals",
        "supply",
        "is_mutable",
        "is_compressed",
        "has_unverified_creator",
        "creator_wallets",
        "token_name",
        "token_symbol",
        "is_burnt",
        "scam_flag",
    ]

    with open(TOKEN_FEATURES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_token_rows)

    print(f"\n[INFO] Saved {len(all_token_rows)} token rows to {TOKEN_FEATURES_CSV}")

    with open(SCAM_WALLETS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["wallet"])
        writer.writeheader()
        for w in sorted(scam_wallets):
            writer.writerow({"wallet": w})

    print(f"[INFO] Saved {len(scam_wallets)} scam wallets to {SCAM_WALLETS_CSV}")

    scam_tokens = sum(1 for r in all_token_rows if r["scam_flag"] == 1)
    legit_tokens = sum(1 for r in all_token_rows if r["scam_flag"] == 0)
    print("\n[SUMMARY]")
    print(f"  Total token rows: {len(all_token_rows)}")
    print(f"  Scam tokens:      {scam_tokens}")
    print(f"  Legit tokens:     {legit_tokens}")
    print(f"  Scam ratio:       {scam_tokens / max(1, len(all_token_rows)) * 100:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
