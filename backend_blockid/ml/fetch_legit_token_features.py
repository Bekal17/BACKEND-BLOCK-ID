"""
Fetch token features for legitimate wallets and append to token_features.csv.
Used to improve ML model training data with more legitimate wallet examples.
"""
import asyncio
import csv
import os
import time
from pathlib import Path

import httpx

HELIUS_API_KEY = os.environ.get("HELIUS_API_KEY", "")
HELIUS_RPC_URL = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"

_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"

ADDITIONAL_LEGIT_CSV = _DATA_DIR / "additional_legit_wallets.csv"
TOKEN_FEATURES_CSV = _DATA_DIR / "token_features.csv"

FEATURE_COLUMNS = [
    "wallet", "token_id", "interface",
    "mint_authority_exists", "freeze_authority_exists",
    "metadata_missing", "decimals", "supply",
    "is_mutable", "is_compressed", "has_unverified_creator",
    "creator_wallets", "token_name", "token_symbol",
    "is_burnt", "scam_flag",
]


def safe_num(x, default=0):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


async def fetch_token_features(wallet: str, client: httpx.AsyncClient) -> list[dict]:
    """Fetch token features for a wallet via Helius DAS getAssetsByOwner."""
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
                    "showNativeBalance": False,
                },
            },
        }
        resp = await client.post(HELIUS_RPC_URL, json=payload, timeout=30)
        if resp.status_code != 200:
            print(f"  [WARN] HTTP {resp.status_code} for {wallet[:16]}")
            return []

        data = resp.json()
        if "error" in data or "result" not in data:
            print(f"  [WARN] API error for {wallet[:16]}: {data.get('error')}")
            return []

        items = data["result"].get("items", [])
        if not items:
            print(f"  [INFO] No tokens for {wallet[:16]}")
            return []

        rows = []
        for item in items:
            token_info = item.get("token_info") or {}
            ownership = item.get("ownership") or {}
            authorities = item.get("authorities") or []
            creators = item.get("creators") or []
            compression = item.get("compression") or {}
            content = item.get("content") or {}
            metadata = content.get("metadata") or {}

            has_mint_authority = 0
            has_freeze_authority = 0
            for auth in authorities:
                scopes = auth.get("scopes", [])
                if isinstance(scopes, list):
                    if "full" in scopes:
                        has_mint_authority = 1
                    if "freeze" in scopes:
                        has_freeze_authority = 1
            if ownership.get("frozen", False):
                has_freeze_authority = 1

            metadata_missing = 1 if not metadata.get("name") else 0
            is_mutable = 1 if item.get("mutable", False) else 0
            is_compressed = 1 if compression.get("compressed", False) else 0
            is_burnt = 1 if item.get("burnt", False) else 0

            has_unverified_creator = 0
            creator_wallet_list = []
            for c in creators:
                if isinstance(c, dict):
                    if not c.get("verified", False):
                        has_unverified_creator = 1
                    addr = c.get("address", "")
                    if addr:
                        creator_wallet_list.append(addr)

            rows.append({
                "wallet": wallet,
                "token_id": item.get("id", ""),
                "interface": item.get("interface", ""),
                "mint_authority_exists": has_mint_authority,
                "freeze_authority_exists": has_freeze_authority,
                "metadata_missing": metadata_missing,
                "decimals": int(safe_num(token_info.get("decimals"), 0)),
                "supply": safe_num(token_info.get("supply"), 0),
                "is_mutable": is_mutable,
                "is_compressed": is_compressed,
                "has_unverified_creator": has_unverified_creator,
                "creator_wallets": "|".join(creator_wallet_list),
                "token_name": metadata.get("name", "") or token_info.get("symbol", ""),
                "token_symbol": token_info.get("symbol", ""),
                "is_burnt": is_burnt,
                "scam_flag": 0,  # All wallets in additional_legit_wallets.csv are legitimate
            })

        return rows

    except Exception as e:
        print(f"  [ERROR] {wallet[:16]}: {e}")
        return []


async def main():
    if not HELIUS_API_KEY:
        print("[ERROR] HELIUS_API_KEY not set")
        return

    # Load wallets
    wallets = []
    with open(ADDITIONAL_LEGIT_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            w = row.get("wallet", "").strip()
            if w:
                wallets.append(w)

    print(f"[INFO] Fetching token features for {len(wallets)} wallets...")

    # Check existing wallets in token_features.csv to avoid duplicates
    existing_wallets = set()
    if TOKEN_FEATURES_CSV.exists():
        with open(TOKEN_FEATURES_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = row.get("wallet", "").strip()
                if w:
                    existing_wallets.add(w)
    print(f"[INFO] Existing wallets in token_features.csv: {len(existing_wallets)}")

    new_wallets = [w for w in wallets if w not in existing_wallets]
    print(f"[INFO] New wallets to fetch: {len(new_wallets)}")

    if not new_wallets:
        print("[INFO] No new wallets to fetch. Exiting.")
        return

    # Fetch and append
    total_rows = 0
    async with httpx.AsyncClient() as client:
        # Check if file exists for header
        file_exists = TOKEN_FEATURES_CSV.exists()

        with open(TOKEN_FEATURES_CSV, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FEATURE_COLUMNS)
            if not file_exists:
                writer.writeheader()

            for i, wallet in enumerate(new_wallets):
                print(f"[{i+1}/{len(new_wallets)}] Fetching {wallet[:16]}...")
                rows = await fetch_token_features(wallet, client)
                if rows:
                    writer.writerows(rows)
                    total_rows += len(rows)
                    print(f"  → {len(rows)} tokens appended")
                else:
                    # Append synthetic row for wallets with no tokens
                    writer.writerow({
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
                        "scam_flag": 0,
                    })
                    total_rows += 1
                    print(f"  → synthetic row appended (no tokens found)")

                await asyncio.sleep(0.2)  # rate limit

    print(f"\n[DONE] Appended {total_rows} rows to {TOKEN_FEATURES_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
