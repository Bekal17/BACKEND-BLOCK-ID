from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import List, Dict, Any

import httpx


def _project_root() -> Path:
    # backend_blockid/tools/.. -> project root
    return Path(__file__).resolve().parents[2]


def _helius_rpc_url() -> str | None:
    """
    Resolve Helius RPC URL for getAssets calls, using HELIUS_API_KEY / SOLANA_RPC_URL.
    """
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return None


def fetch_latest_solana_tokens(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch latest Solana token mints via Helius getAssets API (JSON-RPC).

    Returns:
        List of asset dicts; we primarily care about 'id' (mint address).
    """
    rpc_url = _helius_rpc_url()
    if not rpc_url:
        raise RuntimeError("HELIUS_API_KEY or SOLANA_RPC_URL must be set for getAssets")

    body = {
        "jsonrpc": "2.0",
        "id": "get-assets-latest",
        "method": "getAssets",
        "params": {
            # Helius getAssets supports pagination and sorting; here we sort by created desc.
            "page": 1,
            "limit": limit,
            "sortBy": {
                "sortBy": "created",
                "sortDirection": "desc",
            },
        },
    }

    with httpx.Client(timeout=20.0) as client:
        resp = client.post(rpc_url, json=body)
        resp.raise_for_status()
        data = resp.json()

    result = data.get("result")
    if not isinstance(result, dict):
        return []
    items = result.get("items") or []
    if not isinstance(items, list):
        return []
    return items


def save_suspicious_tokens(tokens: List[Dict[str, Any]]) -> Path:
    """
    Save token mints to backend_blockid/data/suspicious_tokens.csv.
    """
    root = _project_root()
    data_dir = root / "backend_blockid" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "suspicious_tokens.csv"

    # Existing set of mints to avoid duplicates
    existing_mints = set()
    if out_path.exists():
        with open(out_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and "mint" in reader.fieldnames:
                for row in reader:
                    mint = (row.get("mint") or "").strip()
                    if mint:
                        existing_mints.add(mint)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["mint", "reason"])

        # Re-write existing mints first
        for m in sorted(existing_mints):
            writer.writerow([m, "existing"])

        # Append new mints from Helius getAssets
        new_count = 0
        for t in tokens:
            # Helius getAssets uses 'id' as the asset address (mint)
            mint = (t.get("id") or "").strip()
            if not mint or mint in existing_mints:
                continue
            writer.writerow([mint, "helius_new"])
            new_count += 1

    print("Suspicious tokens file:", out_path)
    print("Existing mints kept:", len(existing_mints))
    print("New mints added from Helius getAssets:", new_count)
    return out_path


def main() -> int:
    print("fetch_new_tokens started (Helius getAssets)")
    try:
        tokens = fetch_latest_solana_tokens(limit=100)
    except Exception as e:  # noqa: BLE001
        print("Error fetching tokens from Helius getAssets:", e)
        return 1

    if not tokens:
        print("No tokens returned from Helius getAssets.")
        return 0

    save_suspicious_tokens(tokens)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


