
from __future__ import annotations

import csv
import os
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
from dotenv import load_dotenv


# Load .env from project root so RPC/API keys are available regardless of CWD.
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

RPC = os.getenv("SOLANA_RPC_URL")
API_KEY = os.getenv("HELIUS_API_KEY")

print("Loaded RPC:", RPC)
print("Loaded API key:", "YES" if API_KEY else "NO")

if not RPC:
    raise Exception("SOLANA_RPC_URL not set in .env")


def _load_existing_scam_wallets(path: Path) -> Dict[str, str]:
    """
    Load existing scam_wallets.csv as wallet -> label mapping.
    Returns empty dict if file doesn't exist.
    """
    existing: Dict[str, str] = {}
    if not path.exists():
        return existing

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "wallet" not in reader.fieldnames:
            return existing
        for row in reader:
            wallet = (row.get("wallet") or "").strip()
            if not wallet or wallet in existing:
                continue
            label = (row.get("label") or "scam").strip() or "scam"
            existing[wallet] = label
    return existing


def _save_scam_wallets(path: Path, wallets: Dict[str, str]) -> None:
    """Persist wallet -> label mapping back to CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["wallet", "label"])
        for wallet, label in wallets.items():
            writer.writerow([wallet, label])


def _load_suspicious_tokens(path: Path) -> List[Tuple[str, str]]:
    """
    Load suspicious token mints from CSV.

    Expected format (recommended):
        mint,reason
        So1111...,airdrop_scam

    If no header, treats each line as 'mint[,reason]'. Reason defaults to 'suspicious_token'.
    """
    tokens: List[Tuple[str, str]] = []
    if not path.exists():
        print(f"No suspicious_tokens.csv found at: {path}")
        return tokens

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and "mint" in reader.fieldnames:
            for row in reader:
                mint = (row.get("mint") or "").strip()
                if not mint:
                    continue
                reason = (row.get("reason") or "suspicious_token").strip() or "suspicious_token"
                tokens.append((mint, reason))
        else:
            # Headerless: each line is "mint[,reason]"
            f.seek(0)
            for line in f:
                parts = [p.strip() for p in line.strip().split(",")]
                if not parts or not parts[0]:
                    continue
                mint = parts[0]
                reason = parts[1] if len(parts) > 1 and parts[1] else "suspicious_token"
                tokens.append((mint, reason))

    print(f"Loaded suspicious tokens: {len(tokens)} from {path}")
    return tokens


def _helius_rpc_url() -> str | None:
    """
    Resolve Helius RPC URL for getAsset calls.

    Priority:
      1. SOLANA_RPC_URL (if it points to a Helius RPC)
      2. HELIUS_API_KEY -> https://mainnet.helius-rpc.com/?api-key=KEY
    """
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return None


def _fetch_creators_for_mint(client: httpx.Client, mint: str) -> List[str]:
    """
    Call Helius getAsset for a given mint and extract creator addresses.

    This uses the JSON-RPC getAsset method and is defensive about response shape.
    """
    rpc_url = _helius_rpc_url()
    if not rpc_url:
        print("HELIUS RPC URL not configured (SOLANA_RPC_URL / HELIUS_API_KEY); skipping getAsset")
        return []

    body = {
        "jsonrpc": "2.0",
        "id": "get-asset",
        "method": "getAsset",
        "params": {
            "id": mint,
        },
    }
    try:
        resp = client.post(rpc_url, json=body, timeout=20.0)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:  # noqa: BLE001
        print(f"Helius getAsset failed for {mint}: {e}")
        return []

    result = data.get("result")
    if not isinstance(result, dict):
        return []

    # Helius getAsset: result.content.metadata.creators or similar
    creators: List[str] = []
    try:
        content = result.get("content") or {}
        metadata = content.get("metadata") or {}
        # Two possible shapes used in Helius docs: metadata.creators or metadata.data.creators
        raw_creators = metadata.get("creators")
        if raw_creators is None:
            raw_creators = (metadata.get("data") or {}).get("creators")
        if not isinstance(raw_creators, list):
            return []
        for entry in raw_creators:
            if isinstance(entry, dict):
                addr = (entry.get("address") or entry.get("creator") or "").strip()
                if addr:
                    creators.append(addr)
    except Exception:
        return creators

    return creators


def main() -> int:
    print("auto collect started")

    # Project root = two levels above this tools module
    root = Path(__file__).resolve().parents[2]
    data_dir = root / "backend_blockid" / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    scam_file = data_dir / "scam_wallets.csv"
    suspicious_tokens_file = data_dir / "suspicious_tokens.csv"

    print("Project root:", root)
    print("Scam wallets CSV path:", scam_file)

    existing = _load_existing_scam_wallets(scam_file)

    if not scam_file.exists():
        # Ensure file exists with header even if there are no wallets yet
        _save_scam_wallets(scam_file, existing)
        print("Created new scam_wallets.csv")

    # Load suspicious tokens to drive collection
    suspicious_tokens = _load_suspicious_tokens(suspicious_tokens_file)
    if not suspicious_tokens:
        print("No suspicious tokens to process.")
        return 0

    # Fetch creators via Helius getAsset
    new_wallets: List[Tuple[str, str]] = []
    with httpx.Client() as client_http:
        for mint, reason in suspicious_tokens:
            creators = _fetch_creators_for_mint(client_http, mint)
            if not creators:
                continue
            label = reason or "suspicious_token_creator"
            for creator in creators:
                new_wallets.append((creator, label))

    # Merge into existing without duplicates
    added = 0
    for wallet, label in new_wallets:
        wallet = (wallet or "").strip()
        if not wallet or wallet in existing:
            continue
        existing[wallet] = (label or "scam").strip() or "scam"
        added += 1

    # Save combined mapping back to disk
    _save_scam_wallets(scam_file, existing)

    print("New scam wallets added:", added)
    print("Total scam wallets:", len(existing))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
