"""
Extract important fields from Helius getAsset RPC response for BlockID ML and scam detection.

Usage:
    python -m backend_blockid.oracle.helius_extract_fields

Requires .env with HELIUS_API_KEY or SOLANA_RPC_URL (Helius RPC).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

# Load .env from project root
ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / ".env")

# Example mints: USDC, BONK, NFT
EXAMPLE_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
EXAMPLE_BONK_MINT = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263"
EXAMPLE_NFT_MINT = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"  # Mad Lads example
EXAMPLE_MINTS = (EXAMPLE_USDC_MINT, EXAMPLE_BONK_MINT, EXAMPLE_NFT_MINT)


def _helius_rpc_url() -> str | None:
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    key = (os.getenv("HELIUS_API_KEY") or "").strip()
    if key:
        return f"https://mainnet.helius-rpc.com/?api-key={key}"
    return None


def analyze_token(asset: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract important fields from a Helius getAsset RPC result for ML and scam detection.

    Handles missing keys safely. Returns a flat dict with:
      - name, symbol, supply, decimals
      - mint_authority, freeze_authority
      - creator_authorities (list of addresses)
      - scam flags: mint_authority_exists, freeze_authority_exists, metadata_missing
    """
    token: Dict[str, Any] = {
        "name": None,
        "symbol": None,
        "supply": None,
        "decimals": None,
        "mint_authority": None,
        "freeze_authority": None,
        "creator_authorities": [],
        "mint_authority_exists": False,
        "freeze_authority_exists": False,
        "metadata_missing": True,
    }

    if not asset or not isinstance(asset, dict):
        return token

    # On-chain metadata: content.metadata or content.onChainMetadata.metadata
    content = asset.get("content") or {}
    if not isinstance(content, dict):
        content = {}
    metadata = content.get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}
    if not metadata and content.get("onChainMetadata"):
        metadata = (content.get("onChainMetadata") or {}).get("metadata") or {}
    if not isinstance(metadata, dict):
        metadata = {}

    token["metadata_missing"] = not bool(metadata)

    # Name, symbol from metadata
    token["name"] = (metadata.get("name") or "").strip() or None
    token["symbol"] = (metadata.get("symbol") or "").strip() or None

    # token_info: some Helius assets store supply/decimals/authorities here
    token_info = asset.get("token_info") or content.get("token_info") or {}
    if not isinstance(token_info, dict):
        token_info = {}
    token["token_info_raw"] = token_info  # for debugging

    # Supply, decimals: prefer token_info, then top-level asset
    def _int_or_none(val: Any) -> int | None:
        if val is None:
            return None
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    token["supply"] = _int_or_none(token_info.get("supply")) or _int_or_none(asset.get("supply"))
    token["decimals"] = _int_or_none(token_info.get("decimals")) or _int_or_none(asset.get("decimals"))

    # Authorities: Helius DAS uses "authorities" array or top-level or token_info
    authorities = asset.get("authorities") or []
    if not isinstance(authorities, list):
        authorities = []
    creator_authorities: List[str] = []
    mint_authority = None
    freeze_authority = None
    for auth in authorities:
        if not isinstance(auth, dict):
            continue
        at = (auth.get("type") or auth.get("authority_type") or "").strip().lower()
        addr = (auth.get("address") or auth.get("authority") or "").strip()
        if not addr:
            continue
        if at in ("mint", "mint_authority"):
            mint_authority = addr
        elif at in ("freeze", "freeze_authority"):
            freeze_authority = addr
        else:
            creator_authorities.append(addr)
    # Fallback: token_info then top-level keys
    if mint_authority is None:
        mint_authority = (
            (token_info.get("mint_authority") or token_info.get("mintAuthority") or "").strip()
            or (asset.get("mint_authority") or asset.get("mintAuthority") or "").strip()
        ) or None
    if freeze_authority is None:
        freeze_authority = (
            (token_info.get("freeze_authority") or token_info.get("freezeAuthority") or "").strip()
            or (asset.get("freeze_authority") or asset.get("freezeAuthority") or "").strip()
        ) or None
    # Creators from metadata
    creators = (metadata.get("creators") or metadata.get("data", {}).get("creators") or [])
    if isinstance(creators, list):
        for c in creators:
            if isinstance(c, dict):
                addr = (c.get("address") or c.get("creator") or "").strip()
                if addr and addr not in creator_authorities:
                    creator_authorities.append(addr)

    token["mint_authority"] = mint_authority
    token["freeze_authority"] = freeze_authority
    token["creator_authorities"] = creator_authorities
    token["mint_authority_exists"] = bool(mint_authority)
    token["freeze_authority_exists"] = bool(freeze_authority)

    return token


def fetch_asset(mint: str) -> Dict[str, Any] | None:
    """Call Helius getAsset RPC and return result (asset dict) or None on failure."""
    rpc_url = _helius_rpc_url()
    if not rpc_url:
        print("[helius_extract] ERROR: HELIUS_API_KEY or SOLANA_RPC_URL not set")
        return None

    payload = {
        "jsonrpc": "2.0",
        "id": "blockid-extract",
        "method": "getAsset",
        "params": {"id": mint},
    }
    try:
        resp = requests.post(rpc_url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print("[helius_extract] ERROR: RPC request failed:", e)
        return None

    err = data.get("error")
    if err:
        print("[helius_extract] ERROR: RPC error:", err)
        return None
    result = data.get("result")
    if result is None:
        print("[helius_extract] WARN: result is null")
        return None
    return result if isinstance(result, dict) else None


def main() -> int:
    print("[helius_extract] Load dotenv from project root:", ROOT / ".env")
    rpc_url = _helius_rpc_url()
    print("[helius_extract] RPC URL:", rpc_url or "(not set)")
    if not rpc_url:
        print("[helius_extract] ERROR: Stop — RPC URL missing")
        return 1

    for mint in EXAMPLE_MINTS:
        print("\n[helius_extract] --- mint:", mint)
        asset = fetch_asset(mint)
        if asset is None:
            continue

        token = analyze_token(asset)
        token_info_raw = token.get("token_info_raw") or {}

        print("[helius_extract] raw token_info:", json.dumps(token_info_raw, indent=2, default=str))
        if not token_info_raw:
            print("[helius_extract] WARN: token_info empty")

        # Structured output (exclude token_info_raw from compact view; it was logged above)
        out = {k: v for k, v in token.items() if k != "token_info_raw"}
        print("[helius_extract] Structured output:")
        print(json.dumps(out, indent=2, default=str))
        print("[helius_extract] Scam flags: mint_authority_exists={}, freeze_authority_exists={}, metadata_missing={}".format(
            token["mint_authority_exists"],
            token["freeze_authority_exists"],
            token["metadata_missing"],
        ))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
