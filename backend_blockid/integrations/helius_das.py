"""
Helius DAS API client for BlockID.
Fetches NFTs owned by wallet and verifies NFT ownership.
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")
BLOCKID_ENV = os.getenv("BLOCKID_ENV", "DEV")


def get_rpc_url() -> str:
    if BLOCKID_ENV == "DEV":
        return f"https://devnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    return f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"


async def get_wallet_nfts(
    wallet: str,
    page: int = 1,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    Fetch all NFTs owned by wallet using Helius DAS getAssetsByOwner.

    Returns list of:
    {
        "mint": str,
        "name": str,
        "symbol": str,
        "image_url": str,
        "animation_url": str | None,
        "is_animated": bool,
        "collection": str | None,
        "description": str,
    }

    Returns [] on any error (graceful degradation).
    Filters out fungible tokens and NFTs without images.
    """
    if not HELIUS_API_KEY:
        logger.warning("helius_das_no_api_key")
        return []

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                get_rpc_url(),
                json={
                    "jsonrpc": "2.0",
                    "id": "blockid-nft-fetch",
                    "method": "getAssetsByOwner",
                    "params": {
                        "ownerAddress": wallet,
                        "page": page,
                        "limit": limit,
                        "displayOptions": {
                            "showFungible": False,
                            "showNativeBalance": False,
                            "showCollectionMetadata": True,
                        },
                    },
                },
            )
            response.raise_for_status()
            data = response.json()

        items = (data.get("result") or {}).get("items", []) or []
        nfts: list[dict[str, Any]] = []

        for item in items:
            interface = item.get("interface") or ""
            if interface in ("FungibleToken", "FungibleAsset"):
                continue

            content = item.get("content") or {}
            links = content.get("links") or {}
            files = content.get("files") or []
            metadata = content.get("metadata") or {}

            image_url = links.get("image") or links.get("external_url")
            animation_url = links.get("animation_url")

            if not image_url and files:
                for f in files:
                    if not isinstance(f, dict):
                        continue
                    mime = (f.get("mime") or "").lower()
                    # Accept PNG, JPEG, GIF, WebP, SVG
                    if mime in ("image/jpeg", "image/png", "image/gif",
                                "image/webp", "image/svg+xml"):
                        # Prefer CDN URI for faster loading
                        image_url = f.get("cdn_uri") or f.get("uri") or ""
                        break

            # Also use CDN URI if available from links
            if image_url and files:
                for f in files:
                    if not isinstance(f, dict):
                        continue
                    if f.get("uri") == image_url and f.get("cdn_uri"):
                        image_url = f.get("cdn_uri")
                        break

            if not image_url:
                continue

            is_animated = bool(animation_url) or image_url.lower().endswith(".gif")

            grouping = item.get("grouping") or []
            collection: str | None = None
            for g in grouping:
                if g.get("group_key") == "collection":
                    col_meta = g.get("collection_metadata") or {}
                    collection = col_meta.get("name")
                    break

            nfts.append(
                {
                    "mint": item.get("id") or "",
                    "name": metadata.get("name") or "Unknown NFT",
                    "symbol": metadata.get("symbol") or "",
                    "image_url": image_url,
                    "animation_url": animation_url,
                    "is_animated": is_animated,
                    "collection": collection,
                    "description": metadata.get("description") or "",
                }
            )

        logger.info("helius_das_nfts_fetched", wallet=wallet[:16], count=len(nfts))
        return nfts

    except httpx.TimeoutException:
        logger.warning("helius_das_timeout", wallet=wallet[:16])
        return []
    except Exception as e:
        logger.warning("helius_das_error", wallet=wallet[:16], error=str(e))
        return []


async def verify_nft_ownership(
    wallet: str,
    nft_mint: str,
) -> bool:
    """
    Verify wallet owns the NFT using Helius DAS getAsset.
    Returns False on any error (safe default).
    """
    if not HELIUS_API_KEY:
        return False

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                get_rpc_url(),
                json={
                    "jsonrpc": "2.0",
                    "id": "blockid-verify-ownership",
                    "method": "getAsset",
                    "params": {
                        "id": nft_mint,
                        "displayOptions": {
                            "showOwnership": True,
                        },
                    },
                },
            )
            response.raise_for_status()
            data = response.json()

        result = data.get("result") or {}
        ownership = result.get("ownership") or {}
        owner = ownership.get("owner") or ""

        is_owner = owner.lower() == wallet.lower()
        logger.info(
            "helius_das_ownership_verified",
            wallet=wallet[:16],
            nft_mint=nft_mint[:16],
            is_owner=is_owner,
        )
        return is_owner

    except Exception as e:
        logger.warning(
            "helius_das_verify_error",
            wallet=wallet[:16],
            error=str(e),
        )
        return False

