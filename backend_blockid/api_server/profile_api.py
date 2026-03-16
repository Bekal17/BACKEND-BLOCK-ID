from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.integrations.helius_das import (
    get_wallet_nfts,
    verify_nft_ownership,
)
from backend_blockid.integrations.r2_client import upload_profile_photo, validate_photo

logger = get_logger(__name__)

router = APIRouter(prefix="/social", tags=["Profile"])

DEVNET_BYPASS = {"devtest_signature_bypass"}
BLOCKID_ENV = os.getenv("BLOCKID_ENV", "DEV")


def _is_bypass(signature: str) -> bool:
    return BLOCKID_ENV.upper() == "DEV" and signature in DEVNET_BYPASS


def get_border_style(avatar_type: str) -> str:
    if avatar_type == "NFT":
        return "square_gold"
    if avatar_type == "PHOTO":
        return "round"
    return "none"


class SetAvatarRequest(BaseModel):
    wallet: str
    type: str  # "NFT" or "PHOTO" (PHOTO not yet implemented)
    nft_mint: Optional[str] = None
    signed_message: str = ""
    signature: str = ""


class RemoveAvatarRequest(BaseModel):
    wallet: str
    signature: str = ""


@router.get("/nfts/{wallet}")
async def get_wallet_nft_gallery(
    wallet: str,
    page: int = 1,
    limit: int = 50,
):
    """
    Fetch all NFTs owned by wallet using Helius DAS API.
    Used to populate NFT selection gallery in frontend.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    nfts = await get_wallet_nfts(wallet, page=page, limit=limit)
    return {
        "wallet": wallet,
        "nfts": nfts,
        "total": len(nfts),
        "page": page,
    }


@router.get("/avatar/{wallet}")
async def get_avatar(wallet: str):
    """
    Get current avatar + banner info for wallet.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT
                wallet,
                avatar_type,
                avatar_url,
                avatar_nft_mint,
                avatar_nft_name,
                avatar_nft_collection,
                avatar_is_animated,
                banner_type,
                banner_url,
                banner_is_animated
            FROM social_profiles
            WHERE wallet = $1
            """,
            wallet,
        )
    finally:
        await release_conn(conn)

    if not row:
        return {
            "wallet": wallet,
            "avatar_type": "NONE",
            "avatar_url": None,
            "avatar_nft_mint": None,
            "avatar_nft_name": None,
            "avatar_nft_collection": None,
            "avatar_is_animated": False,
            "border_style": "none",
            "banner_type": "NONE",
            "banner_url": None,
            "banner_is_animated": False,
        }

    avatar_type = row["avatar_type"] or "NONE"
    banner_type = row["banner_type"] or "NONE"
    return {
        "wallet": wallet,
        "avatar_type": avatar_type,
        "avatar_url": row["avatar_url"],
        "avatar_nft_mint": row["avatar_nft_mint"],
        "avatar_nft_name": row["avatar_nft_name"],
        "avatar_nft_collection": row["avatar_nft_collection"],
        "avatar_is_animated": bool(row["avatar_is_animated"]),
        "border_style": get_border_style(avatar_type),
        "banner_type": banner_type,
        "banner_url": row["banner_url"],
        "banner_is_animated": bool(row["banner_is_animated"]),
    }


@router.post("/avatar")
async def set_avatar(request: SetAvatarRequest):
    """
    Set avatar — NFT only (PHOTO reserved for future).
    """
    wallet = (request.wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    avatar_type = (request.type or "").upper()
    if avatar_type != "NFT":
        raise HTTPException(status_code=400, detail="Only NFT avatar supported currently")

    nft_mint = (request.nft_mint or "").strip()
    if not nft_mint:
        raise HTTPException(status_code=400, detail="nft_mint is required for NFT avatar")

    if not _is_bypass(request.signature):
        # TODO: integrate real signature verification
        if not request.signed_message:
            raise HTTPException(status_code=400, detail="signed_message required")

    owns = await verify_nft_ownership(wallet, nft_mint)
    if not owns:
        raise HTTPException(status_code=403, detail="You do not own this NFT")

    nfts = await get_wallet_nfts(wallet, limit=200)
    nft = next((n for n in nfts if n.get("mint") == nft_mint), None)
    if not nft:
        raise HTTPException(status_code=404, detail="NFT metadata not found")

    image_url = nft.get("image_url")
    if not image_url:
        raise HTTPException(status_code=400, detail="NFT has no image_url")

    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO social_profiles (
                wallet, handle,
                avatar_type, avatar_url, avatar_nft_mint,
                avatar_nft_name, avatar_nft_collection, avatar_is_animated,
                updated_at
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                'NFT', $2, $3,
                $4, $5, $6,
                NOW()
            )
            ON CONFLICT (wallet) DO UPDATE SET
                avatar_type = 'NFT',
                avatar_url = EXCLUDED.avatar_url,
                avatar_nft_mint = EXCLUDED.avatar_nft_mint,
                avatar_nft_name = EXCLUDED.avatar_nft_name,
                avatar_nft_collection = EXCLUDED.avatar_nft_collection,
                avatar_is_animated = EXCLUDED.avatar_is_animated,
                updated_at = NOW()
            """,
            wallet,
            image_url,
            nft_mint,
            nft.get("name") or "",
            nft.get("collection") or "",
            bool(nft.get("is_animated")),
        )
    finally:
        await release_conn(conn)

    return {
        "success": True,
        "wallet": wallet,
        "avatar_type": "NFT",
        "avatar_url": image_url,
        "avatar_nft_mint": nft_mint,
        "avatar_nft_name": nft.get("name") or "",
        "avatar_nft_collection": nft.get("collection") or "",
        "avatar_is_animated": bool(nft.get("is_animated")),
        "border_style": "square_gold",
    }


@router.post("/banner")
async def set_banner(request: SetAvatarRequest):
    """
    Set banner — NFT only (PHOTO reserved for future).
    """
    wallet = (request.wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    banner_type = (request.type or "").upper()
    if banner_type != "NFT":
        raise HTTPException(status_code=400, detail="Only NFT banner supported currently")

    nft_mint = (request.nft_mint or "").strip()
    if not nft_mint:
        raise HTTPException(status_code=400, detail="nft_mint is required for NFT banner")

    if not _is_bypass(request.signature):
        if not request.signed_message:
            raise HTTPException(status_code=400, detail="signed_message required")

    owns = await verify_nft_ownership(wallet, nft_mint)
    if not owns:
        raise HTTPException(status_code=403, detail="You do not own this NFT")

    nfts = await get_wallet_nfts(wallet, limit=200)
    nft = next((n for n in nfts if n.get("mint") == nft_mint), None)
    if not nft:
        raise HTTPException(status_code=404, detail="NFT metadata not found")

    image_url = nft.get("image_url")
    if not image_url:
        raise HTTPException(status_code=400, detail="NFT has no image_url")

    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO social_profiles (
                wallet, handle,
                banner_type, banner_url, banner_nft_mint,
                banner_nft_name, banner_is_animated,
                updated_at
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                'NFT', $2, $3,
                $4, $5,
                NOW()
            )
            ON CONFLICT (wallet) DO UPDATE SET
                banner_type = 'NFT',
                banner_url = EXCLUDED.banner_url,
                banner_nft_mint = EXCLUDED.banner_nft_mint,
                banner_nft_name = EXCLUDED.banner_nft_name,
                banner_is_animated = EXCLUDED.banner_is_animated,
                updated_at = NOW()
            """,
            wallet,
            image_url,
            nft_mint,
            nft.get("name") or "",
            bool(nft.get("is_animated")),
        )
    finally:
        await release_conn(conn)

    return {
        "success": True,
        "wallet": wallet,
        "banner_type": "NFT",
        "banner_url": image_url,
        "banner_nft_mint": nft_mint,
        "banner_nft_name": nft.get("name") or "",
        "banner_is_animated": bool(nft.get("is_animated")),
    }


@router.post("/avatar/photo")
async def set_avatar_photo(
    wallet: str = Form(...),
    signature: str = Form(default=""),
    file: UploadFile = File(...),
):
    """
    Upload photo as avatar to Cloudflare R2.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    if not _is_bypass(signature):
        # TODO: integrate real signature verification
        if not signature:
            raise HTTPException(status_code=400, detail="signature required")

    content_type = file.content_type or "image/jpeg"
    file_bytes = await file.read()
    is_valid, error = validate_photo(file_bytes, content_type)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)

    try:
        upload_res = await upload_profile_photo(file_bytes, content_type, wallet, "avatar")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    avatar_url = upload_res["url"]
    is_animated = content_type == "image/gif"

    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO social_profiles (
                wallet, handle,
                avatar_type, avatar_url, avatar_nft_mint,
                avatar_nft_name, avatar_nft_collection, avatar_is_animated,
                updated_at
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                'PHOTO', $2, NULL,
                NULL, NULL, $3,
                NOW()
            )
            ON CONFLICT (wallet) DO UPDATE SET
                avatar_type = 'PHOTO',
                avatar_url = EXCLUDED.avatar_url,
                avatar_nft_mint = NULL,
                avatar_nft_name = NULL,
                avatar_nft_collection = NULL,
                avatar_is_animated = EXCLUDED.avatar_is_animated,
                updated_at = NOW()
            """,
            wallet,
            avatar_url,
            is_animated,
        )
    finally:
        await release_conn(conn)

    return {
        "success": True,
        "wallet": wallet,
        "avatar_type": "PHOTO",
        "avatar_url": avatar_url,
        "avatar_is_animated": is_animated,
        "border_style": "round",
    }


@router.post("/banner/photo")
async def set_banner_photo(
    wallet: str = Form(...),
    signature: str = Form(default=""),
    file: UploadFile = File(...),
):
    """
    Upload photo as banner to Cloudflare R2.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    if not _is_bypass(signature):
        if not signature:
            raise HTTPException(status_code=400, detail="signature required")

    content_type = file.content_type or "image/jpeg"
    file_bytes = await file.read()
    is_valid, error = validate_photo(file_bytes, content_type)
    if not is_valid:
        raise HTTPException(status_code=400, detail=error)

    try:
        upload_res = await upload_profile_photo(file_bytes, content_type, wallet, "banner")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    banner_url = upload_res["url"]
    is_animated = content_type == "image/gif"

    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO social_profiles (
                wallet, handle,
                banner_type, banner_url, banner_nft_mint,
                banner_nft_name, banner_is_animated,
                updated_at
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                'PHOTO', $2, NULL,
                NULL, $3,
                NOW()
            )
            ON CONFLICT (wallet) DO UPDATE SET
                banner_type = 'PHOTO',
                banner_url = EXCLUDED.banner_url,
                banner_nft_mint = NULL,
                banner_nft_name = NULL,
                banner_is_animated = EXCLUDED.banner_is_animated,
                updated_at = NOW()
            """,
            wallet,
            banner_url,
            is_animated,
        )
    finally:
        await release_conn(conn)

    return {
        "success": True,
        "wallet": wallet,
        "banner_type": "PHOTO",
        "banner_url": banner_url,
        "banner_is_animated": is_animated,
    }


@router.delete("/avatar")
async def delete_avatar(request: RemoveAvatarRequest):
    """
    Remove avatar, reset to NONE.
    """
    wallet = (request.wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    if not _is_bypass(request.signature):
        if not request.signature:
            raise HTTPException(status_code=400, detail="signature required")

    conn = await get_conn()
    try:
        await conn.execute(
            """
            UPDATE social_profiles
            SET avatar_type = 'NONE',
                avatar_url = NULL,
                avatar_nft_mint = NULL,
                avatar_nft_name = NULL,
                avatar_nft_collection = NULL,
                avatar_is_animated = FALSE,
                updated_at = NOW()
            WHERE wallet = $1
            """,
            wallet,
        )
    finally:
        await release_conn(conn)

    return {"success": True, "wallet": wallet}


@router.delete("/banner")
async def delete_banner(request: RemoveAvatarRequest):
    """
    Remove banner, reset to NONE.
    """
    wallet = (request.wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    if not _is_bypass(request.signature):
        if not request.signature:
            raise HTTPException(status_code=400, detail="signature required")

    conn = await get_conn()
    try:
        await conn.execute(
            """
            UPDATE social_profiles
            SET banner_type = 'NONE',
                banner_url = NULL,
                banner_nft_mint = NULL,
                banner_nft_name = NULL,
                banner_is_animated = FALSE,
                updated_at = NOW()
            WHERE wallet = $1
            """,
            wallet,
        )
    finally:
        await release_conn(conn)

    return {"success": True, "wallet": wallet}

