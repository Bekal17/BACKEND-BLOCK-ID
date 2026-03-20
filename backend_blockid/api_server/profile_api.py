from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel

from backend_blockid.api_server.session_auth import verify_session_token
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.integrations.helius_das import (
    get_wallet_nfts,
    verify_nft_ownership,
)
from backend_blockid.integrations.r2_client import upload_profile_photo, validate_photo
from backend_blockid.api_server.vision_moderation import check_image_safe

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


class ProfileUpdateRequest(BaseModel):
    wallet: str
    session_token: str
    display_name: Optional[str] = None
    display_name_source: Optional[str] = None
    bio: Optional[str] = None
    website: Optional[str] = None
    location: Optional[str] = None


@router.get("/profile/names/{wallet}")
async def get_wallet_names(wallet: str) -> dict:
    """
    Detect all on-chain names/handles owned by wallet.
    Returns list of names from all supported name services.

    Sources:
    1. BlockID handle (from handle_registry table)
    2. SNS .sol domains (Bonfida API)
    3. ANS .abc domains (AllDomains API)
    4. ENS .eth domains (skip for now — ETH chain)

    Response:
    {
        "wallet": "...",
        "names": [
            {"name": "@bee121", "source": "BLOCKID", "display": "@bee121", "icon": "blockid"},
            {"name": "bee121.sol", "source": "SNS", "display": "bee121.sol", "icon": "sol"},
            {"name": "bee.abc", "source": "ANS", "display": "bee.abc", "icon": "abc"}
        ],
        "fallback": "CJGGn82f...7XiQ"
    }
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    names = []
    conn = await get_conn()

    try:
        # 1. BlockID handle
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry "
            "WHERE owner_wallet = $1 AND status = 'ACTIVE' LIMIT 1",
            wallet,
        )
        if handle_row and handle_row["handle"]:
            names.append({
                "name": f"@{handle_row['handle']}",
                "source": "BLOCKID",
                "display": f"@{handle_row['handle']}",
                "icon": "blockid",
            })

        # 2. SNS .sol domains (Bonfida)
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(
                    f"https://sns-sdk-proxy.bonfida.workers.dev/domains/{wallet}",
                )
                if res.status_code == 200:
                    data = res.json()
                    domains = data.get("result", [])
                    for domain in domains[:3]:
                        name = domain if isinstance(domain, str) else domain.get("domain", "")
                        if name:
                            if not name.endswith(".sol"):
                                name = f"{name}.sol"
                            names.append({
                                "name": name,
                                "source": "SNS",
                                "display": name,
                                "icon": "sol",
                            })
        except Exception:
            pass

        # 3. ANS .abc domains (AllDomains)
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                res = await client.get(
                    f"https://api.alldomains.id/v1/owner/{wallet}",
                )
                if res.status_code == 200:
                    data = res.json()
                    domains = data.get("domains", data if isinstance(data, list) else [])
                    for domain in domains[:3]:
                        name = domain if isinstance(domain, str) else domain.get("name", "")
                        if name:
                            if not name.endswith(".abc"):
                                name = f"{name}.abc"
                            names.append({
                                "name": name,
                                "source": "ANS",
                                "display": name,
                                "icon": "abc",
                            })
        except Exception:
            pass

        fallback = f"{wallet[:4]}...{wallet[-4:]}"
        return {
            "wallet": wallet,
            "names": names,
            "fallback": fallback,
            "total": len(names),
        }
    finally:
        await release_conn(conn)


@router.put("/profile/update")
async def update_profile(body: ProfileUpdateRequest) -> dict:
    """
    Update user profile fields.
    Requires valid session token.

    Validates:
    - session_token must be valid JWT for body.wallet
    - display_name max 50 chars
    - bio max 160 chars
    - website max 255 chars, must start with http
    - location max 100 chars
    - display_name_source must be one of: WALLET | BLOCKID | SNS | ENS | ANS

    Returns updated profile fields.
    """
    wallet = (body.wallet or "").strip()

    # Verify session
    if BLOCKID_ENV != "DEV":
        verified = verify_session_token(body.session_token)
        if verified != wallet:
            raise HTTPException(status_code=401, detail="Invalid session")

    # Validate fields
    if body.display_name is not None and len(body.display_name) > 50:
        raise HTTPException(status_code=400, detail="display_name max 50 chars")
    if body.bio is not None and len(body.bio) > 160:
        raise HTTPException(status_code=400, detail="bio max 160 chars")
    if body.website is not None:
        if not body.website.startswith("http"):
            raise HTTPException(status_code=400, detail="website must start with http")
        if len(body.website) > 255:
            raise HTTPException(status_code=400, detail="website max 255 chars")
    if body.location is not None and len(body.location) > 100:
        raise HTTPException(status_code=400, detail="location max 100 chars")
    valid_sources = {"WALLET", "BLOCKID", "SNS", "ENS", "ANS"}
    if body.display_name_source is not None and body.display_name_source not in valid_sources:
        raise HTTPException(
            status_code=400,
            detail=f"display_name_source must be one of {valid_sources}",
        )

    conn = await get_conn()
    try:
        set_clauses = []
        values = []
        idx = 1
        if body.display_name is not None:
            set_clauses.append(f"display_name = ${idx}")
            values.append(body.display_name)
            idx += 1
        if body.display_name_source is not None:
            set_clauses.append(f"display_name_source = ${idx}")
            values.append(body.display_name_source)
            idx += 1
        if body.bio is not None:
            set_clauses.append(f"bio = ${idx}")
            values.append(body.bio)
            idx += 1
        if body.website is not None:
            set_clauses.append(f"website = ${idx}")
            values.append(body.website)
            idx += 1
        if body.location is not None:
            set_clauses.append(f"location = ${idx}")
            values.append(body.location)
            idx += 1

        if not set_clauses:
            raise HTTPException(status_code=400, detail="No fields to update")

        set_clauses.append("updated_at = NOW()")
        values.append(wallet)

        await conn.execute(
            "INSERT INTO social_profiles (wallet, updated_at) VALUES ($1, NOW()) "
            "ON CONFLICT (wallet) DO NOTHING",
            wallet,
        )
        await conn.execute(
            f"UPDATE social_profiles SET {', '.join(set_clauses)} WHERE wallet = ${idx}",
            *values,
        )

        updated_fields = [c.split(" = ")[0] for c in set_clauses[:-1]]
        return {
            "success": True,
            "wallet": wallet,
            "updated_fields": updated_fields,
        }
    finally:
        await release_conn(conn)


@router.get("/profile/{wallet}")
async def get_profile(wallet: str) -> dict:
    """
    Get full profile for a wallet, including display_name, bio, website, location.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT
                wallet, handle,
                display_name, display_name_source, bio, website, location,
                avatar_type, avatar_url, avatar_nft_mint, avatar_nft_name,
                avatar_nft_collection, avatar_is_animated,
                banner_type, banner_url, banner_is_animated,
                updated_at
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
            "handle": None,
            "display_name": None,
            "display_name_source": None,
            "bio": None,
            "website": None,
            "location": None,
            "avatar_type": "NONE",
            "avatar_url": None,
            "banner_type": "NONE",
            "banner_url": None,
        }

    return {
        "wallet": row["wallet"],
        "handle": row["handle"],
        "display_name": row.get("display_name"),
        "display_name_source": row.get("display_name_source"),
        "bio": row.get("bio"),
        "website": row.get("website"),
        "location": row.get("location"),
        "avatar_type": row.get("avatar_type") or "NONE",
        "avatar_url": row.get("avatar_url"),
        "avatar_nft_mint": row.get("avatar_nft_mint"),
        "avatar_nft_name": row.get("avatar_nft_name"),
        "avatar_nft_collection": row.get("avatar_nft_collection"),
        "avatar_is_animated": bool(row.get("avatar_is_animated")),
        "banner_type": row.get("banner_type") or "NONE",
        "banner_url": row.get("banner_url"),
        "banner_is_animated": bool(row.get("banner_is_animated")),
        "updated_at": row.get("updated_at"),
    }


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

    vision_result = await check_image_safe(file_bytes)
    if not vision_result["safe"]:
        raise HTTPException(
            status_code=400,
            detail=f"Image rejected: {vision_result['reason']}",
        )

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

    vision_result = await check_image_safe(file_bytes)
    if not vision_result["safe"]:
        raise HTTPException(
            status_code=400,
            detail=f"Image rejected: {vision_result['reason']}",
        )

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

