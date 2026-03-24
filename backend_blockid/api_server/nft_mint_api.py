"""
NFT Mint API — BlockID
POST /social/nft/mint-avatar
Mints a new NFT from user-uploaded image and sets it as avatar.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import httpx
from botocore.config import Config
from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)
router = APIRouter(prefix="/social/nft", tags=["nft"])

R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.getenv("R2_BUCKET_NAME", os.getenv("R2_BUCKET", "blockid-social-media"))
R2_ENDPOINT = (os.getenv("R2_ENDPOINT") or os.getenv("R2_ENDPOINT_URL") or "").strip()
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "https://media.blockidscore.fun")
MINT_SERVICE_URL = (os.getenv("MINT_SERVICE_URL", "http://localhost:3001")).rstrip("/")


async def upload_to_r2(data: bytes, key: str, content_type: str) -> str:
    """Upload bytes to R2 and return public URL."""
    import boto3

    if not R2_ENDPOINT or not R2_ACCESS_KEY_ID or not R2_SECRET_ACCESS_KEY:
        raise ValueError("R2 not configured (R2_ENDPOINT, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY)")

    def _put() -> None:
        s3 = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY_ID,
            aws_secret_access_key=R2_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=key,
            Body=data,
            ContentType=content_type,
        )

    await asyncio.to_thread(_put)
    return f"{R2_PUBLIC_URL.rstrip('/')}/{key}"


@router.post("/mint-avatar")
async def mint_nft_avatar(
    wallet: str = Form(...),
    file: UploadFile = File(...),
    name: str = Form(default="BlockID NFT"),
    description: str = Form(default="Minted via BlockID"),
):
    """
    Mint a new NFT from uploaded image and set as avatar.

    Flow:
    1. Validate file (image only, max 5MB)
    2. Upload image to R2
    3. Generate and upload metadata JSON to R2
    4. Call mint service to mint NFT on-chain
    5. Set NFT as avatar in social_profiles
    6. Return mint result
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    # Validate file
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    contents = await file.read()
    if len(contents) > 5 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File too large (max 5MB)")

    file_id = str(uuid.uuid4())
    ext = file.filename.split(".")[-1] if file.filename and "." in file.filename else "png"
    image_key = f"nft-avatar/{wallet}/{file_id}.{ext}"
    metadata_key = f"nft-avatar/{wallet}/{file_id}-metadata.json"

    try:
        # Upload image
        image_url = await upload_to_r2(contents, image_key, file.content_type)
        logger.info("nft_image_uploaded", wallet=wallet[:16], image_url=image_url)

        # Generate metadata
        metadata = {
            "name": name,
            "symbol": "BLOCKID",
            "description": description,
            "image": image_url,
            "attributes": [
                {"trait_type": "Platform", "value": "BlockID"},
                {"trait_type": "Type", "value": "Avatar"},
            ],
            "properties": {
                "files": [{"uri": image_url, "type": file.content_type}],
                "category": "image",
            },
        }
        metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
        metadata_url = await upload_to_r2(metadata_bytes, metadata_key, "application/json")
        logger.info("nft_metadata_uploaded", wallet=wallet[:16])

        # Call mint service
        async with httpx.AsyncClient(timeout=60.0) as client:
            mint_res = await client.post(
                f"{MINT_SERVICE_URL}/mint",
                json={
                    "wallet": wallet,
                    "image_url": image_url,
                    "metadata_url": metadata_url,
                    "name": name,
                    "description": description,
                },
            )
            try:
                mint_data = mint_res.json()
            except Exception:
                raise HTTPException(
                    status_code=502,
                    detail=f"Mint service invalid response: {mint_res.text[:200]}",
                )

        if not mint_data.get("success"):
            raise HTTPException(
                status_code=500,
                detail=f"Mint failed: {mint_data.get('error', mint_data)}",
            )

        mint_address = mint_data["mint_address"]
        logger.info("nft_minted", wallet=wallet[:16], mint_address=mint_address)

        # Set as avatar in DB
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
                    'NFT', $2, $3, $4, 'BlockID', false, NOW()
                )
                ON CONFLICT (wallet) DO UPDATE SET
                    avatar_type = 'NFT',
                    avatar_url = EXCLUDED.avatar_url,
                    avatar_nft_mint = EXCLUDED.avatar_nft_mint,
                    avatar_nft_name = EXCLUDED.avatar_nft_name,
                    avatar_nft_collection = 'BlockID',
                    avatar_is_animated = false,
                    updated_at = NOW()
                """,
                wallet,
                image_url,
                mint_address,
                name,
            )
        finally:
            await release_conn(conn)

        return {
            "success": True,
            "mint_address": mint_address,
            "image_url": image_url,
            "metadata_url": metadata_url,
            "avatar_url": image_url,
            "avatar_type": "NFT",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error("nft_mint_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=500, detail=f"Mint failed: {str(e)}") from e
