"""
NFT Mint API — BlockID
POST /social/nft/mint-avatar
Mints a new NFT from user-uploaded image and sets it as avatar.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import uuid

import httpx
from botocore.config import Config
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from solders.pubkey import Pubkey
from solders.transaction import Transaction

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

TREASURY_WALLET = os.getenv("TREASURY_WALLET", "4DdLPRDiLRY8Q2E4Fv31kvcfMf3XJf11HgaSaW7tKVcx")
REQUIRED_SOL = float(os.getenv("NFT_MINT_PRICE_SOL", "0.01"))
LAMPORTS_PER_SOL = 1_000_000_000
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "")


async def ensure_tables() -> None:
    conn = await get_conn()
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nft_mint_payments (
                id SERIAL PRIMARY KEY,
                wallet VARCHAR(64) NOT NULL,
                tx_signature VARCHAR(128) NOT NULL UNIQUE,
                amount_sol NUMERIC(18, 9) NOT NULL,
                mint_address VARCHAR(64),
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_mint_payments_wallet ON nft_mint_payments(wallet)"
        )
        await conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_nft_mint_payments_wallet_date
            ON nft_mint_payments(wallet, created_at DESC)
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_nft_mint_payments_tx ON nft_mint_payments(tx_signature)"
        )
    finally:
        await release_conn(conn)


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


async def verify_payment_tx(tx_signature: str, payer_wallet: str) -> bool:
    """
    Verify that tx_signature is a valid SOL transfer:
    - from payer_wallet
    - to TREASURY_WALLET
    - amount >= REQUIRED_SOL
    - not already used
    """
    # Check replay — tx already used?
    conn = await get_conn()
    try:
        existing = await conn.fetchrow(
            "SELECT id FROM nft_mint_payments WHERE tx_signature = $1",
            tx_signature,
        )
        if existing:
            raise HTTPException(status_code=400, detail="Transaction already used")
    finally:
        await release_conn(conn)

    if not HELIUS_API_KEY.strip():
        raise HTTPException(status_code=500, detail="HELIUS_API_KEY not configured")

    # Fetch tx from Helius
    rpc_url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
    async with httpx.AsyncClient(timeout=15.0) as client:
        res = await client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getTransaction",
                "params": [
                    tx_signature,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "confirmed",
                        "maxSupportedTransactionVersion": 0,
                    },
                ],
            },
        )
        data = res.json()

    result = data.get("result")
    if not result:
        raise HTTPException(status_code=400, detail="Transaction not found or not confirmed")

    # Check tx not failed
    if result.get("meta", {}).get("err") is not None:
        raise HTTPException(status_code=400, detail="Transaction failed on-chain")

    # Parse instructions — find SOL transfer to treasury
    instructions = (
        result.get("transaction", {})
        .get("message", {})
        .get("instructions", [])
    )

    treasury = TREASURY_WALLET.lower()
    payer = payer_wallet.lower()
    found_transfer = False

    for ix in instructions:
        parsed = ix.get("parsed", {})
        if not isinstance(parsed, dict):
            continue
        ix_type = parsed.get("type", "")
        info = parsed.get("info", {})

        if ix_type == "transfer":
            source = (info.get("source") or "").lower()
            dest = (info.get("destination") or "").lower()
            lamports = int(info.get("lamports") or 0)
            sol_amount = lamports / LAMPORTS_PER_SOL

            if source == payer and dest == treasury and sol_amount >= REQUIRED_SOL:
                found_transfer = True
                break

    if not found_transfer:
        raise HTTPException(
            status_code=400,
            detail=f"No valid SOL transfer found. Required: {REQUIRED_SOL} SOL to {TREASURY_WALLET}",
        )

    return True


@router.post("/mint-avatar")
async def mint_nft_avatar(
    wallet: str = Form(...),
    tx_signature: str = Form(...),
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

    tx_signature = (tx_signature or "").strip()
    if not tx_signature:
        raise HTTPException(status_code=400, detail="tx_signature required")

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
        # Verify payment first
        await verify_payment_tx(tx_signature, wallet)
        logger.info("nft_payment_verified", wallet=wallet[:16], tx=tx_signature[:16])

        # Check plan-based mint limit
        conn = await get_conn()
        try:
            async with conn.transaction():
                # Get user plan
                sub = await conn.fetchrow(
                    """
                    SELECT plan FROM subscriptions
                    WHERE user_id = $1
                    AND status = 'active'
                    AND (valid_until IS NULL OR valid_until > NOW())
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    wallet,
                )

                plan = (sub["plan"] if sub else "free") or "free"
                plan = str(plan).lower()

                # Free plan cannot mint
                if plan == "free":
                    raise HTTPException(
                        403,
                        "Make Your Own NFT requires Explorer or PRO plan. "
                        "Upgrade at app.blockidscore.fun/upgrade",
                    )

                # Explorer: max 3 mints per month
                if plan == "explorer":
                    mint_count = await conn.fetchval(
                        """
                        SELECT COUNT(*) FROM nft_mint_payments
                        WHERE wallet = $1
                        AND created_at >= DATE_TRUNC('month', NOW())
                        """,
                        wallet,
                    )

                    if int(mint_count or 0) >= 3:
                        raise HTTPException(
                            429,
                            "Explorer plan allows 3 NFT mints per month. "
                            "Upgrade to PRO for unlimited mints.",
                        )

                # PRO plan: unlimited — no check needed
        finally:
            await release_conn(conn)

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
                f"{MINT_SERVICE_URL}/mint-avatar",
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
            await conn.execute(
                """
                INSERT INTO nft_mint_payments (wallet, tx_signature, amount_sol, mint_address)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (tx_signature) DO NOTHING
                """,
                wallet,
                tx_signature,
                REQUIRED_SOL,
                mint_address,
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
