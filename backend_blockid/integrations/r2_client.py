"""
Cloudflare R2 client for BlockID Social Layer.
Handles image upload, delete, and URL generation.
Uses boto3 with S3-compatible API.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Tuple

R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID", "")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME", "blockid-social-media")
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

MAX_FILE_SIZE = 5 * 1024 * 1024  # 5MB
ALLOWED_TYPES = {"image/jpeg", "image/png", "image/gif", "image/webp"}

MAX_PHOTO_SIZE = 5 * 1024 * 1024  # 5MB
ALLOWED_PHOTO_TYPES = {
    "image/jpeg",
    "image/jpg",
    "image/png",
    "image/gif",
    "image/webp",
}


def get_r2_client():
    """Get boto3 S3 client configured for Cloudflare R2."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name="auto",
    )


def get_public_url(key: str) -> str:
    """Get public URL for R2 object."""
    key = key.lstrip("/")
    return f"{R2_PUBLIC_URL.rstrip('/')}/{key}"


def validate_image(file_bytes: bytes, content_type: str) -> Tuple[bool, str]:
    """
    Validate image before upload.
    Returns (is_valid, error_message).

    Rules:
    - Max 5MB
    - Only jpeg, png, gif, webp
    - Min 10 bytes (not empty)
    """
    if not file_bytes or len(file_bytes) < 10:
        return False, "Image file is empty or too small."
    if len(file_bytes) > MAX_FILE_SIZE:
        return False, "Image file exceeds maximum size of 5MB."
    if content_type not in ALLOWED_TYPES:
        return False, f"Unsupported image content type: {content_type}"
    return True, ""


async def upload_image(
    file_bytes: bytes,
    content_type: str,
    wallet: str,
) -> dict:
    """
    Upload image to Cloudflare R2.
    Returns {\"key\": str, \"url\": str}

    Key format: social/{wallet[:8]}/{uuid}.{ext}
    """
    is_valid, error = validate_image(file_bytes, content_type)
    if not is_valid:
        raise ValueError(error)

    wallet_prefix = (wallet or "").strip()[:8] or "unknown"
    ext = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
    }.get(content_type, "bin")

    key = f"social/{wallet_prefix}/{uuid.uuid4().hex}.{ext}"

    client = get_r2_client()
    # Upload synchronously; FastAPI layer can run this in a thread if needed.
    client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=file_bytes,
        ContentType=content_type,
        ACL="public-read",
    )
    return {"key": key, "url": get_public_url(key)}


async def delete_image(key: str) -> bool:
    """Delete image from R2 by key."""
    if not key:
        return False
    client = get_r2_client()
    try:
        client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)
        return True
    except Exception:
        # Best-effort delete; caller can log details.
        return False


def validate_photo(file_bytes: bytes, content_type: str) -> Tuple[bool, str]:
    """
    Validate profile photo before upload.
    Returns (is_valid, error_message).
    """
    if not file_bytes or len(file_bytes) < 10:
        return False, "File is empty"
    if len(file_bytes) > MAX_PHOTO_SIZE:
        return False, "File too large (max 5MB)"
    if content_type not in ALLOWED_PHOTO_TYPES:
        return False, f"File type not allowed: {content_type}"
    return True, ""


async def upload_profile_photo(
    file_bytes: bytes,
    content_type: str,
    wallet: str,
    photo_type: str = "avatar",  # "avatar" or "banner"
) -> dict:
    """
    Upload profile photo to R2.
    Key: profile/{photo_type}/{wallet[:8]}/{uuid}.{ext}

    Returns { "key": str, "url": str }.
    """
    is_valid, error = validate_photo(file_bytes, content_type)
    if not is_valid:
        raise ValueError(error)

    ext_map = {
        "image/jpeg": "jpg",
        "image/jpg": "jpg",
        "image/png": "png",
        "image/gif": "gif",
        "image/webp": "webp",
    }
    ext = ext_map.get(content_type, "jpg")
    wallet_prefix = (wallet or "").strip()[:8] or "unknown"
    key = f"profile/{photo_type}/{wallet_prefix}/{uuid.uuid4()}.{ext}"

    client = get_r2_client()
    client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=file_bytes,
        ContentType=content_type,
        ACL="public-read",
    )

    url = get_public_url(key)
    return {"key": key, "url": url}


