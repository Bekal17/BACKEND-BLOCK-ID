from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/openfort", tags=["openfort"])


@router.post("/encryption-session")
async def create_encryption_session(req: Request):
    """
    Create Openfort Shield encryption session for embedded wallet recovery.
    Called by frontend during wallet creation/recovery.
    Requires valid Openfort access token in Authorization header.
    """
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization token")

    token = auth_header.replace("Bearer ", "", 1).strip()

    shield_secret = os.getenv("OPENFORT_SHIELD_SECRET", "")
    openfort_secret = os.getenv("OPENFORT_SECRET_KEY", "")

    if not shield_secret or not openfort_secret:
        raise HTTPException(status_code=500, detail="Shield not configured")

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://shield.openfort.io/projects/encryption-session",
            headers={
                "Content-Type": "application/json",
                "x-openfort-publishable-key": os.getenv(
                    "OPENFORT_PUBLISHABLE_KEY", ""
                ),
                "Authorization": f"Bearer {token}",
                "x-shield-secret-key": shield_secret,
            },
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Shield session error: {resp.text}",
        )

    return resp.json()

