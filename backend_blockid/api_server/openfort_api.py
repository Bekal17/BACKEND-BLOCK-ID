from __future__ import annotations

import os
import httpx

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/openfort", tags=["openfort"])


@router.post("/encryption-session")
async def create_encryption_session(req: Request):
    """
    Create Openfort Shield encryption session for embedded wallet.
    Returns { session: "..." } format expected by Openfort SDK.
    Calls Shield API: POST /project/encryption-session
    """
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization token")

    token = auth_header.replace("Bearer ", "", 1).strip()

    shield_publishable_key = os.getenv("OPENFORT_SHIELD_PUBLISHABLE_KEY", "")
    shield_secret_key = os.getenv("OPENFORT_SHIELD_SECRET", "")
    encryption_share = os.getenv("OPENFORT_SHIELD_ENCRYPTION_SHARE", "")

    if not shield_publishable_key or not shield_secret_key or not encryption_share:
        missing = []
        if not shield_publishable_key: missing.append("OPENFORT_SHIELD_PUBLISHABLE_KEY")
        if not shield_secret_key: missing.append("OPENFORT_SHIELD_SECRET")
        if not encryption_share: missing.append("OPENFORT_SHIELD_ENCRYPTION_SHARE")
        raise HTTPException(
            status_code=500,
            detail=f"Shield not configured. Missing: {', '.join(missing)}"
        )

    # Call Shield API to create encryption session
    # Reference: https://github.com/openfort-xyz/shield
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            "https://shield.openfort.io/project/encryption-session",
            headers={
                "Content-Type": "application/json",
                "x-api-key": shield_publishable_key,
                "x-api-secret": shield_secret_key,
                "x-encryption-part": encryption_share,
                "Authorization": f"Bearer {token}",
                "x-auth-provider": "openfort",
            },
            json={},
        )

    if resp.status_code != 200:
        raise HTTPException(
            status_code=resp.status_code,
            detail=f"Shield error ({resp.status_code}): {resp.text}",
        )

    data = resp.json()
    # Shield returns { session_id: "..." } or { session: "..." }
    session = data.get("session_id") or data.get("session") or data.get("id")
    if not session:
        raise HTTPException(
            status_code=500,
            detail=f"No session in Shield response: {data}",
        )

    return {"session": session}

