from __future__ import annotations

import os

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(prefix="/openfort", tags=["openfort"])


@router.post("/encryption-session")
async def create_encryption_session(req: Request):
    """
    Create Openfort Shield encryption session for embedded wallet.
    Returns { session: "..." } format expected by Openfort SDK.
    Uses openfort-node SDK registerRecoverySession method.
    """
    # Verify user is authenticated (has Bearer token)
    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing authorization token")

    shield_publishable_key = os.getenv("OPENFORT_SHIELD_PUBLISHABLE_KEY", "")
    shield_secret_key = os.getenv("OPENFORT_SHIELD_SECRET", "")
    encryption_share = os.getenv("OPENFORT_SHIELD_ENCRYPTION_SHARE", "")

    if not shield_publishable_key or not shield_secret_key or not encryption_share:
        missing = []
        if not shield_publishable_key:
            missing.append("OPENFORT_SHIELD_PUBLISHABLE_KEY")
        if not shield_secret_key:
            missing.append("OPENFORT_SHIELD_SECRET")
        if not encryption_share:
            missing.append("OPENFORT_SHIELD_ENCRYPTION_SHARE")
        raise HTTPException(
            status_code=500,
            detail=f"Shield not configured. Missing: {', '.join(missing)}"
        )

    try:
        import openfort

        of_client = openfort.Openfort(api_key=os.getenv("OPENFORT_SECRET_KEY", ""))
        session = of_client.register_recovery_session(
            shield_publishable_key=shield_publishable_key,
            shield_secret_key=shield_secret_key,
            shield_encryption_share=encryption_share,
        )
        return {"session": session}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create session: {str(e)}")

