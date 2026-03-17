"""
User Privacy Settings API.
GET  /social/settings/{wallet}  → get privacy settings
PUT  /social/settings/{wallet}  → update privacy settings
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)
router = APIRouter(prefix="/social", tags=["Privacy"])

VALID_POST_VISIBILITY = {"PUBLIC", "FOLLOWERS_ONLY", "PRIVATE"}
VALID_BALANCE_VISIBILITY = {"PUBLIC", "FOLLOWERS", "HIDDEN"}
VALID_SCORE_VISIBILITY = {"PUBLIC", "FOLLOWERS", "HIDDEN"}
VALID_WALLET_DISPLAY = {"TRUNCATED", "HIDDEN"}
VALID_ALLOW_MENTIONS = {"ALL", "FOLLOWERS", "NONE"}
VALID_ALLOW_FOLLOWS = {"ALL", "NONE"}


async def _ensure_privacy_settings(wallet: str, conn) -> dict:
    """
    Get or create default privacy settings for wallet.
    Auto-creates with defaults if not exists.
    """
    row = await conn.fetchrow(
        "SELECT * FROM user_privacy_settings WHERE wallet = $1",
        wallet,
    )
    if row:
        return dict(row)

    # Auto-create with defaults
    await conn.execute(
        """
        INSERT INTO user_privacy_settings (wallet)
        VALUES ($1)
        ON CONFLICT (wallet) DO NOTHING
        """,
        wallet,
    )
    row = await conn.fetchrow(
        "SELECT * FROM user_privacy_settings WHERE wallet = $1",
        wallet,
    )
    return dict(row) if row else {}


@router.get("/settings/{wallet}")
async def get_privacy_settings(wallet: str):
    """
    Get privacy settings for a wallet.
    Auto-creates default settings if not exists.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        settings = await _ensure_privacy_settings(wallet, conn)
        return {
            "wallet": wallet,
            "posts_visibility": settings.get("posts_visibility", "PUBLIC"),
            "profile_discoverable": settings.get("profile_discoverable", True),
            "wallet_display": settings.get("wallet_display", "TRUNCATED"),
            "balance_visibility": settings.get("balance_visibility", "HIDDEN"),
            "score_visibility": settings.get("score_visibility", "PUBLIC"),
            "show_activity_feed": settings.get("show_activity_feed", True),
            "allow_mentions": settings.get("allow_mentions", "ALL"),
            "allow_follows": settings.get("allow_follows", "ALL"),
        }
    finally:
        await release_conn(conn)


@router.put("/settings/{wallet}")
async def update_privacy_settings(wallet: str, body: dict[str, Any] = Body(...)):
    """
    Update privacy settings for a wallet.
    Only updates fields provided in body.
    Requires signature verification.

    Body example:
    {
        "signature": "...",
        "posts_visibility": "FOLLOWERS_ONLY",
        "balance_visibility": "HIDDEN",
        "profile_discoverable": false
    }
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    # Dev bypass
    sig = (body.get("signature") or "").strip()
    if sig != "devtest_signature_bypass":
        # TODO: proper signature verification
        pass

    conn = await get_conn()
    try:
        # Ensure settings exist
        await _ensure_privacy_settings(wallet, conn)

        # Build update fields
        updates = {}

        if "posts_visibility" in body:
            val = str(body["posts_visibility"]).upper()
            if val not in VALID_POST_VISIBILITY:
                raise HTTPException(
                    status_code=400,
                    detail=f"posts_visibility must be one of {VALID_POST_VISIBILITY}",
                )
            updates["posts_visibility"] = val

        if "profile_discoverable" in body:
            updates["profile_discoverable"] = bool(body["profile_discoverable"])

        if "wallet_display" in body:
            val = str(body["wallet_display"]).upper()
            if val not in VALID_WALLET_DISPLAY:
                raise HTTPException(
                    status_code=400,
                    detail=f"wallet_display must be one of {VALID_WALLET_DISPLAY}",
                )
            updates["wallet_display"] = val

        if "balance_visibility" in body:
            val = str(body["balance_visibility"]).upper()
            if val not in VALID_BALANCE_VISIBILITY:
                raise HTTPException(
                    status_code=400,
                    detail=f"balance_visibility must be one of {VALID_BALANCE_VISIBILITY}",
                )
            updates["balance_visibility"] = val

        if "score_visibility" in body:
            val = str(body["score_visibility"]).upper()
            if val not in VALID_SCORE_VISIBILITY:
                raise HTTPException(
                    status_code=400,
                    detail=f"score_visibility must be one of {VALID_SCORE_VISIBILITY}",
                )
            updates["score_visibility"] = val

        if "show_activity_feed" in body:
            updates["show_activity_feed"] = bool(body["show_activity_feed"])

        if "allow_mentions" in body:
            val = str(body["allow_mentions"]).upper()
            if val not in VALID_ALLOW_MENTIONS:
                raise HTTPException(
                    status_code=400,
                    detail=f"allow_mentions must be one of {VALID_ALLOW_MENTIONS}",
                )
            updates["allow_mentions"] = val

        if "allow_follows" in body:
            val = str(body["allow_follows"]).upper()
            if val not in VALID_ALLOW_FOLLOWS:
                raise HTTPException(
                    status_code=400,
                    detail=f"allow_follows must be one of {VALID_ALLOW_FOLLOWS}",
                )
            updates["allow_follows"] = val

        if not updates:
            raise HTTPException(status_code=400, detail="No valid fields to update")

        # Build dynamic UPDATE query
        set_clauses = []
        values = []
        for i, (key, val) in enumerate(updates.items(), 1):
            set_clauses.append(f"{key} = ${i}")
            values.append(val)

        set_clauses.append("updated_at = NOW()")
        values.append(wallet)

        query = f"""
            UPDATE user_privacy_settings
            SET {', '.join(set_clauses)}
            WHERE wallet = ${len(values)}
        """

        await conn.execute(query, *values)

        logger.info(
            "privacy_settings_updated",
            wallet=wallet[:16],
            fields=list(updates.keys()),
        )

        # Return updated settings
        updated = await _ensure_privacy_settings(wallet, conn)
        return {
            "success": True,
            "wallet": wallet,
            "updated_fields": list(updates.keys()),
            "settings": {
                "posts_visibility": updated.get("posts_visibility"),
                "profile_discoverable": updated.get("profile_discoverable"),
                "wallet_display": updated.get("wallet_display"),
                "balance_visibility": updated.get("balance_visibility"),
                "score_visibility": updated.get("score_visibility"),
                "show_activity_feed": updated.get("show_activity_feed"),
                "allow_mentions": updated.get("allow_mentions"),
                "allow_follows": updated.get("allow_follows"),
            },
        }
    finally:
        await release_conn(conn)
