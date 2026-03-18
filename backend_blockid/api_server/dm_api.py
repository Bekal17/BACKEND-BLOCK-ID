"""
Direct Messages API for BlockID.
Plaintext DM — access controlled by privacy settings.
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body, HTTPException

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)
router = APIRouter(prefix="/social/dm", tags=["DM"])


async def _check_dm_allowed(
    from_wallet: str,
    to_wallet: str,
    conn,
) -> bool:
    """
    Check if from_wallet can DM to_wallet.
    Based on to_wallet's privacy settings (allow_follows).

    Rules:
    - allow_follows = 'ALL' → anyone can DM
    - allow_follows = 'NONE' → no one can DM
    - Default (no settings) → allow
    """
    settings = await conn.fetchrow(
        "SELECT allow_follows FROM user_privacy_settings "
        "WHERE wallet = $1",
        to_wallet,
    )
    if not settings:
        return True  # no settings = allow by default

    allow = settings.get("allow_follows", "ALL")
    if allow == "NONE":
        return False
    return True


@router.post("/send")
async def send_dm(body: dict[str, Any] = Body(...)):
    """
    Send a direct message.

    Body: {
        wallet: str,          # sender
        to_wallet: str,       # recipient
        content: str,         # message text
        signature: str
    }
    """
    from_wallet = (body.get("wallet") or "").strip()
    to_wallet = (body.get("to_wallet") or "").strip()
    content = (body.get("content") or "").strip()

    if not from_wallet or not to_wallet or not content:
        raise HTTPException(
            status_code=400,
            detail="wallet, to_wallet, and content required",
        )

    if from_wallet == to_wallet:
        raise HTTPException(
            status_code=400,
            detail="Cannot send DM to yourself",
        )

    if len(content) > 1000:
        raise HTTPException(
            status_code=400,
            detail="Message too long (max 1000 chars)",
        )

    conn = await get_conn()
    try:
        allowed = await _check_dm_allowed(from_wallet, to_wallet, conn)
        if not allowed:
            raise HTTPException(
                status_code=403,
                detail="This wallet does not accept messages",
            )

        msg = await conn.fetchrow(
            """
            INSERT INTO direct_messages
                (from_wallet, to_wallet, content)
            VALUES ($1, $2, $3)
            RETURNING id, from_wallet, to_wallet,
                      content, is_read, created_at
            """,
            from_wallet,
            to_wallet,
            content,
        )

        logger.info(
            "dm_sent",
            from_wallet=from_wallet[:16],
            to_wallet=to_wallet[:16],
        )

        return {
            "success": True,
            "message": dict(msg),
        }
    finally:
        await release_conn(conn)


@router.get("/conversations/{wallet}")
async def get_conversations(wallet: str):
    """
    Get all conversations for a wallet.
    Returns list of unique conversation partners
    with last message and unread count.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            WITH conversations AS (
                SELECT
                    CASE
                        WHEN from_wallet = $1 THEN to_wallet
                        ELSE from_wallet
                    END AS other_wallet,
                    MAX(created_at) AS last_message_at,
                    COUNT(*) FILTER (
                        WHERE to_wallet = $1
                        AND is_read = FALSE
                    ) AS unread_count
                FROM direct_messages
                WHERE from_wallet = $1 OR to_wallet = $1
                GROUP BY
                    CASE
                        WHEN from_wallet = $1 THEN to_wallet
                        ELSE from_wallet
                    END
            )
            SELECT
                c.other_wallet,
                c.last_message_at,
                c.unread_count,
                dm.content AS last_message,
                hr.handle
            FROM conversations c
            LEFT JOIN direct_messages dm ON (
                (dm.from_wallet = $1 AND dm.to_wallet = c.other_wallet)
                OR
                (dm.from_wallet = c.other_wallet AND dm.to_wallet = $1)
            )
            AND dm.created_at = c.last_message_at
            LEFT JOIN handle_registry hr
                ON hr.owner_wallet = c.other_wallet
            ORDER BY c.last_message_at DESC
            LIMIT 50
            """,
            wallet,
        )

        conversations = []
        seen: set[str] = set()
        for row in rows:
            other = row["other_wallet"]
            if other in seen:
                continue
            seen.add(other)
            last_at = row.get("last_message_at")
            conversations.append({
                "other_wallet": other,
                "handle": row.get("handle"),
                "last_message": row.get("last_message"),
                "last_message_at": last_at.isoformat() if last_at else None,
                "unread_count": int(row.get("unread_count") or 0),
            })

        return {
            "wallet": wallet,
            "conversations": conversations,
            "total": len(conversations),
        }
    finally:
        await release_conn(conn)


@router.get("/messages/{wallet}/{other_wallet}")
async def get_messages(wallet: str, other_wallet: str):
    """
    Get all messages between two wallets.
    Also marks received messages as read.
    """
    wallet = (wallet or "").strip()
    other_wallet = (other_wallet or "").strip()

    if not wallet or not other_wallet:
        raise HTTPException(status_code=400, detail="Both wallets required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT id, from_wallet, to_wallet,
                   content, is_read, created_at
            FROM direct_messages
            WHERE (from_wallet = $1 AND to_wallet = $2)
               OR (from_wallet = $2 AND to_wallet = $1)
            ORDER BY created_at ASC
            LIMIT 100
            """,
            wallet,
            other_wallet,
        )

        await conn.execute(
            """
            UPDATE direct_messages
            SET is_read = TRUE
            WHERE to_wallet = $1
              AND from_wallet = $2
              AND is_read = FALSE
            """,
            wallet,
            other_wallet,
        )

        messages = [
            {
                "id": r["id"],
                "from_wallet": r["from_wallet"],
                "to_wallet": r["to_wallet"],
                "content": r["content"],
                "is_read": True
                if r["to_wallet"] == wallet
                else r["is_read"],
                "created_at": r["created_at"].isoformat()
                if r.get("created_at")
                else None,
                "is_mine": r["from_wallet"] == wallet,
            }
            for r in rows
        ]

        return {
            "wallet": wallet,
            "other_wallet": other_wallet,
            "messages": messages,
            "total": len(messages),
        }
    finally:
        await release_conn(conn)


@router.get("/unread-count/{wallet}")
async def get_unread_count(wallet: str):
    """Get total unread DM count for a wallet (notification badge)."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        count = await conn.fetchval(
            """
            SELECT COUNT(*) FROM direct_messages
            WHERE to_wallet = $1 AND is_read = FALSE
            """,
            wallet,
        )
        return {
            "wallet": wallet,
            "unread_count": int(count or 0),
        }
    finally:
        await release_conn(conn)
