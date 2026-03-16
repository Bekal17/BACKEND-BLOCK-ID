from __future__ import annotations

import json
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

_ALLOWED_ACTION_TYPES = {
    "SCORE_OVERRIDE",
    "FORCE_RECALCULATE",
    "MANUAL_BAN",
    "MANUAL_UNBAN",
    "WEIGHT_CHANGE",
    "MIGRATION",
}


async def log_admin_action(
    action_type: str,
    value_before: dict[str, Any] | None = None,
    value_after: dict[str, Any] | None = None,
    reason: str | None = None,
    admin_id: str | None = None,
    wallet: str | None = None,
) -> None:
    """
    Log admin action to admin_actions table.
    Non-fatal: never raises exception.
    """
    try:
        atype = (action_type or "").strip().upper()
        if atype not in _ALLOWED_ACTION_TYPES:
            atype = "MIGRATION"

        wb = (wallet or "").strip() or None

        vb_json = None
        va_json = None
        try:
            if value_before is not None:
                vb_json = json.dumps(value_before)
            if value_after is not None:
                va_json = json.dumps(value_after)
        except Exception:
            vb_json = None
            va_json = None

        conn = await get_conn()
        try:
            await conn.execute(
                """
                INSERT INTO admin_actions (
                    wallet,
                    action_type,
                    value_before,
                    value_after,
                    reason,
                    admin_id,
                    executed_at
                )
                VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, NOW())
                """,
                wb,
                atype,
                vb_json,
                va_json,
                reason,
                admin_id,
            )
        finally:
            await release_conn(conn)
    except Exception as e:  # pragma: no cover - best-effort logging
        logger.warning("log_admin_action_failed", action_type=action_type, error=str(e))

