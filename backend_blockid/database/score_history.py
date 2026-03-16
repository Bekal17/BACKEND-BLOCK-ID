from __future__ import annotations

import json
from typing import Any, Iterable

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

_ALLOWED_CHANGE_CATEGORIES = {
    "BEHAVIORAL",
    "SOCIAL_ACTION",
    "MODERATION",
    "LINKING",
    "ADMIN",
    "SYSTEM",
}

_ALLOWED_TRIGGERED_BY = {
    "realtime_pipeline",
    "moderation_engine",
    "social_engine",
    "linking_engine",
    "admin_panel",
    "db_trigger",
}


async def log_score_change(
    wallet: str,
    score_before: float | None,
    score_after: float,
    change_category: str,
    triggered_by: str,
    reason_codes: Iterable[str] | None = None,
    violation_level: int | None = None,
    confidence: float | None = None,
    ml_score: float | None = None,
    dynamic_risk: float | None = None,
    reason_penalty: float | None = None,
    graph_penalty: float | None = None,
    decay: float | None = None,
    activity_boost: float | None = None,
    risk_level: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    """
    Non-fatal audit hook for trust_score_history.
    Never raises; logs a warning on failure.
    """
    try:
        wallet = (wallet or "").strip()
        if not wallet:
            return

        category = (change_category or "").upper()
        if category not in _ALLOWED_CHANGE_CATEGORIES:
            category = "SYSTEM"

        trig = triggered_by or "db_trigger"
        if trig not in _ALLOWED_TRIGGERED_BY:
            trig = "db_trigger"

        rc_array: list[str] | None = None
        if reason_codes is not None:
            rc_array = [str(rc).strip() for rc in reason_codes if str(rc).strip()]
            if not rc_array:
                rc_array = None

        metadata_json: Any = None
        if metadata is not None:
            try:
                metadata_json = json.dumps(metadata)
            except Exception:
                metadata_json = None

        conn = await get_conn()
        try:
            await conn.execute(
                """
                INSERT INTO trust_score_history (
                    wallet,
                    score_before,
                    score_after,
                    change_category,
                    triggered_by,
                    reason_codes,
                    violation_level,
                    confidence,
                    ml_score,
                    dynamic_risk,
                    reason_penalty,
                    graph_penalty,
                    decay,
                    activity_boost,
                    risk_level,
                    metadata,
                    recorded_at
                )
                VALUES (
                    $1, $2, $3,
                    $4, $5,
                    $6,
                    $7, $8,
                    $9, $10, $11, $12, $13, $14,
                    $15,
                    $16::jsonb,
                    NOW()
                )
                """,
                wallet,
                float(score_before) if score_before is not None else None,
                float(score_after),
                category,
                trig,
                rc_array,
                int(violation_level) if violation_level is not None else None,
                float(confidence) if confidence is not None else None,
                float(ml_score) if ml_score is not None else None,
                float(dynamic_risk) if dynamic_risk is not None else None,
                float(reason_penalty) if reason_penalty is not None else None,
                float(graph_penalty) if graph_penalty is not None else None,
                float(decay) if decay is not None else None,
                float(activity_boost) if activity_boost is not None else None,
                risk_level,
                metadata_json,
            )
        finally:
            await release_conn(conn)
    except Exception as e:  # pragma: no cover - best-effort logging
        logger.warning("log_score_change_failed", wallet=wallet[:16], error=str(e))

