"""
BlockID Usage API — quota summary, daily charts, endpoint stats, status breakdown.
Requires X-User-ID header. Joins with api_keys for user filtering.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Header, Query
from pydantic import BaseModel

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter(prefix="/usage", tags=["Usage"])


# -----------------------------------------------------------------------------
# Response models
# -----------------------------------------------------------------------------


class UsageSummaryResponse(BaseModel):
    quota_used: int
    quota_limit: int
    quota_percentage: float
    success_count: int
    error_count: int
    avg_response_ms: float


class DailyUsageItem(BaseModel):
    date: str
    requests: int


class EndpointUsageItem(BaseModel):
    endpoint: str
    requests: int


class StatusUsageItem(BaseModel):
    name: str
    value: int


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _get_user_id(x_user_id: str | None) -> str:
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")
    return user_id


def _parse_range(range_param: str) -> int:
    r = (range_param or "30d").strip().lower()
    if r == "7d":
        return 7
    if r == "90d":
        return 90
    return 30


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------


@router.get("/summary", response_model=UsageSummaryResponse)
async def get_usage_summary(
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Monthly quota summary for the current user."""
    user_id = _get_user_id(x_user_id)

    conn = await get_conn()
    try:
        # Get quota_limit from api_keys (sum for user)
        quota_row = await conn.fetchrow(
            """
            SELECT COALESCE(SUM(quota_limit), 0)::bigint AS quota_limit
            FROM api_keys
            WHERE user_id = $1 AND is_active = true
            """,
            user_id,
        )
        quota_limit = int(quota_row["quota_limit"] or 0)

        # Sum api_usage_hourly for current month, joined with api_keys
        try:
            row = await conn.fetchrow(
                """
                SELECT
                    COALESCE(SUM(u.request_count), 0)::bigint AS quota_used,
                    COALESCE(SUM(u.success_count), 0)::bigint AS success_count,
                    COALESCE(SUM(u.error_count), 0)::bigint AS error_count,
                    CASE WHEN SUM(u.request_count) > 0
                        THEN SUM(u.avg_response_ms * u.request_count) / NULLIF(SUM(u.request_count), 0)
                        ELSE 0
                    END AS avg_response_ms
                FROM api_usage_hourly u
                JOIN api_keys k ON k.id = u.api_key_id AND k.user_id = $1
                WHERE u.hour_bucket >= date_trunc('month', NOW())
                  AND u.hour_bucket < date_trunc('month', NOW()) + interval '1 month'
                """,
                user_id,
            )
        except Exception:
            row = None

        if not row:
            return UsageSummaryResponse(
                quota_used=0,
                quota_limit=quota_limit,
                quota_percentage=0.0,
                success_count=0,
                error_count=0,
                avg_response_ms=0.0,
            )

        quota_used = int(row["quota_used"] or 0)
        success_count = int(row["success_count"] or 0)
        error_count = int(row["error_count"] or 0)
        avg_ms = float(row["avg_response_ms"] or 0.0)
        quota_pct = (quota_used / quota_limit * 100.0) if quota_limit > 0 else 0.0

        return UsageSummaryResponse(
            quota_used=quota_used,
            quota_limit=quota_limit,
            quota_percentage=round(quota_pct, 1),
            success_count=success_count,
            error_count=error_count,
            avg_response_ms=round(avg_ms, 1),
        )
    finally:
        await release_conn(conn)


@router.get("/daily", response_model=list[DailyUsageItem])
async def get_usage_daily(
    range_param: str = Query("30d", alias="range", description="7d, 30d, or 90d"),
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Daily request counts for charting."""
    user_id = _get_user_id(x_user_id)
    days = _parse_range(range_param)

    conn = await get_conn()
    try:
        try:
            rows = await conn.fetch(
                """
                SELECT
                    to_char(DATE(u.hour_bucket), 'Mon DD') AS date,
                    COALESCE(SUM(u.request_count), 0)::bigint AS requests
                FROM api_usage_hourly u
                JOIN api_keys k ON k.id = u.api_key_id AND k.user_id = $1
                WHERE u.hour_bucket >= NOW() - ($2::int * interval '1 day')
                GROUP BY DATE(u.hour_bucket)
                ORDER BY DATE(u.hour_bucket)
                """,
                user_id,
                days,
            )
        except Exception:
            rows = []

        return [DailyUsageItem(date=r["date"], requests=int(r["requests"] or 0)) for r in rows]
    finally:
        await release_conn(conn)


@router.get("/endpoints", response_model=list[EndpointUsageItem])
async def get_usage_endpoints(
    range_param: str = Query("30d", alias="range", description="7d, 30d, or 90d"),
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Request count grouped by endpoint."""
    user_id = _get_user_id(x_user_id)
    days = _parse_range(range_param)

    conn = await get_conn()
    try:
        try:
            rows = await conn.fetch(
                """
                SELECT
                    COALESCE(u.endpoint, 'unknown') AS endpoint,
                    COALESCE(SUM(u.request_count), 0)::bigint AS requests
                FROM api_usage_hourly u
                JOIN api_keys k ON k.id = u.api_key_id AND k.user_id = $1
                WHERE u.hour_bucket >= NOW() - ($2::int * interval '1 day')
                GROUP BY u.endpoint
                ORDER BY requests DESC
                """,
                user_id,
                days,
            )
        except Exception:
            rows = []

        return [EndpointUsageItem(endpoint=r["endpoint"], requests=int(r["requests"] or 0)) for r in rows]
    finally:
        await release_conn(conn)


@router.get("/status", response_model=list[StatusUsageItem])
async def get_usage_status(
    range_param: str = Query("30d", alias="range", description="7d, 30d, or 90d"),
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Success vs error breakdown from success_count and error_count."""
    user_id = _get_user_id(x_user_id)
    days = _parse_range(range_param)

    conn = await get_conn()
    try:
        try:
            rows = await conn.fetch(
                """
                SELECT
                    COALESCE(SUM(u.success_count), 0)::bigint AS success_count,
                    COALESCE(SUM(u.error_count), 0)::bigint AS error_count
                FROM api_usage_hourly u
                JOIN api_keys k ON k.id = u.api_key_id AND k.user_id = $1
                WHERE u.hour_bucket >= NOW() - ($2::int * interval '1 day')
                """,
                user_id,
                days,
            )
        except Exception:
            rows = []

        if not rows:
            return [
                StatusUsageItem(name="200 OK", value=0),
                StatusUsageItem(name="Errors", value=0),
            ]

        r = rows[0]
        success = int(r["success_count"] or 0)
        errors = int(r["error_count"] or 0)

        return [
            StatusUsageItem(name="200 OK", value=success),
            StatusUsageItem(name="Errors", value=errors),
        ]
    finally:
        await release_conn(conn)
