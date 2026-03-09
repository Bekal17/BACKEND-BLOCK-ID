"""
API Key authentication middleware for /v1/ routes.

Checks Authorization: Bearer blk_live_xxx (or blk_test_xxx).
Returns 401 if invalid/inactive, 429 if quota exceeded.
Injects user_id, api_key_id, environment into request.state.
Buffers usage for hourly flush to api_usage_hourly.
"""
from __future__ import annotations

import asyncio
import threading
import time
from datetime import datetime, timezone

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.utils.api_key_utils import hash_api_key


# In-memory buffer: key = "{api_key_id}:{endpoint}:{hour_bucket}"
# value = { api_key_id, endpoint, hour_bucket, request_count, success_count, error_count, response_ms_total }
_usage_buffer: dict[str, dict] = {}
_usage_buffer_lock = threading.Lock()


def get_hour_bucket() -> str:
    """Return current UTC time rounded down to the hour (ISO format for DB)."""
    now = datetime.now(timezone.utc)
    return now.replace(minute=0, second=0, microsecond=0).isoformat()


def record_usage(api_key_id: str, endpoint: str, status_code: int, response_ms: float) -> None:
    """Record usage in buffer. Synchronous and fast; never blocks."""
    hour_bucket = get_hour_bucket()
    endpoint = (endpoint or "").strip() or "/"
    key = f"{api_key_id}:{endpoint}:{hour_bucket}"

    with _usage_buffer_lock:
        if key not in _usage_buffer:
            _usage_buffer[key] = {
                "api_key_id": api_key_id,
                "endpoint": endpoint,
                "hour_bucket": hour_bucket,
                "request_count": 0,
                "success_count": 0,
                "error_count": 0,
                "response_ms_total": 0.0,
            }
        entry = _usage_buffer[key]
        entry["request_count"] += 1
        if status_code < 400:
            entry["success_count"] += 1
        else:
            entry["error_count"] += 1
        entry["response_ms_total"] += response_ms


async def flush_usage_to_db() -> None:
    """Flush buffer to api_usage_hourly. Handles all DB errors silently."""
    with _usage_buffer_lock:
        snapshot = dict(_usage_buffer)
        _usage_buffer.clear()

    if not snapshot:
        return

    conn = None
    try:
        conn = await get_conn()
        for item in snapshot.values():
            try:
                request_count = item["request_count"]
                avg_response_ms = item["response_ms_total"] / request_count if request_count > 0 else 0.0
                await conn.execute(
                    """
                    INSERT INTO api_usage_hourly
                        (api_key_id, hour_bucket, endpoint, request_count, success_count, error_count, avg_response_ms)
                    VALUES ($1::uuid, $2::timestamptz, $3, $4, $5, $6, $7)
                    ON CONFLICT (api_key_id, hour_bucket, endpoint) DO UPDATE SET
                        request_count = api_usage_hourly.request_count + EXCLUDED.request_count,
                        success_count = api_usage_hourly.success_count + EXCLUDED.success_count,
                        error_count = api_usage_hourly.error_count + EXCLUDED.error_count,
                        avg_response_ms = (
                            api_usage_hourly.avg_response_ms * api_usage_hourly.request_count
                            + EXCLUDED.avg_response_ms * EXCLUDED.request_count
                        ) / NULLIF(api_usage_hourly.request_count + EXCLUDED.request_count, 0)
                    """,
                    item["api_key_id"],
                    item["hour_bucket"],
                    item["endpoint"],
                    item["request_count"],
                    item["success_count"],
                    item["error_count"],
                    avg_response_ms,
                )
            except Exception:
                pass
    except Exception:
        pass
    finally:
        if conn is not None:
            await release_conn(conn)


async def start_hourly_flush(app) -> None:
    """Background task: flush usage buffer to DB every hour."""
    while True:
        await asyncio.sleep(3600)
        await flush_usage_to_db()


def _extract_bearer_key(request: Request) -> str | None:
    auth = request.headers.get("Authorization") or ""
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return None


def _is_v1_path(path: str) -> bool:
    return path.startswith("/v1/")


class ApiKeyMiddleware(BaseHTTPMiddleware):
    """
    Authenticate requests to /v1/ routes using API keys.
    Skips paths that do not start with /v1/.
    """

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method == "OPTIONS":
            return await call_next(request)

        path = request.url.path
        if not _is_v1_path(path):
            return await call_next(request)

        raw_key = _extract_bearer_key(request)
        if not raw_key or not (raw_key.startswith("blk_live_") or raw_key.startswith("blk_test_")):
            return JSONResponse(
                status_code=401,
                content={"detail": "Invalid or missing API key. Use Authorization: Bearer blk_live_xxx"},
            )

        key_hash = hash_api_key(raw_key)

        conn = await get_conn()
        try:
            row = await conn.fetchrow(
                """
                SELECT id, user_id, environment, is_active, quota_limit, quota_used
                FROM api_keys
                WHERE key_hash = $1
                """,
                key_hash,
            )
            if not row:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid API key"},
                )
            if not row["is_active"]:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "API key is inactive"},
                )

            quota_limit = row["quota_limit"] or 1000
            quota_used = row["quota_used"] or 0
            if quota_used >= quota_limit:
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": "Quota exceeded",
                        "quota_limit": quota_limit,
                        "quota_used": quota_used,
                    },
                )

            # Inject into request.state
            request.state.user_id = row["user_id"]
            request.state.api_key_id = str(row["id"])
            request.state.environment = row["environment"]

            start_ms = time.perf_counter() * 1000
            response = await call_next(request)
            response_ms = time.perf_counter() * 1000 - start_ms

            # Increment quota and update last_used_at on success
            if 200 <= response.status_code < 300:
                await conn.execute(
                    """
                    UPDATE api_keys
                    SET quota_used = quota_used + 1, last_used_at = NOW()
                    WHERE id = $1
                    """,
                    row["id"],
                )

            # Track usage in buffer for hourly flush
            record_usage(
                str(row["id"]),
                request.url.path,
                response.status_code,
                response_ms,
            )

            return response
        finally:
            await release_conn(conn)
