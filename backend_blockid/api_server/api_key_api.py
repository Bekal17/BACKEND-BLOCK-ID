"""
BlockID API Key Management — create, list, update, delete, validate.
"""
from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel, Field

from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.utils.api_key_utils import generate_api_key, hash_api_key

router = APIRouter(prefix="/keys", tags=["API Keys"])


# -----------------------------------------------------------------------------
# Pydantic Schemas
# -----------------------------------------------------------------------------


class CreateKeyRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=256, description="Key display name")
    environment: str = Field(default="live", description="live | test")
    quota_limit: int = Field(default=1000, ge=1, le=1_000_000, description="Monthly quota limit")


class CreateKeyResponse(BaseModel):
    id: str
    name: str
    raw_key: str
    key_prefix: str
    environment: str
    quota_limit: int
    created_at: datetime
    message: str


class ApiKeyItem(BaseModel):
    id: str
    user_id: str
    name: str
    key_prefix: str
    environment: str
    is_active: bool
    quota_limit: int
    quota_used: int
    quota_percentage: float
    last_used_at: datetime | None
    created_at: datetime
    updated_at: datetime


class UpdateKeyRequest(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=256)
    is_active: bool | None = None


class ValidateKeyRequest(BaseModel):
    api_key: str = Field(..., description="Raw API key to validate")


class ValidateKeyResponse(BaseModel):
    valid: bool
    user_id: str | None
    key_id: str | None
    environment: str | None
    quota_remaining: int | None


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _row_to_api_key_item(row) -> ApiKeyItem:
    quota_limit = row["quota_limit"] or 1000
    quota_used = row["quota_used"] or 0
    quota_percentage = (quota_used / quota_limit) * 100.0 if quota_limit > 0 else 0.0
    return ApiKeyItem(
        id=str(row["id"]),
        user_id=row["user_id"],
        name=row["name"],
        key_prefix=row["key_prefix"],
        environment=row["environment"],
        is_active=row["is_active"],
        quota_limit=quota_limit,
        quota_used=quota_used,
        quota_percentage=round(quota_percentage, 2),
        last_used_at=row["last_used_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


# -----------------------------------------------------------------------------
# Endpoints
# -----------------------------------------------------------------------------


@router.post("", response_model=CreateKeyResponse)
async def create_key(
    req: CreateKeyRequest,
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Create a new API key. raw_key is returned ONLY on create."""
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")

    env = (req.environment or "live").strip().lower()
    if env not in ("live", "test"):
        raise HTTPException(status_code=400, detail="environment must be live or test")

    raw_key, key_hash, key_prefix = generate_api_key(env)

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            INSERT INTO api_keys (user_id, name, key_hash, key_prefix, environment, quota_limit)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, name, key_prefix, environment, quota_limit, created_at
            """,
            user_id,
            req.name.strip(),
            key_hash,
            key_prefix,
            env,
            req.quota_limit,
        )
        return CreateKeyResponse(
            id=str(row["id"]),
            name=row["name"],
            raw_key=raw_key,
            key_prefix=row["key_prefix"],
            environment=row["environment"],
            quota_limit=row["quota_limit"],
            created_at=row["created_at"],
            message="Store this key securely. It will not be shown again.",
        )
    finally:
        await release_conn(conn)


@router.get("", response_model=list[ApiKeyItem])
async def list_keys(
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """List all API keys for the user (without key_hash)."""
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT id, user_id, name, key_prefix, environment, is_active,
                   quota_limit, quota_used, last_used_at, created_at, updated_at
            FROM api_keys
            WHERE user_id = $1
            ORDER BY created_at DESC
            """,
            user_id,
        )
        return [_row_to_api_key_item(r) for r in rows]
    finally:
        await release_conn(conn)


@router.post("/validate", response_model=ValidateKeyResponse)
async def validate_key(req: ValidateKeyRequest):
    """Validate raw key, increment quota_used, update last_used_at."""
    raw_key = (req.api_key or "").strip()
    if not raw_key:
        raise HTTPException(status_code=400, detail="api_key is required")

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
            return ValidateKeyResponse(valid=False, user_id=None, key_id=None, environment=None, quota_remaining=None)

        if not row["is_active"]:
            return ValidateKeyResponse(valid=False, user_id=None, key_id=None, environment=None, quota_remaining=None)

        quota_limit = row["quota_limit"] or 1000
        quota_used = (row["quota_used"] or 0) + 1
        if quota_used > quota_limit:
            return ValidateKeyResponse(
                valid=False,
                user_id=row["user_id"],
                key_id=str(row["id"]),
                environment=row["environment"],
                quota_remaining=0,
            )

        await conn.execute(
            """
            UPDATE api_keys
            SET quota_used = quota_used + 1, last_used_at = NOW()
            WHERE id = $1
            """,
            row["id"],
        )
        return ValidateKeyResponse(
            valid=True,
            user_id=row["user_id"],
            key_id=str(row["id"]),
            environment=row["environment"],
            quota_remaining=quota_limit - quota_used,
        )
    finally:
        await release_conn(conn)


@router.patch("/{key_id}", response_model=ApiKeyItem)
async def update_key(
    key_id: str,
    req: UpdateKeyRequest,
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Update name or toggle is_active. Ownership required."""
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")

    try:
        uid = UUID(key_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid key ID")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT id, user_id FROM api_keys WHERE id = $1",
            uid,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Key not found")
        if row["user_id"] != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to update this key")

        updates = []
        params = []
        i = 1
        if req.name is not None:
            updates.append(f"name = ${i}")
            params.append(req.name.strip())
            i += 1
        if req.is_active is not None:
            updates.append(f"is_active = ${i}")
            params.append(req.is_active)
            i += 1
        if not updates:
            pass  # no-op, return current state
        else:
            params.append(uid)
            await conn.execute(
                f"UPDATE api_keys SET {', '.join(updates)}, updated_at = NOW() WHERE id = ${i}",
                *params,
            )

        row = await conn.fetchrow(
            """
            SELECT id, user_id, name, key_prefix, environment, is_active,
                   quota_limit, quota_used, last_used_at, created_at, updated_at
            FROM api_keys
            WHERE id = $1
            """,
            uid,
        )
        return _row_to_api_key_item(row)
    finally:
        await release_conn(conn)


@router.delete("/{key_id}", status_code=204)
async def delete_key(
    key_id: str,
    x_user_id: str | None = Header(None, alias="X-User-ID"),
):
    """Delete key. Ownership required."""
    user_id = (x_user_id or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="X-User-ID header is required")

    try:
        uid = UUID(key_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid key ID")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT id, user_id FROM api_keys WHERE id = $1",
            uid,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Key not found")
        if row["user_id"] != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to delete this key")

        await conn.execute("DELETE FROM api_keys WHERE id = $1", uid)
    finally:
        await release_conn(conn)
