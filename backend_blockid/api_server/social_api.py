from __future__ import annotations

import asyncio
import os
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, Query, Request, status
from pydantic import BaseModel, Field

from backend_blockid.api_server.identity_eligibility import get_score_tier
from backend_blockid.api_server.session_auth import verify_session_token
from backend_blockid.api_server.signature_verify import BLOCKID_ENV, DEVNET_BYPASS, verify_or_raise
from backend_blockid.api_server.privacy_api import _ensure_privacy_settings
from backend_blockid.api_server.vision_moderation import check_image_safe
from backend_blockid.integrations.r2_client import upload_image
from backend_blockid.api_server.social_moderation import (
    check_appeal,
    check_post_visibility,
    process_flag,
)
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.score_history import log_score_change
from backend_blockid.config.env import get_helius_api_key
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.database.repositories import insert_wallet_reason
from backend_blockid.utils.og_fetcher import extract_first_url, fetch_og_metadata


logger = get_logger(__name__)

router = APIRouter(prefix="/social", tags=["social"])

ENDORSE_TRUST_BOOST = 5
SOCIAL_MIN_SCORE_TO_ENDORSE = 50
SCORE_CAP = 97
NEGATIVE_CODES = {
    "SCAM_CLUSTER_MEMBER",
    "SCAM_CLUSTER_MEMBER_SMALL",
    "SCAM_CLUSTER_MEMBER_LARGE",
    "DRAINER_FLOW",
    "DRAINER_FLOW_DETECTED",
    "MEGA_DRAINER",
    "RUG_PULL_DEPLOYER",
    "HIGH_RISK_TOKEN_INTERACTION",
    "SUSPICIOUS_TOKEN_MINT",
    "BLACKLISTED_CREATOR",
    "VICTIM_OF_SCAM",
    "HIGH_VALUE_OUTFLOW",
}

TREASURY_WALLET = os.getenv(
    "TREASURY_WALLET",
    "4DdLPRDiLRY8Q2E4Fv31kvcfMf3XJf11HgaSaW7tKVcx",
)
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

PLAN_PRICES = {
    "explorer": {"monthly": 9.0, "annual": 86.4},
    "pro": {"monthly": 29.0, "annual": 278.4},
}

SOL_TOLERANCE = 0.03  # 3% tolerance for SOL price slippage


class PostResponse(BaseModel):
    id: int
    wallet: str
    handle: Optional[str] = None
    content: str
    image_url: Optional[str] = None
    post_type: str
    trust_score: Optional[float] = None
    risk_level: Optional[str] = None
    is_hidden: bool
    created_at: datetime


class CreatePostRequest(BaseModel):
    wallet: str
    content: str = Field(..., max_length=500)
    post_type: str = Field(default="PUBLIC")
    parent_id: Optional[int] = None
    signed_message: str = ""
    signature: str = ""
    session_token: str = ""


class SetBadgesRequest(BaseModel):
    wallet: str
    badges: list[str]


async def _has_identity_nft(wallet: str) -> bool:
    """Check if wallet has Identity NFT (mint_status = MINTED)."""
    conn = await get_conn()
    try:
        row = await conn.fetchval(
            "SELECT 1 FROM identity_nft WHERE wallet = $1 AND (mint_status = $2 OR mint_address IS NOT NULL)",
            wallet,
            "MINTED",
        )
        return row is not None
    finally:
        await release_conn(conn)


async def _require_identity_nft(wallet: str) -> None:
    if not await _has_identity_nft(wallet):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Identity NFT required",
        )


async def _notify(
    conn,
    wallet: str,
    notif_type: str,
    from_wallet: str,
    post_id: Optional[int] = None,
) -> None:
    """Insert notification. Non-blocking, errors logged only."""
    try:
        await conn.execute(
            """
            INSERT INTO social_notifications
            (wallet, type, from_wallet, post_id, created_at)
            VALUES ($1, $2, $3, $4, NOW())
            """,
            wallet,
            notif_type,
            from_wallet,
            post_id,
        )
    except Exception as e:
        logger.debug("notify_failed", error=str(e))


@router.post("/post", response_model=PostResponse)
async def create_post(body: CreatePostRequest):
    wallet = (body.wallet or "").strip()
    content = (body.content or "").strip()
    post_type = body.post_type or "PUBLIC"
    parent_id = body.parent_id
    signed_message = body.signed_message
    signature = body.signature
    session_token = getattr(body, "session_token", "") or ""

    if not wallet:
        raise HTTPException(status_code=400, detail="Wallet is required")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(wallet)

    if not content:
        raise HTTPException(status_code=400, detail="Content is required")
    if len(content) > 500:
        raise HTTPException(status_code=400, detail="Content too long")

    conn = await get_conn()
    try:
        # Score gate: require score >= 40 to post
        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1", wallet
        )
        score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        tier = get_score_tier(score)

        if tier == "BLOCKED":
            raise HTTPException(
                status_code=403,
                detail="Trust score too low to post. "
                "Score 30+ required for basic access.",
            )
        if tier == "READ_ONLY":
            raise HTTPException(
                status_code=403,
                detail="Score 40+ required to create posts.",
            )

        # Rate limit for BASIC tier (score 40-49): 3 posts/day
        if tier == "BASIC":
            today_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM social_posts
                WHERE wallet = $1
                  AND created_at > NOW() - INTERVAL '24 hours'
                """,
                wallet,
            )
            if (today_count or 0) >= 3:
                raise HTTPException(
                    status_code=429,
                    detail="Daily post limit reached (3/day for your score tier). "
                    "Reach score 50+ for unlimited posting.",
                )

        visibility = await check_post_visibility(wallet, conn)
        if not visibility["can_post"]:
            raise HTTPException(status_code=403, detail=visibility.get("reason") or "NOT_ALLOWED")

        # Get user stats for context-aware moderation
        user_stats = await conn.fetchrow(
            """
            SELECT
                ts.score AS trust_score,
                COALESCE(inft.wallet_age_days, 0) AS wallet_age_days,
                (SELECT COUNT(*) FROM social_posts sp WHERE sp.wallet = ts.wallet) AS post_count
            FROM trust_scores ts
            LEFT JOIN identity_nft inft ON inft.wallet = ts.wallet
            WHERE ts.wallet = $1
            """,
            wallet,
        )
        trust_score_val = float(user_stats["trust_score"] or 0) if user_stats else 0.0
        wallet_age_days_val = int(user_stats["wallet_age_days"] or 0) if user_stats else 0
        post_count_val = int(user_stats["post_count"] or 0) if user_stats else 0

        # Context-aware content check
        from backend_blockid.api_server.content_moderation import (
            check_content,
            apply_content_penalty,
            log_violation,
        )
        moderation = await check_content(
            content,
            trust_score=trust_score_val,
            wallet_age_days=wallet_age_days_val,
            post_count=post_count_val,
        )

        if moderation.get("downgraded"):
            logger.debug(
                "content_moderation_downgraded",
                wallet=wallet[:16],
                original_level=moderation["violation_level"] + 1,
                downgraded_to=moderation["violation_level"],
                context=moderation.get("context"),
            )

        if moderation["action"] == "REJECT":
            await apply_content_penalty(wallet, moderation["violation_level"], conn)
            await log_violation(wallet, content[:100], moderation["violation_level"], conn)
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "CONTENT_VIOLATION",
                    "level": moderation["violation_level"],
                    "message": "Post contains inappropriate content",
                    "violations": moderation["violations"],
                },
            )

        if moderation["action"] == "BLOCK":
            await apply_content_penalty(wallet, 4, conn)
            await log_violation(wallet, content[:100], 4, conn)
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "ACCOUNT_BLOCKED",
                    "message": "Account permanently disabled for content violations",
                },
            )

        if moderation["action"] == "ALLOW_CENSORED":
            content = moderation["cleaned_text"]
            await apply_content_penalty(wallet, 1, conn)
            await log_violation(wallet, content[:100], 1, conn)

        # --- Link preview: extract URL and fetch OG metadata ---
        link_url = None
        link_title = None
        link_description = None
        link_image = None

        detected_url = extract_first_url(content)
        if detected_url:
            try:
                og = await fetch_og_metadata(detected_url)
                if og:
                    link_url = og.get("url")
                    link_title = og.get("title")
                    link_description = og.get("description")
                    link_image = og.get("image")
            except Exception as e:
                logger.debug("og_fetch_post_error", error=str(e))

        image_url: Optional[str] = None
        image_key: Optional[str] = None

        # Snapshot trust and risk
        ts_row = await conn.fetchrow(
            "SELECT score AS trust_score, risk_level FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        trust_score = float(ts_row["trust_score"]) if ts_row and ts_row["trust_score"] is not None else None
        risk_level = (ts_row["risk_level"] or "").upper() if ts_row and ts_row["risk_level"] else None

        auto_hide = bool(visibility.get("auto_hide"))
        hide_reason = visibility.get("hide_reason")

        row = await conn.fetchrow(
            """
            INSERT INTO social_posts (
                wallet, handle, content, image_url, image_key,
                post_type, parent_id, is_hidden, hide_reason,
                trust_score, risk_level,
                link_url, link_title, link_description, link_image
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                $2, $3, $4,
                $5, $6, $7, $8,
                $9, $10,
                $11, $12, $13, $14
            )
            RETURNING id, wallet, handle, content, image_url, post_type,
                      trust_score, risk_level, is_hidden, created_at
            """,
            wallet,
            content,
            image_url,
            image_key,
            post_type,
            parent_id,
            auto_hide,
            hide_reason,
            trust_score,
            risk_level,
            link_url,
            link_title,
            link_description,
            link_image,
        )
        if parent_id:
            await conn.execute(
                "UPDATE social_posts SET reply_count = reply_count + 1 WHERE id = $1",
                parent_id,
            )
            parent_row = await conn.fetchrow(
                "SELECT wallet FROM social_posts WHERE id = $1", parent_id
            )
            if parent_row and parent_row["wallet"] != wallet:
                await _notify(conn, parent_row["wallet"], "REPLY", wallet, parent_id)

        return PostResponse(**dict(row))
    finally:
        await release_conn(conn)


@router.post("/post/with-image", response_model=PostResponse)
async def create_post_with_image(
    wallet: str = Form(...),
    content: str = Form(...),
    post_type: str = Form(default="PUBLIC"),
    parent_id: Optional[str] = Form(default=None),
    session_token: str = Form(default=""),
    signature: str = Form(default=""),
    image: Optional[UploadFile] = File(default=None),
):
    """
    Create a post with optional image upload.
    Image is uploaded to R2 and moderated via Vision API.

    Flow:
    1. Verify session token
    2. If image provided:
       a. Read image bytes
       b. Check file size (max 5MB)
       c. Check content type (jpeg/png/gif/webp)
       d. Vision API moderation check
       e. Upload to R2 -> get image_url, image_key
    3. Create post with same logic as create_post.

    Returns same response as create_post.
    """
    wallet = (wallet or "").strip()
    content = (content or "").strip()
    post_type = post_type or "PUBLIC"

    if not wallet:
        raise HTTPException(status_code=400, detail="Wallet is required")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(status_code=401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(status_code=401, detail="Invalid session")
        await _require_identity_nft(wallet)

    if not content:
        raise HTTPException(status_code=400, detail="Content is required")
    if len(content) > 500:
        raise HTTPException(status_code=400, detail="Content too long")

    parent_id_val: Optional[int] = None
    if parent_id and str(parent_id).strip():
        try:
            parent_id_val = int(parent_id)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid parent_id")

    image_url: Optional[str] = None
    image_key: Optional[str] = None

    if image and image.filename:
        content_type = image.content_type or ""
        if content_type not in {"image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp"}:
            raise HTTPException(
                status_code=400,
                detail="Invalid image type. Use JPEG, PNG, GIF or WebP",
            )
        if content_type == "image/jpg":
            content_type = "image/jpeg"

        image_bytes = await image.read()
        if len(image_bytes) > 5 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Image too large. Max 5MB")

        vision_result = await check_image_safe(image_bytes)
        if not vision_result["safe"]:
            raise HTTPException(
                status_code=400,
                detail=f"Image rejected: {vision_result['reason']}",
            )

        try:
            upload_res = await upload_image(
                file_bytes=image_bytes,
                content_type=content_type,
                wallet=wallet,
            )
            image_url = upload_res["url"]
            image_key = upload_res["key"]
        except Exception as e:
            logger.warning("post_image_upload_failed", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to upload image")

    conn = await get_conn()
    try:
        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1", wallet
        )
        score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        tier = get_score_tier(score)

        if tier == "BLOCKED":
            raise HTTPException(
                status_code=403,
                detail="Trust score too low to post. Score 30+ required for basic access.",
            )
        if tier == "READ_ONLY":
            raise HTTPException(
                status_code=403,
                detail="Score 40+ required to create posts.",
            )

        if tier == "BASIC":
            today_count = await conn.fetchval(
                """
                SELECT COUNT(*) FROM social_posts
                WHERE wallet = $1 AND created_at > NOW() - INTERVAL '24 hours'
                """,
                wallet,
            )
            if (today_count or 0) >= 3:
                raise HTTPException(
                    status_code=429,
                    detail="Daily post limit reached (3/day for your score tier). Reach score 50+ for unlimited posting.",
                )

        visibility = await check_post_visibility(wallet, conn)
        if not visibility["can_post"]:
            raise HTTPException(status_code=403, detail=visibility.get("reason") or "NOT_ALLOWED")

        user_stats = await conn.fetchrow(
            """
            SELECT
                ts.score AS trust_score,
                COALESCE(inft.wallet_age_days, 0) AS wallet_age_days,
                (SELECT COUNT(*) FROM social_posts sp WHERE sp.wallet = ts.wallet) AS post_count
            FROM trust_scores ts
            LEFT JOIN identity_nft inft ON inft.wallet = ts.wallet
            WHERE ts.wallet = $1
            """,
            wallet,
        )
        trust_score_val = float(user_stats["trust_score"] or 0) if user_stats else 0.0
        wallet_age_days_val = int(user_stats["wallet_age_days"] or 0) if user_stats else 0
        post_count_val = int(user_stats["post_count"] or 0) if user_stats else 0

        from backend_blockid.api_server.content_moderation import (
            check_content,
            apply_content_penalty,
            log_violation,
        )
        moderation = await check_content(
            content,
            trust_score=trust_score_val,
            wallet_age_days=wallet_age_days_val,
            post_count=post_count_val,
        )

        if moderation.get("downgraded"):
            logger.debug(
                "content_moderation_downgraded",
                wallet=wallet[:16],
                original_level=moderation["violation_level"] + 1,
                downgraded_to=moderation["violation_level"],
                context=moderation.get("context"),
            )

        if moderation["action"] == "REJECT":
            await apply_content_penalty(wallet, moderation["violation_level"], conn)
            await log_violation(wallet, content[:100], moderation["violation_level"], conn)
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "CONTENT_VIOLATION",
                    "level": moderation["violation_level"],
                    "message": "Post contains inappropriate content",
                    "violations": moderation["violations"],
                },
            )

        if moderation["action"] == "BLOCK":
            await apply_content_penalty(wallet, 4, conn)
            await log_violation(wallet, content[:100], 4, conn)
            raise HTTPException(
                status_code=403,
                detail={
                    "error": "ACCOUNT_BLOCKED",
                    "message": "Account permanently disabled for content violations",
                },
            )

        if moderation["action"] == "ALLOW_CENSORED":
            content = moderation["cleaned_text"]
            await apply_content_penalty(wallet, 1, conn)
            await log_violation(wallet, content[:100], 1, conn)

        # --- Link preview: extract URL and fetch OG metadata ---
        link_url = None
        link_title = None
        link_description = None
        link_image = None

        detected_url = extract_first_url(content)
        if detected_url:
            try:
                og = await fetch_og_metadata(detected_url)
                if og:
                    link_url = og.get("url")
                    link_title = og.get("title")
                    link_description = og.get("description")
                    link_image = og.get("image")
            except Exception as e:
                logger.debug("og_fetch_post_error", error=str(e))

        ts_row = await conn.fetchrow(
            "SELECT score AS trust_score, risk_level FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        trust_score = float(ts_row["trust_score"]) if ts_row and ts_row["trust_score"] is not None else None
        risk_level = (ts_row["risk_level"] or "").upper() if ts_row and ts_row["risk_level"] else None
        auto_hide = bool(visibility.get("auto_hide"))
        hide_reason = visibility.get("hide_reason")

        row = await conn.fetchrow(
            """
            INSERT INTO social_posts (
                wallet, handle, content, image_url, image_key,
                post_type, parent_id, is_hidden, hide_reason,
                trust_score, risk_level,
                link_url, link_title, link_description, link_image
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                $2, $3, $4,
                $5, $6, $7, $8,
                $9, $10,
                $11, $12, $13, $14
            )
            RETURNING id, wallet, handle, content, image_url, post_type,
                      trust_score, risk_level, is_hidden, created_at
            """,
            wallet,
            content,
            image_url,
            image_key,
            post_type,
            parent_id_val,
            auto_hide,
            hide_reason,
            trust_score,
            risk_level,
            link_url,
            link_title,
            link_description,
            link_image,
        )
        if parent_id_val:
            await conn.execute(
                "UPDATE social_posts SET reply_count = reply_count + 1 WHERE id = $1",
                parent_id_val,
            )
            parent_row = await conn.fetchrow(
                "SELECT wallet FROM social_posts WHERE id = $1", parent_id_val
            )
            if parent_row and parent_row["wallet"] != wallet:
                await _notify(conn, parent_row["wallet"], "REPLY", wallet, parent_id_val)

        return PostResponse(**dict(row))
    finally:
        await release_conn(conn)


@router.get("/feed/following/{wallet}")
async def get_following_feed(
    wallet: str,
    limit: int = Query(20, ge=1, le=50),
    before: Optional[datetime] = Query(None),
    include_activity: bool = Query(True),
):
    conn = await get_conn()
    try:
        before_clause = "AND p.created_at < $3" if before else ""
        params: List[Any] = [wallet, limit]
        if before:
            params.append(before)

        rows = await conn.fetch(
            f"""
            SELECT
                p.*,
                orig.wallet AS original_wallet,
                orig.handle AS original_handle,
                orig.content AS original_content,
                orig.trust_score AS original_trust_score,
                orig.created_at AS original_created_at,
                sp_orig.avatar_url AS original_avatar_url,
                sp_orig.avatar_type AS original_avatar_type,
                sp_orig.avatar_is_animated AS original_avatar_is_animated,
                COALESCE(sub.plan, 'free') AS plan,
                COALESCE(sub_orig.plan, 'free') AS original_plan,
                sp_prof.avatar_url,
                sp_prof.avatar_type,
                sp_prof.avatar_is_animated
            FROM social_posts p
            JOIN social_follows f
              ON f.following_wallet = p.wallet
            LEFT JOIN social_posts orig
              ON orig.id = p.repost_of
            LEFT JOIN social_profiles sp_orig
              ON sp_orig.wallet = orig.wallet
            LEFT JOIN social_profiles sp_prof
              ON sp_prof.wallet = p.wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = p.wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub_orig ON sub_orig.user_id = orig.wallet
            WHERE f.follower_wallet = $1
              AND p.is_hidden = FALSE
              AND p.parent_id IS NULL
              {before_clause}
            ORDER BY p.created_at DESC
            LIMIT $2
            """,
            *params,
        )

        # Fetch top 1 reply per post (single query, not N+1)
        post_ids = [row["id"] for row in rows]
        top_replies_map: Dict[int, Dict[str, Any]] = {}

        if post_ids:
            placeholders = ", ".join(f"${i + 1}" for i in range(len(post_ids)))
            top_reply_rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (parent_id)
                    p.id,
                    p.parent_id,
                    p.wallet,
                    p.handle,
                    p.content,
                    p.created_at,
                    p.like_count,
                    p.reply_count,
                    p.repost_count,
                    COALESCE(sub.plan, 'free') AS plan
                FROM social_posts p
                LEFT JOIN LATERAL (
                    SELECT DISTINCT ON (user_id) user_id, plan
                    FROM subscriptions
                    WHERE user_id = p.wallet
                      AND status = 'active'
                    ORDER BY user_id, created_at DESC NULLS LAST
                ) sub ON true
                WHERE p.parent_id IN ({placeholders})
                  AND p.is_hidden = FALSE
                ORDER BY parent_id, p.created_at ASC
                """,
                *post_ids,
            )
            for r in top_reply_rows:
                top_replies_map[r["parent_id"]] = dict(r)

        posts = []
        for r in rows:
            post = dict(r)
            original_wallet = post.get("original_wallet")

            if post.get("is_repost") and post.get("repost_of") and original_wallet:
                post["original_post"] = {
                    "wallet": post.pop("original_wallet", None),
                    "handle": post.pop("original_handle", None),
                    "content": post.pop("original_content", None),
                    "trust_score": post.pop("original_trust_score", None),
                    "created_at": post.pop("original_created_at", None),
                    "avatar_url": post.pop("original_avatar_url", None),
                    "avatar_type": post.pop("original_avatar_type", None),
                    "avatar_is_animated": post.pop("original_avatar_is_animated", None),
                    "plan": post.pop("original_plan", "free"),
                }
            else:
                post.pop("original_wallet", None)
                post.pop("original_handle", None)
                post.pop("original_content", None)
                post.pop("original_trust_score", None)
                post.pop("original_created_at", None)
                post.pop("original_avatar_url", None)
                post.pop("original_avatar_type", None)
                post.pop("original_avatar_is_animated", None)
                post.pop("original_plan", None)
                post["original_post"] = None

            post["top_reply"] = top_replies_map.get(post.get("id"))
            posts.append(post)

        next_cursor = posts[-1]["created_at"].isoformat() if posts else None

        activity: List[Dict[str, Any]] = []
        if include_activity:
            wallets = list({p["wallet"] for p in posts})
            activity = await get_activity_feed(wallets, limit=limit)

        return {"posts": posts, "activity": activity, "next_cursor": next_cursor}
    finally:
        await release_conn(conn)


@router.get("/feed/explore")
async def get_explore_feed(
    limit: int = Query(20, ge=1, le=50),
    min_trust_score: float = Query(40.0),
    before: Optional[datetime] = Query(None),
):
    try:
        conn = await get_conn()
        try:
            before_clause = "AND p.created_at < $3" if before else ""
            params: List[Any] = [min_trust_score, limit]
            if before:
                params.append(before)

            rows = await conn.fetch(
                f"""
                SELECT
                    p.*,
                    COALESCE(ts.score, p.trust_score) AS trust_score,
                    orig.wallet AS original_wallet,
                    orig.handle AS original_handle,
                    orig.content AS original_content,
                    COALESCE(ts_orig.score, orig.trust_score) AS original_trust_score,
                    orig.created_at AS original_created_at,
                    sp_orig.avatar_url AS original_avatar_url,
                    sp_orig.avatar_type AS original_avatar_type,
                    sp_orig.avatar_is_animated AS original_avatar_is_animated,
                    COALESCE(sub.plan, 'free') AS plan,
                    COALESCE(sub_orig.plan, 'free') AS original_plan,
                    sp_prof.avatar_url,
                    sp_prof.avatar_type,
                    sp_prof.avatar_is_animated
                FROM social_posts p
                LEFT JOIN social_posts orig
                  ON orig.id = p.repost_of
                LEFT JOIN social_profiles sp_orig
                  ON sp_orig.wallet = orig.wallet
                LEFT JOIN social_profiles sp_prof
                  ON sp_prof.wallet = p.wallet
                LEFT JOIN user_privacy_settings ups
                  ON ups.wallet = p.wallet
                LEFT JOIN trust_scores ts
                  ON ts.wallet = p.wallet
                LEFT JOIN trust_scores ts_orig
                  ON ts_orig.wallet = orig.wallet
                LEFT JOIN (
                    SELECT DISTINCT ON (user_id) user_id, plan
                    FROM subscriptions
                    WHERE status = 'active'
                    ORDER BY user_id, created_at DESC NULLS LAST
                ) sub ON sub.user_id = p.wallet
                LEFT JOIN (
                    SELECT DISTINCT ON (user_id) user_id, plan
                    FROM subscriptions
                    WHERE status = 'active'
                    ORDER BY user_id, created_at DESC NULLS LAST
                ) sub_orig ON sub_orig.user_id = orig.wallet
                WHERE p.post_type = 'PUBLIC'
                  AND p.is_hidden = FALSE
                  AND p.parent_id IS NULL
                  AND (
                    p.is_repost = TRUE
                    OR COALESCE(ts.score, p.trust_score, 0) >= $1
                  )
                  AND (ups.posts_visibility = 'PUBLIC' OR ups.posts_visibility IS NULL)
                  {before_clause}
                ORDER BY COALESCE(ts.score, p.trust_score, 0) DESC, p.created_at DESC
                LIMIT $2
                """,
                *params,
            )

            posts = []
            for r in rows:
                post = dict(r)
                original_wallet = post.get("original_wallet")

                if post.get("is_repost") and post.get("repost_of") and original_wallet:
                    post["original_post"] = {
                        "wallet": post.pop("original_wallet", None),
                        "handle": post.pop("original_handle", None),
                        "content": post.pop("original_content", None),
                        "trust_score": post.pop("original_trust_score", None),
                        "created_at": post.pop("original_created_at", None),
                        "avatar_url": post.pop("original_avatar_url", None),
                        "avatar_type": post.pop("original_avatar_type", None),
                        "avatar_is_animated": post.pop("original_avatar_is_animated", None),
                        "plan": post.pop("original_plan", "free"),
                    }
                else:
                    post.pop("original_wallet", None)
                    post.pop("original_handle", None)
                    post.pop("original_content", None)
                    post.pop("original_trust_score", None)
                    post.pop("original_created_at", None)
                    post.pop("original_avatar_url", None)
                    post.pop("original_avatar_type", None)
                    post.pop("original_avatar_is_animated", None)
                    post.pop("original_plan", None)
                    post["original_post"] = None

                posts.append(post)

            next_cursor = posts[-1]["created_at"].isoformat() if posts else None
            return {"posts": posts, "next_cursor": next_cursor}
        finally:
            await release_conn(conn)
    except Exception as e:
        logger.error(
            "explore_feed_error",
            error=str(e),
            traceback=traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/follow")
async def follow_wallet(body: Dict[str, Any]):
    follower_wallet = body.get("follower_wallet", "").strip()
    following_wallet = body.get("following_wallet", "").strip()
    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    if not follower_wallet or not following_wallet:
        raise HTTPException(status_code=400, detail="Invalid follower/following wallet")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != follower_wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(follower_wallet)

    conn = await get_conn()
    try:
        # Score gate: require score >= 30 to follow
        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1", follower_wallet
        )
        score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        if score < 30:
            raise HTTPException(
                status_code=403,
                detail="Trust score too low. Score 30+ required to follow.",
            )

        # Check if target wallet allows follows
        target_settings = await _ensure_privacy_settings(following_wallet, conn)
        if target_settings.get("allow_follows") == "NONE":
            raise HTTPException(
                status_code=403,
                detail="This wallet does not accept follows",
            )

        await conn.execute(
            """
            INSERT INTO social_follows (follower_wallet, following_wallet)
            VALUES ($1, $2)
            ON CONFLICT (follower_wallet, following_wallet) DO NOTHING
            """,
            follower_wallet,
            following_wallet,
        )
        await _notify(conn, following_wallet, "FOLLOW", follower_wallet, None)
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1",
            following_wallet,
        )
        ts_row = await conn.fetchrow(
            "SELECT score AS trust_score FROM trust_scores WHERE wallet = $1",
            following_wallet,
        )
        return {
            "success": True,
            "follower": follower_wallet,
            "following": following_wallet,
            "following_handle": handle_row["handle"] if handle_row else None,
            "following_trust_score": float(ts_row["trust_score"]) if ts_row and ts_row["trust_score"] is not None else None,
        }
    finally:
        await release_conn(conn)


@router.delete("/follow")
async def unfollow_wallet(body: Dict[str, Any]):
    follower_wallet = body.get("follower_wallet", "").strip()
    following_wallet = body.get("following_wallet", "").strip()

    if not follower_wallet or not following_wallet:
        raise HTTPException(status_code=400, detail="Invalid follower/following wallet")

    conn = await get_conn()
    try:
        await conn.execute(
            "DELETE FROM social_follows WHERE follower_wallet = $1 AND following_wallet = $2",
            follower_wallet,
            following_wallet,
        )
        return {"success": True}
    finally:
        await release_conn(conn)


@router.post("/like")
async def like_post(body: Dict[str, Any]):
    wallet = body.get("wallet", "").strip()
    post_id = int(body.get("post_id", 0))
    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="Invalid wallet/post_id")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(wallet)

    conn = await get_conn()
    try:
        # Score gate: require score >= 40 to like
        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1", wallet
        )
        score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        if score < 40:
            raise HTTPException(
                status_code=403,
                detail="Score 40+ required to like posts.",
            )

        inserted = await conn.fetchrow(
            """
            INSERT INTO social_likes (post_id, wallet)
            VALUES ($1, $2)
            ON CONFLICT (post_id, wallet) DO NOTHING
            RETURNING id
            """,
            post_id,
            wallet,
        )
        if inserted:
            await conn.execute(
                "UPDATE social_posts SET like_count = like_count + 1 WHERE id = $1",
                post_id,
            )
        row = await conn.fetchrow(
            "SELECT like_count, wallet FROM social_posts WHERE id = $1",
            post_id,
        )
        if inserted and row and row["wallet"] != wallet:
            await _notify(conn, row["wallet"], "LIKE", wallet, post_id)
        return {"success": True, "post_id": post_id, "like_count": row["like_count"] if row else None}
    finally:
        await release_conn(conn)


@router.delete("/like")
async def unlike_post(body: Dict[str, Any]):
    wallet = body.get("wallet", "").strip()
    post_id = int(body.get("post_id", 0))

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="Invalid wallet/post_id")

    conn = await get_conn()
    try:
        await conn.execute(
            "DELETE FROM social_likes WHERE post_id = $1 AND wallet = $2",
            post_id,
            wallet,
        )
        row = await conn.fetchrow(
            """
            UPDATE social_posts
            SET like_count = GREATEST(like_count - 1, 0)
            WHERE id = $1
            RETURNING like_count
            """,
            post_id,
        )
        return {"success": True, "post_id": post_id, "like_count": row["like_count"] if row else None}
    finally:
        await release_conn(conn)


@router.post("/repost")
async def repost_post(body: Dict[str, Any]):
    wallet = (body.get("wallet") or "").strip()
    post_id = body.get("post_id")
    quote_content = (body.get("quote_content") or "").strip()
    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="wallet and post_id required")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(wallet)

    conn = await get_conn()
    try:
        # Score gate: require >= 40
        ts = await conn.fetchrow(
            "SELECT score FROM trust_scores WHERE wallet = $1", wallet
        )
        score = float(ts["score"]) if ts and ts.get("score") is not None else 0.0
        tier = get_score_tier(score)
        if tier in ("BLOCKED", "READ_ONLY"):
            raise HTTPException(
                status_code=403,
                detail="Score 40+ required to repost.",
            )

        # Rate limit BASIC tier (40-49): 3 posts/day
        if tier == "BASIC":
            today_count = await conn.fetchval(
                """SELECT COUNT(*) FROM social_posts
                   WHERE wallet = $1
                   AND created_at > NOW() - INTERVAL '24 hours'""",
                wallet,
            )
            if (today_count or 0) >= 3:
                raise HTTPException(
                    status_code=429,
                    detail="Daily limit reached. Score 50+ for unlimited.",
                )

        # Get original post
        original = await conn.fetchrow(
            "SELECT id, wallet, content, post_type "
            "FROM social_posts WHERE id = $1 AND is_hidden = FALSE",
            post_id,
        )
        if not original:
            raise HTTPException(status_code=404, detail="Post not found")

        # Cannot repost own post
        if original["wallet"] == wallet:
            raise HTTPException(
                status_code=400,
                detail="Cannot repost your own post",
            )

        # Check already reposted
        already = await conn.fetchval(
            """SELECT id FROM social_posts
               WHERE wallet = $1 AND repost_of = $2
               AND is_repost = TRUE""",
            wallet,
            post_id,
        )
        if already:
            raise HTTPException(
                status_code=400,
                detail="Already reposted this post",
            )

        # Get handle
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry "
            "WHERE owner_wallet = $1 LIMIT 1",
            wallet,
        )
        handle = handle_row["handle"] if handle_row else None

        # Simple repost uses original content; quote repost uses quote_content
        is_quote = bool(quote_content)
        content = quote_content if is_quote else original["content"]

        # Create new post as repost
        new_post = await conn.fetchrow(
            """INSERT INTO social_posts
               (wallet, handle, content, post_type,
                is_repost, repost_of, quote_content,
                like_count, reply_count, repost_count)
               VALUES ($1, $2, $3, $4, TRUE, $5, $6, 0, 0, 0)
               RETURNING id, created_at""",
            wallet,
            handle,
            content,
            original["post_type"],
            post_id,
            quote_content if is_quote else None,
        )

        # Increment repost_count on original post
        await conn.execute(
            "UPDATE social_posts SET repost_count = "
            "COALESCE(repost_count, 0) + 1 WHERE id = $1",
            post_id,
        )

        # Notify original poster
        await _notify(conn, original["wallet"], "REPOST", wallet, post_id)

        return {
            "success": True,
            "post_id": new_post["id"],
            "is_quote": is_quote,
            "original_post_id": post_id,
            "wallet": wallet,
            "message": "Quote reposted" if is_quote else "Reposted",
        }
    finally:
        await release_conn(conn)


@router.delete("/repost")
async def unrepost_post(body: Dict[str, Any]):
    """Remove a repost. Deletes the repost record and decrements repost_count on original."""
    wallet = (body.get("wallet") or "").strip()
    post_id = body.get("post_id")
    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="wallet and post_id required")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(wallet)

    conn = await get_conn()
    try:
        repost_row = await conn.fetchrow(
            "SELECT id FROM social_posts WHERE wallet = $1 AND repost_of = $2 AND is_repost = TRUE",
            wallet,
            int(post_id),
        )
        if not repost_row:
            raise HTTPException(status_code=404, detail="Repost not found")

        repost_id = repost_row["id"]

        await conn.execute(
            """
            WITH RECURSIVE descendants AS (
                SELECT id FROM social_posts WHERE parent_id = $1
                UNION ALL
                SELECT sp.id
                FROM social_posts sp
                JOIN descendants d ON sp.parent_id = d.id
            )
            DELETE FROM social_likes WHERE post_id IN (SELECT id FROM descendants)
            """,
            repost_id,
        )
        await conn.execute("DELETE FROM social_likes WHERE post_id = $1", repost_id)
        await conn.execute(
            """
            WITH RECURSIVE descendants AS (
                SELECT id FROM social_posts WHERE parent_id = $1
                UNION ALL
                SELECT sp.id
                FROM social_posts sp
                JOIN descendants d ON sp.parent_id = d.id
            )
            DELETE FROM post_bookmarks WHERE post_id IN (SELECT id FROM descendants)
            """,
            repost_id,
        )
        await conn.execute("DELETE FROM post_bookmarks WHERE post_id = $1", repost_id)
        await conn.execute(
            """
            WITH RECURSIVE descendants AS (
                SELECT id FROM social_posts WHERE parent_id = $1
                UNION ALL
                SELECT sp.id
                FROM social_posts sp
                JOIN descendants d ON sp.parent_id = d.id
            )
            DELETE FROM social_posts WHERE id IN (SELECT id FROM descendants)
            """,
            repost_id,
        )
        await conn.execute(
            "DELETE FROM social_posts WHERE id = $1 AND wallet = $2 AND is_repost = TRUE",
            repost_id,
            wallet,
        )
        await conn.execute(
            "UPDATE social_posts SET repost_count = GREATEST(repost_count - 1, 0) WHERE id = $1",
            int(post_id),
        )

        return {"success": True, "message": "Repost removed"}
    finally:
        await release_conn(conn)


@router.post("/flag")
async def flag_post(body: Dict[str, Any]):
    wallet = body.get("wallet", "").strip()
    post_id = int(body.get("post_id", 0))
    reason = body.get("reason", "") or ""
    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="Invalid wallet/post_id")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(wallet)

    conn = await get_conn()
    try:
        result = await process_flag(post_id, wallet, reason, conn)
        return result
    finally:
        await release_conn(conn)


@router.post("/report")
async def report_post(body: Dict[str, Any]):
    wallet = (body.get("wallet") or "").strip()
    post_id = body.get("post_id")
    reason = (body.get("reason") or "OTHER").strip().upper()
    details = (body.get("details") or "").strip()

    valid_reasons = {"SPAM", "HARASSMENT", "MISINFORMATION", "SCAM", "OTHER"}
    if reason not in valid_reasons:
        reason = "OTHER"

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="wallet and post_id required")

    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")
        await _require_identity_nft(wallet)

    conn = await get_conn()
    try:
        post = await conn.fetchrow(
            "SELECT id, wallet FROM social_posts WHERE id = $1", post_id
        )
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")

        if post["wallet"] == wallet:
            raise HTTPException(
                status_code=400,
                detail="Cannot report your own post",
            )

        inserted = await conn.fetchrow(
            """INSERT INTO social_reports
               (post_id, reporter_wallet, reason, details)
               VALUES ($1, $2, $3, $4)
               ON CONFLICT (post_id, reporter_wallet) DO NOTHING
               RETURNING id""",
            post_id,
            wallet,
            reason,
            details or None,
        )

        return {
            "success": True,
            "reported": inserted is not None,
            "post_id": post_id,
            "message": "Report submitted" if inserted else "Already reported",
        }
    finally:
        await release_conn(conn)


@router.post("/endorse")
async def endorse_wallet(body: Dict[str, Any]):
    """Endorse a wallet. Min trust score 50 to endorse. One endorsement per pair. Applies +5 trust boost (capped at 97)."""
    from_wallet = (body.get("from_wallet") or "").strip()
    to_wallet = (body.get("to_wallet") or "").strip()
    message = (body.get("message") or "").strip()
    signature = (body.get("signature") or "").strip()
    signed_message = (body.get("signed_message") or "").strip()

    if not from_wallet or not to_wallet:
        raise HTTPException(status_code=400, detail="Invalid from_wallet or to_wallet")

    # High-value: individual signature required. Message: "BlockID Endorse:{to_wallet}:{from_wallet}"
    expected_msg = f"BlockID Endorse:{to_wallet}:{from_wallet}"
    if not signed_message or signed_message.strip() != expected_msg:
        raise HTTPException(400, detail="signed_message must be 'BlockID Endorse:{to_wallet}:{from_wallet}'")
    verify_or_raise(from_wallet, signed_message.strip(), signature, detail="Invalid endorse signature")

    await _require_identity_nft(from_wallet)

    if from_wallet == to_wallet:
        raise HTTPException(status_code=400, detail="Cannot endorse yourself")

    conn = await get_conn()
    try:
        ts_row = await conn.fetchrow(
            "SELECT score AS trust_score FROM trust_scores WHERE wallet = $1",
            from_wallet,
        )
        score = float(ts_row["trust_score"]) if ts_row and ts_row["trust_score"] is not None else 0.0
        if score < SOCIAL_MIN_SCORE_TO_ENDORSE:
            raise HTTPException(
                status_code=403,
                detail=f"Minimum trust score {SOCIAL_MIN_SCORE_TO_ENDORSE} required to endorse",
            )

        inserted = await conn.fetchrow(
            """
            INSERT INTO social_endorsements (from_wallet, to_wallet, message, is_active)
            VALUES ($1, $2, $3, TRUE)
            ON CONFLICT (from_wallet, to_wallet) DO NOTHING
            RETURNING id
            """,
            from_wallet,
            to_wallet,
            message or None,
        )

        if inserted:
            # Fetch score_before for history
            row_before = await conn.fetchrow(
                "SELECT score, risk_level FROM trust_scores WHERE wallet = $1",
                to_wallet,
            )
            current_score = float(row_before["score"]) if row_before and row_before["score"] is not None else 0.0
            score_before = current_score
            risk_level_before = row_before["risk_level"] if row_before and row_before["risk_level"] is not None else None

            # Compute new_score (match UPDATE expression)
            new_score = min(SCORE_CAP, max(0.0, current_score + ENDORSE_TRUST_BOOST))

            # Score history hook (SOCIAL_ACTION) — non-fatal, BEFORE UPDATE
            try:
                total_endorsements_row = await conn.fetchrow(
                    "SELECT COUNT(*) AS c FROM social_endorsements WHERE to_wallet = $1 AND is_active = TRUE",
                    to_wallet,
                )
                total_endorsements = int(
                    total_endorsements_row["c"] if total_endorsements_row and total_endorsements_row["c"] is not None else 0
                )

                logger.debug(
                    "score_history_social_hook",
                    wallet=to_wallet[:16],
                    endorser=from_wallet[:16],
                    score_before=score_before,
                )
                await log_score_change(
                    wallet=to_wallet,
                    score_before=score_before,
                    score_after=new_score,
                    change_category="SOCIAL_ACTION",
                    triggered_by="social_engine",
                    reason_codes=["SOCIAL_ENDORSEMENT"],
                    confidence=score / 100.0,
                    risk_level=str(risk_level_before) if risk_level_before is not None else None,
                    metadata={
                        "endorser_wallet": from_wallet,
                        "endorser_score": score,
                        "endorse_count": total_endorsements,
                    },
                )
            except Exception:  # pragma: no cover - best-effort
                pass

            await conn.execute(
                """
                UPDATE trust_scores
                SET score = LEAST($1, GREATEST(0, COALESCE(score, 0) + $2)),
                    updated_at = NOW()
                WHERE wallet = $3
                """,
                SCORE_CAP,
                ENDORSE_TRUST_BOOST,
                to_wallet,
            )

            await insert_wallet_reason(
                wallet=to_wallet,
                reason_code="SOCIAL_ENDORSEMENT",
                weight=ENDORSE_TRUST_BOOST,
                confidence=1.0,
            )
            await _notify(conn, to_wallet, "ENDORSE", from_wallet, None)

        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1",
            to_wallet,
        )
        return {
            "success": True,
            "from_wallet": from_wallet,
            "to_wallet": to_wallet,
            "to_handle": handle_row["handle"] if handle_row else None,
            "trust_boost": ENDORSE_TRUST_BOOST if inserted else 0,
            "message": "Endorsement recorded" if inserted else "Already endorsed",
        }
    finally:
        await release_conn(conn)


@router.get("/followers/{wallet}")
async def get_followers(wallet: str, viewer_wallet: Optional[str] = Query(None)):
    """
    Get list of wallets that follow this wallet.
    Returns: { followers: [...], count: int }
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT
                sf.follower_wallet as wallet,
                hr.handle,
                (SELECT score FROM trust_scores
                 WHERE trust_scores.wallet = sf.follower_wallet
                 ORDER BY computed_at DESC NULLS LAST LIMIT 1) as trust_score,
                COALESCE(sub.plan, 'free') as plan
            FROM social_follows sf
            LEFT JOIN handle_registry hr
                ON hr.owner_wallet = sf.follower_wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = sf.follower_wallet
            WHERE sf.following_wallet = $1
            ORDER BY sf.created_at DESC
            LIMIT 100
            """,
            wallet,
        )
        followers = []
        for r in rows:
            f = dict(r)
            followers.append(f)
        if viewer_wallet:
            for f in followers:
                w = f.get("wallet", "")
                # viewer already follows this person?
                viewer_follows = await conn.fetchval(
                    "SELECT 1 FROM social_follows WHERE follower_wallet = $1 AND following_wallet = $2",
                    viewer_wallet, w,
                )
                # this person follows viewer?
                they_follow_viewer = await conn.fetchval(
                    "SELECT 1 FROM social_follows WHERE follower_wallet = $1 AND following_wallet = $2",
                    w, viewer_wallet,
                )
                f["viewer_follows"] = viewer_follows is not None
                f["follows_viewer"] = they_follow_viewer is not None
        else:
            for f in followers:
                f["viewer_follows"] = None
                f["follows_viewer"] = None
        return {
            "wallet": wallet,
            "followers": followers,
            "count": len(followers),
        }
    finally:
        await release_conn(conn)


@router.get("/following/{wallet}")
async def get_following(wallet: str, viewer_wallet: Optional[str] = Query(None)):
    """
    Get list of wallets this wallet follows.
    Returns: { following: [...], count: int }
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT
                sf.following_wallet as wallet,
                hr.handle,
                (SELECT score FROM trust_scores
                 WHERE trust_scores.wallet = sf.following_wallet
                 ORDER BY computed_at DESC NULLS LAST LIMIT 1) as trust_score,
                COALESCE(sub.plan, 'free') as plan
            FROM social_follows sf
            LEFT JOIN handle_registry hr
                ON hr.owner_wallet = sf.following_wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = sf.following_wallet
            WHERE sf.follower_wallet = $1
            ORDER BY sf.created_at DESC
            LIMIT 100
            """,
            wallet,
        )
        following = []
        for r in rows:
            f = dict(r)
            following.append(f)
        if viewer_wallet:
            for f in following:
                w = f.get("wallet", "")
                # viewer already follows this person?
                viewer_follows = await conn.fetchval(
                    "SELECT 1 FROM social_follows WHERE follower_wallet = $1 AND following_wallet = $2",
                    viewer_wallet, w,
                )
                # this person follows viewer?
                they_follow_viewer = await conn.fetchval(
                    "SELECT 1 FROM social_follows WHERE follower_wallet = $1 AND following_wallet = $2",
                    w, viewer_wallet,
                )
                f["viewer_follows"] = viewer_follows is not None
                f["follows_viewer"] = they_follow_viewer is not None
        else:
            for f in following:
                f["viewer_follows"] = None
                f["follows_viewer"] = None
        return {
            "wallet": wallet,
            "following": following,
            "count": len(following),
        }
    finally:
        await release_conn(conn)


@router.get("/profile/{wallet}")
async def get_profile(
    wallet: str,
    viewer_wallet: Optional[str] = Query(None),
):
    """Get full social profile for a wallet."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    conn = await get_conn()
    try:
        ts = await conn.fetchrow(
            "SELECT score AS trust_score, risk_level FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        handle_row = await conn.fetchrow(
            "SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1",
            wallet,
        )
        id_row = await conn.fetchrow(
            "SELECT mint_address, minted_at FROM identity_nft WHERE wallet = $1 AND (mint_status = 'MINTED' OR mint_address IS NOT NULL)",
            wallet,
        )
        fc = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM social_follows WHERE following_wallet = $1",
            wallet,
        )
        fg = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM social_follows WHERE follower_wallet = $1",
            wallet,
        )
        pc = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM social_posts WHERE wallet = $1 AND is_hidden = FALSE",
            wallet,
        )
        ec = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM social_endorsements WHERE to_wallet = $1 AND is_active = TRUE",
            wallet,
        )
        posts_rows = await conn.fetch(
            """
            SELECT sp.id, sp.wallet, sp.handle, sp.content, sp.image_url, sp.post_type,
                   sp.reply_count, sp.like_count, sp.trust_score, sp.risk_level,
                   sp.link_url, sp.link_title, sp.link_description, sp.link_image,
                   sp.is_hidden, sp.created_at,
                   COALESCE(sub.plan, 'free') AS plan
            FROM social_posts sp
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = sp.wallet
            WHERE sp.wallet = $1 AND sp.is_hidden = FALSE AND sp.post_type = 'PUBLIC'
            ORDER BY sp.created_at DESC
            LIMIT 10
            """,
            wallet,
        )
        is_following = False
        if viewer_wallet:
            fol = await conn.fetchval(
                "SELECT 1 FROM social_follows WHERE follower_wallet = $1 AND following_wallet = $2",
                viewer_wallet,
                wallet,
            )
            is_following = fol is not None
        reason_rows = await conn.fetch(
            "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 AND weight > 0",
            wallet,
        )
        badges = [r["reason_code"] for r in reason_rows if r["reason_code"] not in NEGATIVE_CODES] if reason_rows else []
        sp_row = await conn.fetchrow(
            """
            SELECT avatar_type, avatar_url, avatar_nft_mint, avatar_nft_name,
                   avatar_nft_collection, avatar_is_animated,
                   banner_type, banner_url, banner_is_animated,
                   display_name, display_name_source, bio, website, location
            FROM social_profiles WHERE wallet = $1
            """,
            wallet,
        )

        profile_row = await conn.fetchrow(
            "SELECT displayed_badges FROM social_profiles WHERE wallet = $1",
            wallet,
        )
        displayed_badges = (
            list(profile_row["displayed_badges"])
            if profile_row and profile_row["displayed_badges"]
            else []
        )

        plan = "free"
        try:
            sub = await conn.fetchrow(
                """SELECT plan FROM subscriptions
                   WHERE user_id = $1 AND status = 'active'
                   ORDER BY created_at DESC NULLS LAST LIMIT 1""",
                wallet,
            )
            if sub:
                plan = (sub["plan"] or "free").lower()
        except Exception:
            pass

        return {
            "wallet": wallet,
            "handle": handle_row["handle"] if handle_row else None,
            "plan": plan,
            "trust_score": float(ts["trust_score"]) if ts and ts["trust_score"] is not None else None,
            "risk_level": ts["risk_level"] if ts else None,
            "identity_nft": id_row["mint_address"] if id_row else None,
            "follower_count": fc["c"] if fc else 0,
            "following_count": fg["c"] if fg else 0,
            "post_count": pc["c"] if pc else 0,
            "endorsement_count": ec["c"] if ec else 0,
            "posts": [dict(r) for r in posts_rows],
            "is_following": is_following,
            "badges": badges,
            "avatar_type": sp_row["avatar_type"] if sp_row else None,
            "avatar_url": sp_row["avatar_url"] if sp_row else None,
            "avatar_nft_mint": sp_row["avatar_nft_mint"] if sp_row else None,
            "avatar_nft_name": sp_row["avatar_nft_name"] if sp_row else None,
            "avatar_nft_collection": sp_row["avatar_nft_collection"] if sp_row else None,
            "avatar_is_animated": sp_row["avatar_is_animated"] if sp_row else False,
            "banner_type": sp_row["banner_type"] if sp_row else None,
            "banner_url": sp_row["banner_url"] if sp_row else None,
            "banner_is_animated": sp_row["banner_is_animated"] if sp_row else False,
            "display_name": sp_row["display_name"] if sp_row else None,
            "display_name_source": sp_row["display_name_source"] if sp_row else "WALLET",
            "bio": sp_row["bio"] if sp_row else None,
            "website": sp_row["website"] if sp_row else None,
            "location": sp_row["location"] if sp_row else None,
            "displayed_badges": displayed_badges,
            "joined_at": id_row["minted_at"].isoformat() if id_row and id_row.get("minted_at") else None,
        }
    finally:
        await release_conn(conn)


@router.get("/badges/{wallet}")
async def get_badges(wallet: str):
    conn = await get_conn()
    try:
        reason_rows = await conn.fetch(
            "SELECT DISTINCT reason_code FROM wallet_reasons WHERE wallet = $1",
            wallet,
        )
        profile_row = await conn.fetchrow(
            "SELECT displayed_badges FROM social_profiles WHERE wallet = $1",
            wallet,
        )
        earned = [r["reason_code"] for r in reason_rows if r["reason_code"] not in NEGATIVE_CODES]
        displayed = list(profile_row["displayed_badges"]) if profile_row and profile_row["displayed_badges"] else []
        return {"earned": earned, "displayed": displayed}
    finally:
        await release_conn(conn)


@router.post("/badges/display")
async def set_displayed_badges(body: SetBadgesRequest):
    if len(body.badges) > 5:
        raise HTTPException(400, detail="Maximum 5 badges allowed")
    conn = await get_conn()
    try:
        reason_rows = await conn.fetch(
            "SELECT DISTINCT reason_code FROM wallet_reasons WHERE wallet = $1",
            body.wallet,
        )
        earned = {r["reason_code"] for r in reason_rows if r["reason_code"] not in NEGATIVE_CODES}
        for b in body.badges:
            if b not in earned:
                raise HTTPException(400, detail=f"Badge {b} not earned by this wallet")
        await conn.execute(
            "UPDATE social_profiles SET displayed_badges = $1 WHERE wallet = $2",
            body.badges,
            body.wallet,
        )
        return {"success": True, "displayed": body.badges}
    finally:
        await release_conn(conn)


@router.get("/subscription/{wallet}")
async def get_user_subscription(wallet: str):
    """Get B2C subscription info for a wallet. Plans: free (10/mo), explorer (100/mo), pro (unlimited)."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        plan = "free"
        scans_used = 0
        status_val = "free"

        # Check subscriptions table (user_id or wallet column depending on schema)
        try:
            sub = await conn.fetchrow(
                """
                SELECT COALESCE(tier, plan, 'free') as plan, status
                FROM subscriptions
                WHERE (user_id = $1 OR wallet = $1) AND (status = 'active' OR status IS NULL)
                ORDER BY created_at DESC NULLS LAST, updated_at DESC NULLS LAST
                LIMIT 1
                """,
                wallet,
            )
            if sub:
                plan = (sub["plan"] or "free").lower()
                status_val = (sub["status"] or "active").lower()
        except Exception:
            pass  # Table may not exist

        # Get scan usage this month (wallet_scan_usage: wallet, month, scan_count)
        try:
            current_month = datetime.now().strftime("%Y-%m")
            usage = await conn.fetchrow(
                """
                SELECT scan_count FROM wallet_scan_usage
                WHERE wallet = $1 AND month = $2
                """,
                wallet,
                current_month,
            )
            if usage:
                scans_used = int(usage["scan_count"] or 0)
        except Exception:
            pass  # Table may not exist

        return {
            "wallet": wallet,
            "plan": plan,
            "scans_used": scans_used,
            "wallet_scan_usage": scans_used,
            "status": status_val,
        }
    finally:
        await release_conn(conn)


@router.post("/subscription/pay")
async def pay_subscription(req: Request):
    body = await req.json()
    wallet = (body.get("wallet") or "").strip()
    plan = (body.get("plan") or "").strip().lower()  # "explorer" | "pro"
    period = (body.get("period") or "").strip().lower()  # "monthly" | "annual"
    tx_sig = (body.get("tx_signature") or "").strip()
    token = (body.get("token") or "").strip().upper()  # "USDC" | "SOL"

    if not all([wallet, plan, period, tx_sig, token]):
        raise HTTPException(status_code=400, detail="Missing required fields")
    if plan not in ("explorer", "pro"):
        raise HTTPException(status_code=400, detail="Invalid plan")
    if period not in ("monthly", "annual"):
        raise HTTPException(status_code=400, detail="Invalid period")
    if token not in ("USDC", "SOL"):
        raise HTTPException(status_code=400, detail="Invalid token")

    conn = await get_conn()
    try:
        # 1. Replay protection — check tx not already used
        existing = await conn.fetchrow(
            "SELECT id FROM subscriptions WHERE tx_signature = $1",
            tx_sig,
        )
        if existing:
            raise HTTPException(status_code=409, detail="Transaction already used")

        # 2. Verify tx on Solana via Helius
        helius_key = (os.getenv("HELIUS_API_KEY") or "").strip()
        if not helius_key:
            raise HTTPException(status_code=500, detail="Missing HELIUS_API_KEY")
        helius_url = f"https://mainnet.helius-rpc.com/?api-key={helius_key}"

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                helius_url,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        tx_sig,
                        {
                            "encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0,
                        },
                    ],
                },
            )

        if resp.status_code != 200:
            raise HTTPException(
                status_code=400,
                detail=f"Helius error status={resp.status_code}",
            )

        payload = resp.json()
        tx_data = payload.get("result")
        if not tx_data:
            raise HTTPException(status_code=400, detail="Transaction not found on chain")

        # 3. Check tx succeeded
        if tx_data.get("meta", {}).get("err") is not None:
            raise HTTPException(status_code=400, detail="Transaction failed on chain")

        # 4. Verify payment amount
        expected_usd = float(PLAN_PRICES[plan][period])
        verified = False

        if token == "USDC":
            # Check SPL token transfer
            instructions = (
                tx_data.get("transaction", {})
                .get("message", {})
                .get("instructions", [])
            )
            for ix in instructions:
                parsed = ix.get("parsed", {})
                info = parsed.get("info", {})
                if (
                    parsed.get("type") == "transferChecked"
                    and info.get("mint") == USDC_MINT
                    and info.get("destination")  # treasury ATA
                    and float(info.get("tokenAmount", {}).get("uiAmount", 0)) >= expected_usd * 0.99
                ):
                    verified = True
                    break

        elif token == "SOL":
            # Verify SOL transfer — fetch SOL price from Jupiter
            async with httpx.AsyncClient(timeout=10) as client:
                price_resp = await client.get(
                    "https://price.jup.ag/v6/price?ids=SOL&vsToken=USDC"
                )
            if price_resp.status_code != 200:
                raise HTTPException(status_code=500, detail="Could not fetch SOL price")

            sol_price = (
                price_resp.json()
                .get("data", {})
                .get("SOL", {})
                .get("price", 0)
            )
            sol_price = float(sol_price or 0)
            if not sol_price:
                raise HTTPException(status_code=500, detail="Could not fetch SOL price")

            expected_sol = expected_usd / sol_price
            min_sol = expected_sol * (1 - SOL_TOLERANCE)

            pre_bal = tx_data.get("meta", {}).get("preBalances", [])
            post_bal = tx_data.get("meta", {}).get("postBalances", [])
            accounts = tx_data.get("transaction", {}).get("message", {}).get(
                "accountKeys", []
            )

            for i, acc in enumerate(accounts):
                acc_key = acc if isinstance(acc, str) else acc.get("pubkey", "")
                if acc_key == TREASURY_WALLET:
                    if i < len(pre_bal) and i < len(post_bal):
                        received_sol = (post_bal[i] - pre_bal[i]) / 1e9
                        if received_sol >= min_sol:
                            verified = True
                    break

        if not verified:
            raise HTTPException(
                status_code=400, detail="Payment amount insufficient or not found"
            )

        # 5. Upsert subscription
        if period == "monthly":
            valid_until = datetime.utcnow() + timedelta(days=31)
        else:
            valid_until = datetime.utcnow() + timedelta(days=366)

        await conn.execute(
            """
            INSERT INTO subscriptions
              (user_id, plan, status, valid_until, tx_signature, created_at, updated_at)
            VALUES
              ($1, $2, 'active', $3, $4, NOW(), NOW())
            ON CONFLICT (user_id)
            DO UPDATE SET
              plan = EXCLUDED.plan,
              status = 'active',
              valid_until = EXCLUDED.valid_until,
              tx_signature = EXCLUDED.tx_signature,
              updated_at = NOW()
            """,
            wallet,
            plan,
            valid_until,
            tx_sig,
        )

        # 6. Reset scan usage for new billing period
        current_month = datetime.utcnow().strftime("%Y-%m")
        await conn.execute(
            """
            INSERT INTO wallet_scan_usage (wallet, month, scan_count, updated_at)
            VALUES ($1, $2, 0, NOW())
            ON CONFLICT (wallet, month)
            DO UPDATE SET
              scan_count = 0,
              updated_at = NOW()
            """,
            wallet,
            current_month,
        )

        return {
            "success": True,
            "plan": plan,
            "period": period,
            "valid_until": valid_until.isoformat(),
        }
    finally:
        await release_conn(conn)


@router.post("/openfort/encryption-session")
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


@router.get("/notifications/{wallet}")
async def get_notifications(
    wallet: str,
    mark_read: bool = Query(True),
):
    """Get notifications for wallet. Unread first. Optionally mark as read on fetch."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT n.id, n.type, n.from_wallet, n.post_id, n.is_read, n.created_at,
                   h.handle AS from_handle
            FROM social_notifications n
            LEFT JOIN handle_registry h ON h.owner_wallet = n.from_wallet
            WHERE n.wallet = $1
            ORDER BY n.is_read ASC, n.created_at DESC
            LIMIT 100
            """,
            wallet,
        )
        unread_count = sum(1 for r in rows if not r["is_read"])
        notifications = []
        for r in rows:
            msg = _notification_message(r["type"], r.get("from_handle"), r.get("post_id"))
            notifications.append({
                "id": r["id"],
                "type": r["type"],
                "from_wallet": r["from_wallet"],
                "from_handle": r["from_handle"],
                "post_id": r["post_id"],
                "message": msg,
                "is_read": r["is_read"],
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
            })
        if mark_read and unread_count > 0:
            await conn.execute(
                "UPDATE social_notifications SET is_read = TRUE WHERE wallet = $1",
                wallet,
            )
        return {"notifications": notifications, "unread_count": 0 if mark_read else unread_count}
    finally:
        await release_conn(conn)


def _notification_message(notif_type: str, from_handle: Optional[str], post_id: Optional[int]) -> str:
    handle = from_handle or "A wallet"
    if notif_type == "FOLLOW":
        return f"{handle} followed you"
    if notif_type == "LIKE":
        return f"{handle} liked your post"
    if notif_type == "REPLY":
        return f"{handle} replied to your post"
    if notif_type == "ENDORSE":
        return f"{handle} endorsed you"
    if notif_type == "FLAG_RESOLVED":
        return "Your flagged post was resolved"
    return str(notif_type)


@router.post("/appeal/{post_id}")
async def appeal_post(post_id: int):
    """Appeal a hidden post. Requires 3 trusted endorsers (score 80+). If qualifies, unhide and reset flag_weight."""
    conn = await get_conn()
    try:
        appeal_result = await check_appeal(post_id, conn)
        if not appeal_result["qualifies"]:
            return {
                "success": True,
                "post_id": post_id,
                "appeal_granted": False,
                "endorser_count": appeal_result["endorser_count"],
                "message": "Appeal denied: need 3 trusted endorsers (score 80+)",
            }
        await conn.execute(
            """
            UPDATE social_posts
            SET is_hidden = FALSE, hide_reason = NULL, flag_weight = 0
            WHERE id = $1
            """,
            post_id,
        )
        return {
            "success": True,
            "post_id": post_id,
            "appeal_granted": True,
            "endorser_count": appeal_result["endorser_count"],
            "message": "Post unhidden",
        }
    finally:
        await release_conn(conn)


@router.get("/post/{post_id}")
async def get_post(post_id: int):
    """Get single post with replies."""
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT sp.id, sp.wallet, sp.handle, sp.content, sp.image_url, sp.post_type,
                   sp.link_url, sp.link_title, sp.link_description, sp.link_image,
                   sp.parent_id, sp.reply_count, sp.like_count, sp.repost_count,
                   sp.is_hidden, sp.trust_score, sp.risk_level, sp.created_at,
                   COALESCE(sub.plan, 'free') AS plan,
                   sp_prof.avatar_url,
                   sp_prof.avatar_type,
                   sp_prof.avatar_is_animated
            FROM social_posts sp
            LEFT JOIN social_profiles sp_prof
              ON sp_prof.wallet = sp.wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = sp.wallet
            WHERE sp.id = $1
            """,
            post_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Post not found")
        replies_rows = await conn.fetch(
            """
            SELECT r.id, r.wallet, r.handle, r.content, r.image_url, r.post_type,
                   r.link_url, r.link_title, r.link_description, r.link_image,
                   r.parent_id, r.reply_count, r.like_count, r.is_hidden, r.created_at,
                   COALESCE(sub.plan, 'free') AS plan,
                   sp_prof.avatar_url,
                   sp_prof.avatar_type,
                   sp_prof.avatar_is_animated
            FROM social_posts r
            LEFT JOIN social_profiles sp_prof
              ON sp_prof.wallet = r.wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = r.wallet
            WHERE r.parent_id = $1 AND r.is_hidden = FALSE
            ORDER BY r.created_at ASC
            """,
            post_id,
        )
        return {"post": dict(row), "replies": [dict(r) for r in replies_rows]}
    finally:
        await release_conn(conn)


@router.delete("/post/{post_id}")
async def delete_post(
    post_id: int,
    wallet: str,
    session_token: str = "",
) -> dict:
    """Delete a post. Only the post owner can delete their own post."""
    conn = await get_conn()
    try:
        # Verify post exists and belongs to wallet
        post = await conn.fetchrow(
            "SELECT id, wallet FROM social_posts WHERE id = $1",
            post_id,
        )
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        if post["wallet"] != wallet:
            raise HTTPException(status_code=403, detail="Not authorized to delete this post")

        # Delete related data first
        await conn.execute("DELETE FROM social_likes WHERE post_id = $1", post_id)
        await conn.execute("DELETE FROM post_bookmarks WHERE post_id = $1", post_id)
        await conn.execute("DELETE FROM social_posts WHERE parent_id = $1", post_id)

        # Delete the post
        await conn.execute("DELETE FROM social_posts WHERE id = $1", post_id)

        logger.info("post_deleted", post_id=post_id, wallet=wallet[:16])
        return {"success": True, "message": "Post deleted successfully"}
    finally:
        await release_conn(conn)


@router.get("/posts/{wallet}")
async def get_wallet_posts(
    wallet: str,
    limit: int = Query(20, ge=1, le=50),
):
    """Get all public posts from a wallet."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")

    try:
        conn = await get_conn()
        try:
            rows = await conn.fetch(
                """
                SELECT
                    sp.id, sp.wallet, sp.handle, sp.content,
                    sp.image_url, sp.post_type, sp.parent_id,
                    sp.reply_count, sp.like_count, sp.repost_count,
                    sp.is_hidden, COALESCE(ts.score, sp.trust_score) AS trust_score, sp.risk_level,
                    sp.link_url, sp.link_title, sp.link_description, sp.link_image,
                    sp.created_at, sp.is_repost, sp.repost_of,
                    sp.quote_content,
                    orig.wallet AS original_wallet,
                    orig.handle AS original_handle,
                    orig.content AS original_content,
                    COALESCE(ts_orig.score, orig.trust_score) AS original_trust_score,
                    orig.created_at AS original_created_at,
                    COALESCE(sub.plan, 'free') AS plan,
                    COALESCE(sub_orig.plan, 'free') AS original_plan,
                    sp_prof.avatar_url,
                    sp_prof.avatar_type,
                    sp_prof.avatar_is_animated
                FROM social_posts sp
                LEFT JOIN social_posts orig
                    ON orig.id = sp.repost_of
                LEFT JOIN social_profiles sp_prof
                    ON sp_prof.wallet = sp.wallet
                LEFT JOIN trust_scores ts
                    ON ts.wallet = sp.wallet
                LEFT JOIN trust_scores ts_orig
                    ON ts_orig.wallet = orig.wallet
                LEFT JOIN (
                    SELECT DISTINCT ON (user_id) user_id, plan
                    FROM subscriptions
                    WHERE status = 'active'
                    ORDER BY user_id, created_at DESC NULLS LAST
                ) sub ON sub.user_id = sp.wallet
                LEFT JOIN (
                    SELECT DISTINCT ON (user_id) user_id, plan
                    FROM subscriptions
                    WHERE status = 'active'
                    ORDER BY user_id, created_at DESC NULLS LAST
                ) sub_orig ON sub_orig.user_id = orig.wallet
                WHERE sp.wallet = $1 AND sp.is_hidden = FALSE
                    AND sp.parent_id IS NULL
                ORDER BY sp.created_at DESC
                LIMIT $2
                """,
                wallet,
                limit,
            )

            posts = []
            for r in rows:
                post = dict(r)
                original_wallet = post.get("original_wallet")

                if post.get("is_repost") and post.get("repost_of") and original_wallet:
                    post["original_post"] = {
                        "wallet": post.pop("original_wallet", None),
                        "handle": post.pop("original_handle", None),
                        "content": post.pop("original_content", None),
                        "trust_score": post.pop("original_trust_score", None),
                        "created_at": post.pop("original_created_at", None),
                        "plan": post.pop("original_plan", "free"),
                    }
                else:
                    post.pop("original_wallet", None)
                    post.pop("original_handle", None)
                    post.pop("original_content", None)
                    post.pop("original_trust_score", None)
                    post.pop("original_created_at", None)
                    post.pop("original_plan", None)
                    post["original_post"] = None

                posts.append(post)

            return {"wallet": wallet, "posts": posts}
        finally:
            await release_conn(conn)
    except Exception as e:
        logger.error(
            "get_wallet_posts_error",
            wallet=wallet[:16] if wallet else "",
            error=str(e),
            traceback=traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/activity/{wallet}")
async def get_wallet_activity(
    wallet: str,
    viewer_wallet: str = Query(...),
    limit: int = Query(default=20, ge=1, le=50),
):
    """Get wallet's activity feed — private, only visible to owner."""
    wallet = (wallet or "").strip()
    viewer_wallet = (viewer_wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="Invalid wallet")
    if not viewer_wallet:
        raise HTTPException(status_code=400, detail="viewer_wallet required")

    if viewer_wallet.lower() != wallet.lower():
        raise HTTPException(status_code=403, detail="Activity is private")

    conn = await get_conn()
    try:
        comment_rows = await conn.fetch(
            """
            SELECT
                sp.id,
                sp.content,
                sp.created_at,
                sp.parent_id,
                parent.content AS parent_content,
                parent.wallet AS parent_wallet,
                parent.handle AS parent_handle,
                'commented' AS activity_type
            FROM social_posts sp
            LEFT JOIN social_posts parent ON parent.id = sp.parent_id
            WHERE sp.wallet = $1
              AND sp.parent_id IS NOT NULL
              AND sp.is_hidden = FALSE
            ORDER BY sp.created_at DESC
            LIMIT $2
            """,
            wallet,
            limit,
        )

        like_rows = await conn.fetch(
            """
            SELECT
                sl.post_id AS id,
                sl.created_at,
                sp.content AS parent_content,
                sp.wallet AS parent_wallet,
                sp.handle AS parent_handle,
                'liked' AS activity_type
            FROM social_likes sl
            LEFT JOIN social_posts sp ON sp.id = sl.post_id
            WHERE sl.wallet = $1
            ORDER BY sl.created_at DESC
            LIMIT $2
            """,
            wallet,
            limit,
        )

        repost_rows = await conn.fetch(
            """
            SELECT
                sp.id,
                sp.created_at,
                sp.repost_of AS parent_id,
                orig.content AS parent_content,
                orig.wallet AS parent_wallet,
                orig.handle AS parent_handle,
                'reposted' AS activity_type
            FROM social_posts sp
            LEFT JOIN social_posts orig ON orig.id = sp.repost_of
            WHERE sp.wallet = $1
              AND sp.is_repost = TRUE
              AND sp.is_hidden = FALSE
            ORDER BY sp.created_at DESC
            LIMIT $2
            """,
            wallet,
            limit,
        )

        activities: List[Dict[str, Any]] = []
        for row in comment_rows:
            activities.append(dict(row))
        for row in like_rows:
            activities.append(dict(row))
        for row in repost_rows:
            activities.append(dict(row))

        def _activity_ts(item: Dict[str, Any]) -> float:
            ts = item.get("created_at")
            if ts is None:
                return 0.0
            try:
                return float(ts.timestamp())
            except Exception:
                return 0.0

        activities.sort(key=_activity_ts, reverse=True)

        return {"activities": activities[:limit]}
    finally:
        await release_conn(conn)


@router.post("/bookmark")
async def bookmark_post(body: Dict[str, Any]):
    """
    Add or remove bookmark (toggle).
    Body: { wallet, post_id, session_token }
    """
    wallet = (body.get("wallet") or "").strip()
    post_id = body.get("post_id")
    signature = body.get("signature", "")
    session_token = body.get("session_token", "")

    if not wallet or not post_id:
        raise HTTPException(
            status_code=400,
            detail="wallet and post_id required",
        )

    bypass = BLOCKID_ENV == "DEV" and signature in DEVNET_BYPASS
    if not bypass:
        if BLOCKID_ENV != "DEV":
            if not session_token:
                raise HTTPException(401, detail="session_token required")
            verified_wallet = verify_session_token(session_token)
            if verified_wallet != wallet:
                raise HTTPException(401, detail="Session wallet mismatch")

    conn = await get_conn()
    try:
        existing = await conn.fetchrow(
            "SELECT id FROM post_bookmarks "
            "WHERE wallet = $1 AND post_id = $2",
            wallet,
            int(post_id),
        )

        if existing:
            await conn.execute(
                "DELETE FROM post_bookmarks "
                "WHERE wallet = $1 AND post_id = $2",
                wallet,
                int(post_id),
            )
            return {
                "success": True,
                "bookmarked": False,
                "post_id": post_id,
            }
        else:
            await conn.execute(
                """
                INSERT INTO post_bookmarks
                    (wallet, post_id)
                VALUES ($1, $2)
                ON CONFLICT (wallet, post_id)
                DO NOTHING
                """,
                wallet,
                int(post_id),
            )
            return {
                "success": True,
                "bookmarked": True,
                "post_id": post_id,
            }
    finally:
        await release_conn(conn)


@router.get("/bookmarks/{wallet}")
async def get_bookmarks(wallet: str):
    """
    Get all bookmarked posts for a wallet.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT
                sp.id,
                sp.wallet,
                sp.content,
                sp.image_url,
                sp.like_count,
                sp.reply_count,
                sp.repost_count,
                sp.created_at,
                sp.is_hidden,
                sp.is_repost,
                sp.repost_of,
                sp.quote_content,
                COALESCE(ts.score, sp.trust_score) AS trust_score,
                hr.handle,
                pb.created_at AS bookmarked_at,
                orig.wallet AS original_wallet,
                orig.handle AS original_handle,
                orig.content AS original_content,
                orig.image_url AS original_image_url,
                COALESCE(ts_orig.score, orig.trust_score) AS original_trust_score,
                orig.created_at AS original_created_at,
                COALESCE(sub.plan, 'free') AS plan,
                COALESCE(sub_orig.plan, 'free') AS original_plan,
                sp_prof.avatar_url,
                sp_prof.avatar_type,
                sp_prof.avatar_is_animated
            FROM post_bookmarks pb
            JOIN social_posts sp ON sp.id = pb.post_id
            LEFT JOIN social_profiles sp_prof ON sp_prof.wallet = sp.wallet
            LEFT JOIN social_posts orig ON orig.id = sp.repost_of
            LEFT JOIN handle_registry hr ON hr.owner_wallet = sp.wallet
            LEFT JOIN trust_scores ts ON ts.wallet = sp.wallet
            LEFT JOIN trust_scores ts_orig ON ts_orig.wallet = orig.wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub ON sub.user_id = sp.wallet
            LEFT JOIN (
                SELECT DISTINCT ON (user_id) user_id, plan
                FROM subscriptions
                WHERE status = 'active'
                ORDER BY user_id, created_at DESC NULLS LAST
            ) sub_orig ON sub_orig.user_id = orig.wallet
            WHERE pb.wallet = $1
              AND sp.is_hidden = FALSE
            ORDER BY pb.created_at DESC
            LIMIT 100
            """,
            wallet,
        )

        posts = []
        for r in rows:
            post = {
                "id": r["id"],
                "wallet": r["wallet"],
                "handle": r.get("handle"),
                "content": r["content"],
                "image_url": r.get("image_url"),
                "like_count": r["like_count"],
                "reply_count": r["reply_count"],
                "repost_count": r["repost_count"],
                "created_at": r["created_at"].isoformat()
                if r.get("created_at")
                else None,
                "is_hidden": r["is_hidden"],
                "is_repost": r.get("is_repost", False),
                "repost_of": r.get("repost_of"),
                "quote_content": r.get("quote_content"),
                "trust_score": r["trust_score"],
                "bookmarked_at": r["bookmarked_at"].isoformat()
                if r.get("bookmarked_at")
                else None,
                "plan": r.get("plan", "free"),
                "avatar_url": r.get("avatar_url"),
                "avatar_type": r.get("avatar_type"),
                "avatar_is_animated": r.get("avatar_is_animated"),
            }
            original_wallet = r.get("original_wallet")
            if r.get("is_repost") and r.get("repost_of") and original_wallet:
                post["original_post"] = {
                    "wallet": original_wallet,
                    "handle": r.get("original_handle"),
                    "content": r.get("original_content"),
                    "image_url": r.get("original_image_url"),
                    "trust_score": r.get("original_trust_score"),
                    "created_at": r["original_created_at"].isoformat()
                    if r.get("original_created_at")
                    else None,
                    "plan": r.get("original_plan", "free"),
                }
            else:
                post["original_post"] = None
            posts.append(post)

        return {
            "wallet": wallet,
            "posts": posts,
            "total": len(posts),
        }
    finally:
        await release_conn(conn)


@router.get("/bookmarks/{wallet}/ids")
async def get_bookmark_ids(wallet: str):
    """
    Get just the post IDs that wallet has bookmarked.
    Used for showing bookmark state in feed.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT post_id FROM post_bookmarks "
            "WHERE wallet = $1",
            wallet,
        )
        return {
            "wallet": wallet,
            "post_ids": [r["post_id"] for r in rows],
        }
    finally:
        await release_conn(conn)


@router.get("/liked/{wallet}/ids")
async def get_liked_ids(wallet: str):
    """Get post IDs that this wallet has liked."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT post_id FROM social_likes WHERE wallet = $1",
            wallet,
        )
        return {
            "wallet": wallet,
            "post_ids": [r["post_id"] for r in rows],
        }
    finally:
        await release_conn(conn)


@router.get("/reposted/{wallet}/ids")
async def get_reposted_ids(wallet: str):
    """Get original post IDs that this wallet has reposted."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT repost_of FROM social_posts "
            "WHERE wallet = $1 AND is_repost = TRUE AND repost_of IS NOT NULL",
            wallet,
        )
        return {
            "wallet": wallet,
            "post_ids": [r["repost_of"] for r in rows],
        }
    finally:
        await release_conn(conn)


@router.get("/cashtag/{ticker}/stats")
async def get_cashtag_stats(ticker: str):
    """
    Returns stats for a cashtag token mention in social posts.
    Used by TokenPreviewSheet to show trusted wallet discussions.
    """
    ticker_clean = ticker.upper().lstrip("$")
    if not ticker_clean or len(ticker_clean) > 10:
        raise HTTPException(status_code=400, detail="Invalid ticker")

    conn = await get_conn()
    try:
        pattern = f"%${ticker_clean}%"

        rows = await conn.fetch(
            """
            SELECT DISTINCT
                sp.wallet,
                sp.handle,
                ts.final_score,
                sp2.avatar_url,
                sp2.avatar_type,
                sp2.avatar_is_animated
            FROM social_posts sp
            JOIN trust_scores ts ON sp.wallet = ts.wallet
            LEFT JOIN social_profiles sp2 ON sp2.wallet = sp.wallet
            WHERE sp.content ILIKE $1
              AND ts.final_score > 50
            ORDER BY ts.final_score DESC
            LIMIT 10
            """,
            pattern,
        )

        post_count_row = await conn.fetchrow(
            """
            SELECT COUNT(*) as count
            FROM social_posts
            WHERE content ILIKE $1
              AND created_at > NOW() - INTERVAL '24 hours'
            """,
            pattern,
        )

        wallets = [
            {
                "wallet": r["wallet"],
                "handle": r["handle"],
                "trust_score": round(r["final_score"], 1),
                "avatar_url": r["avatar_url"],
                "avatar_type": r["avatar_type"],
                "avatar_is_animated": r["avatar_is_animated"],
            }
            for r in rows
        ]

        return {
            "ticker": ticker_clean,
            "trusted_wallet_count": len(wallets),
            "post_count_today": post_count_row["count"] if post_count_row else 0,
            "wallets": wallets,
        }
    finally:
        await release_conn(conn)


@router.get("/tokens/list")
async def get_token_list():
    """Proxy Jupiter verified token list to avoid CORS."""
    jupiter_api_key = os.environ.get("JUPITER_API_KEY", "")
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(
                "https://api.jup.ag/tokens/v2/tag?query=verified",
                headers={"x-api-key": jupiter_api_key} if jupiter_api_key else {},
            )
            if resp.status_code == 200:
                return resp.json()
            return []
    except Exception:
        return []


@router.get("/tokens/search/{ticker}")
async def search_token(ticker: str):
    """Proxy Jupiter token search to avoid CORS."""
    ticker_clean = ticker.upper().lstrip("$")[:10]
    jupiter_api_key = os.environ.get("JUPITER_API_KEY", "")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"https://api.jup.ag/tokens/v2/search?query={ticker_clean}",
                headers={"x-api-key": jupiter_api_key} if jupiter_api_key else {},
            )
            if resp.status_code == 200:
                return resp.json()
            return []
    except Exception:
        return []


HELIUS_BASE = (os.getenv("HELIUS_BASE") or "https://api.helius.xyz").rstrip("/")
ACTIVITY_FEED_MAX_WALLETS_PARALLEL = 3
ACTIVITY_FEED_TX_PER_WALLET = 10
LARGE_TRANSFER_SOL = 10.0


async def _fetch_helius_transactions(wallet: str, api_key: str) -> List[Dict[str, Any]]:
    """Fetch last N enhanced transactions for one wallet. Returns [] on failure."""
    url = (
        f"{HELIUS_BASE}/v0/addresses/{wallet}/transactions"
        f"?api-key={api_key}&limit={ACTIVITY_FEED_TX_PER_WALLET}"
        "&type=NFT_MINT,NFT_SALE,SWAP,TRANSFER"
    )
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
    except Exception as e:
        logger.debug("activity_feed_helius_skip", wallet=wallet[:16], error=str(e))
        return []


def _tx_to_activity_item(
    tx: Dict[str, Any],
    wallet: str,
    handle: Optional[str],
    activity_type: str,
) -> Optional[Dict[str, Any]]:
    sig = tx.get("signature") or tx.get("transactionSignature") or tx.get("txHash") or ""
    if not sig:
        return None
    ts = tx.get("timestamp") or tx.get("blockTime") or 0
    timestamp_str = datetime.utcfromtimestamp(ts).isoformat() + "Z" if ts else ""

    description = ""
    amount: Optional[float] = None
    token: Optional[str] = None
    source: Optional[str] = None

    if activity_type == "NFT_MINT":
        desc = tx.get("description") or ""
        name = (tx.get("tokenTransfers") or [{}])[0].get("tokenSymbol") or (tx.get("nftTransfers") or [{}])[0].get("mint") or "NFT"
        description = f"{handle or wallet[:8]} minted {name}" if not desc else desc
    elif activity_type == "NFT_SALE":
        native = (tx.get("nativeTransfers") or [{}])
        amt_lamports = float((native[0].get("amount") or 0)) if native else 0
        amount = amt_lamports / 1e9
        description = f"{handle or wallet[:8]} sold NFT for {amount:.2f} SOL" if amount else (tx.get("description") or "NFT sale")
        token = "SOL"
    elif activity_type == "SWAP":
        source = tx.get("source") or (tx.get("accountData") or [{}])[0].get("nativeBalanceChange") or "DEX"
        description = f"{handle or wallet[:8]} swapped on {source}"
    elif activity_type == "TRANSFER":
        native = tx.get("nativeTransfers") or []
        for nt in native:
            amt_lamports = float(nt.get("amount") or 0)
            if amt_lamports >= LARGE_TRANSFER_SOL * 1e9:
                amount = amt_lamports / 1e9
                description = f"{handle or wallet[:8]} sent {amount:.2f} SOL"
                token = "SOL"
                break
        if not description:
            return None

    return {
        "type": "activity",
        "activity_type": activity_type,
        "wallet": wallet,
        "handle": handle,
        "description": description or str(activity_type),
        "amount": amount,
        "token": token,
        "tx_signature": sig,
        "timestamp": timestamp_str,
        "source": source,
    }


async def get_activity_feed(
    wallets: List[str],
    limit: int = 20,
) -> List[Dict[str, Any]]:
    """
    Fetch on-chain activity from Helius for list of wallets.
    Format as social feed items. Max 3 wallets in parallel; graceful degradation.
    """
    api_key = get_helius_api_key()
    if not api_key or not wallets:
        return []

    unique_wallets = list(dict.fromkeys(wallets))[:ACTIVITY_FEED_MAX_WALLETS_PARALLEL]
    tasks = [_fetch_helius_transactions(w, api_key) for w in unique_wallets]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    all_items: List[Dict[str, Any]] = []
    wallets_with_results: List[str] = []
    for w, res in zip(unique_wallets, results):
        if isinstance(res, Exception):
            continue
        wallets_with_results.append(w)
        for tx in res or []:
            tx_type = (tx.get("type") or "").upper()
            if tx_type not in ("NFT_MINT", "NFT_SALE", "SWAP", "TRANSFER"):
                continue
            if tx_type == "TRANSFER":
                native = tx.get("nativeTransfers") or []
                total = sum(float(n.get("amount") or 0) for n in native)
                if total < LARGE_TRANSFER_SOL * 1e9:
                    continue
            item = _tx_to_activity_item(tx, w, None, tx_type)
            if item:
                all_items.append(item)

    if not all_items:
        return []

    # Resolve handles for wallets that appear in results
    conn = await get_conn()
    try:
        handle_map: Dict[str, Optional[str]] = {}
        for w in wallets_with_results:
            row = await conn.fetchrow(
                "SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1",
                w,
            )
            handle_map[w] = row["handle"] if row else None
        for it in all_items:
            it["handle"] = handle_map.get(it["wallet"])
            if it["handle"] and it.get("description"):
                # Optionally re-render description with handle
                pass
    finally:
        await release_conn(conn)

    all_items.sort(key=lambda x: x.get("timestamp") or "", reverse=True)
    return all_items[:limit]

