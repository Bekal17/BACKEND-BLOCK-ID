from __future__ import annotations

import asyncio
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, File, HTTPException, UploadFile, Query, status
from pydantic import BaseModel, Field

from backend_blockid.api_server.identity_eligibility import get_score_tier
from backend_blockid.api_server.privacy_api import _ensure_privacy_settings
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


logger = get_logger(__name__)

router = APIRouter(prefix="/social", tags=["social"])

ENDORSE_TRUST_BOOST = 5
SOCIAL_MIN_SCORE_TO_ENDORSE = 50
SCORE_CAP = 97


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

    if not wallet:
        raise HTTPException(status_code=400, detail="Wallet is required")

    # Dev bypass
    if signature != "devtest_signature_bypass":
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
            logger.info(
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
                trust_score, risk_level
            )
            VALUES (
                $1,
                (SELECT handle FROM handle_registry WHERE owner_wallet = $1 LIMIT 1),
                $2, $3, $4,
                $5, $6, $7, $8,
                $9, $10
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
            SELECT p.*
            FROM social_posts p
            JOIN social_follows f
              ON f.following_wallet = p.wallet
            WHERE f.follower_wallet = $1
              AND p.is_hidden = FALSE
              {before_clause}
            ORDER BY p.created_at DESC
            LIMIT $2
            """,
            *params,
        )
        posts = [dict(r) for r in rows]
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
    conn = await get_conn()
    try:
        before_clause = "AND p.created_at < $3" if before else ""
        params: List[Any] = [min_trust_score, limit]
        if before:
            params.append(before)

        rows = await conn.fetch(
            f"""
            SELECT p.*
            FROM social_posts p
            LEFT JOIN user_privacy_settings ups ON ups.wallet = p.wallet
            WHERE p.post_type = 'PUBLIC'
              AND p.is_hidden = FALSE
              AND COALESCE(p.trust_score, 0) >= $1
              AND (ups.posts_visibility = 'PUBLIC' OR ups.posts_visibility IS NULL)
              {before_clause}
            ORDER BY p.trust_score DESC, p.created_at DESC
            LIMIT $2
            """,
            *params,
        )
        posts = [dict(r) for r in rows]
        next_cursor = posts[-1]["created_at"].isoformat() if posts else None
        return {"posts": posts, "next_cursor": next_cursor}
    finally:
        await release_conn(conn)


@router.post("/follow")
async def follow_wallet(body: Dict[str, Any]):
    follower_wallet = body.get("follower_wallet", "").strip()
    following_wallet = body.get("following_wallet", "").strip()
    signature = body.get("signature", "")

    if not follower_wallet or not following_wallet:
        raise HTTPException(status_code=400, detail="Invalid follower/following wallet")

    if signature != "devtest_signature_bypass":
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

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="Invalid wallet/post_id")

    if signature != "devtest_signature_bypass":
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

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="wallet and post_id required")

    signature = body.get("signature", "")
    if signature != "devtest_signature_bypass":
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


@router.post("/flag")
async def flag_post(body: Dict[str, Any]):
    wallet = body.get("wallet", "").strip()
    post_id = int(body.get("post_id", 0))
    reason = body.get("reason", "") or ""
    signature = body.get("signature", "")

    if not wallet or not post_id:
        raise HTTPException(status_code=400, detail="Invalid wallet/post_id")

    if signature != "devtest_signature_bypass":
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
    if signature != "devtest_signature_bypass":
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

    if not from_wallet or not to_wallet:
        raise HTTPException(status_code=400, detail="Invalid from_wallet or to_wallet")

    if signature != "devtest_signature_bypass":
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

                logger.info(
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
async def get_followers(wallet: str):
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
                 ORDER BY computed_at DESC NULLS LAST LIMIT 1) as trust_score
            FROM social_follows sf
            LEFT JOIN handle_registry hr
                ON hr.owner_wallet = sf.follower_wallet
            WHERE sf.following_wallet = $1
            ORDER BY sf.created_at DESC
            LIMIT 100
            """,
            wallet,
        )
        followers = [dict(r) for r in rows]
        return {
            "wallet": wallet,
            "followers": followers,
            "count": len(followers),
        }
    finally:
        await release_conn(conn)


@router.get("/following/{wallet}")
async def get_following(wallet: str):
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
                 ORDER BY computed_at DESC NULLS LAST LIMIT 1) as trust_score
            FROM social_follows sf
            LEFT JOIN handle_registry hr
                ON hr.owner_wallet = sf.following_wallet
            WHERE sf.follower_wallet = $1
            ORDER BY sf.created_at DESC
            LIMIT 100
            """,
            wallet,
        )
        following = [dict(r) for r in rows]
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
            "SELECT COUNT(*) AS c FROM social_follows WHERE follower_wallet = $1",
            wallet,
        )
        fg = await conn.fetchrow(
            "SELECT COUNT(*) AS c FROM social_follows WHERE following_wallet = $1",
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
            SELECT id, wallet, handle, content, image_url, post_type, reply_count, like_count,
                   trust_score, risk_level, is_hidden, created_at
            FROM social_posts
            WHERE wallet = $1 AND is_hidden = FALSE AND post_type = 'PUBLIC'
            ORDER BY created_at DESC
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
        badges = [r["reason_code"] for r in reason_rows] if reason_rows else []

        return {
            "wallet": wallet,
            "handle": handle_row["handle"] if handle_row else None,
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
            "joined_at": id_row["minted_at"].isoformat() if id_row and id_row.get("minted_at") else None,
        }
    finally:
        await release_conn(conn)


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
            SELECT id, wallet, handle, content, image_url, post_type, parent_id,
                   reply_count, like_count, repost_count, is_hidden, trust_score, risk_level, created_at
            FROM social_posts
            WHERE id = $1
            """,
            post_id,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Post not found")
        replies = await conn.fetch(
            """
            SELECT id, wallet, handle, content, image_url, post_type, parent_id,
                   reply_count, like_count, is_hidden, created_at
            FROM social_posts
            WHERE parent_id = $1 AND is_hidden = FALSE
            ORDER BY created_at ASC
            """,
            post_id,
        )
        return {"post": dict(row), "replies": [dict(r) for r in replies]}
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

    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT id, wallet, handle, content, image_url, post_type, parent_id,
                   reply_count, like_count, repost_count, is_hidden, trust_score,
                   risk_level, created_at, is_repost, repost_of, quote_content
            FROM social_posts
            WHERE wallet = $1 AND is_hidden = FALSE
            ORDER BY created_at DESC
            LIMIT $2
            """,
            wallet,
            limit,
        )
        return {"wallet": wallet, "posts": [dict(r) for r in rows]}
    finally:
        await release_conn(conn)


@router.post("/bookmark")
async def bookmark_post(body: Dict[str, Any]):
    """
    Add or remove bookmark (toggle).
    Body: { wallet, post_id, signature }
    """
    wallet = (body.get("wallet") or "").strip()
    post_id = body.get("post_id")

    if not wallet or not post_id:
        raise HTTPException(
            status_code=400,
            detail="wallet and post_id required",
        )

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
                sp.like_count,
                sp.reply_count,
                sp.repost_count,
                sp.created_at,
                sp.is_hidden,
                sp.trust_score,
                hr.handle,
                pb.created_at AS bookmarked_at
            FROM post_bookmarks pb
            JOIN social_posts sp ON sp.id = pb.post_id
            LEFT JOIN handle_registry hr
                ON hr.owner_wallet = sp.wallet
            WHERE pb.wallet = $1
              AND sp.is_hidden = FALSE
            ORDER BY pb.created_at DESC
            LIMIT 100
            """,
            wallet,
        )

        posts = [
            {
                "id": r["id"],
                "wallet": r["wallet"],
                "handle": r.get("handle"),
                "content": r["content"],
                "like_count": r["like_count"],
                "reply_count": r["reply_count"],
                "repost_count": r["repost_count"],
                "created_at": r["created_at"].isoformat()
                if r.get("created_at")
                else None,
                "is_hidden": r["is_hidden"],
                "trust_score": r["trust_score"],
                "bookmarked_at": r["bookmarked_at"].isoformat()
                if r.get("bookmarked_at")
                else None,
            }
            for r in rows
        ]

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

