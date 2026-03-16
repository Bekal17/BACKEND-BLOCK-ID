"""
Social Layer moderation engine.

Layer 1 — Auto-hide:
- risk_level = HIGH → auto-hide
- daemon_is_sanctioned = True → permanent hide
- trust_score < 20 → label warning (not hidden)

Layer 2 — Community flag (weighted):
- Flag weight by trust score:
  80+ = weight 3
  50-79 = weight 2
  20-49 = weight 1
  < 20  = weight 0 (cannot flag)
- Total weight >= 10 → auto-hidden

Consequences:
- 3 hides in 30 days → trust_score penalty -5, rate limited (3 posts/day)
- 5 hides in 30 days → trust_score penalty -10, suspended 7 days
- Sanctioned → permanent disable

Appeal:
- 3 trusted wallets (score 80+) endorse post → unhide
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict

from backend_blockid.database.pg_connection import get_conn, release_conn


def calculate_flag_weight(trust_score: float) -> int:
    """
    Calculate flag weight based on trust score.
    80+ = 3, 50-79 = 2, 20-49 = 1, <20 = 0.
    """
    if trust_score >= 80:
        return 3
    if trust_score >= 50:
        return 2
    if trust_score >= 20:
        return 1
    return 0


async def check_post_visibility(
    wallet: str,
    conn,
) -> Dict[str, Any]:
    """
    Check if wallet is allowed to post and what visibility rules apply.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return {
            "can_post": False,
            "reason": "INVALID_WALLET",
            "auto_hide": False,
            "hide_reason": None,
            "warning_label": None,
            "rate_limit": 0,
            "suspended_until": None,
        }

    # Default response
    result: Dict[str, Any] = {
        "can_post": True,
        "reason": None,
        "auto_hide": False,
        "hide_reason": None,
        "warning_label": None,
        "rate_limit": 20,
        "suspended_until": None,
    }

    # Check posting restrictions (content moderation)
    restriction = await conn.fetchrow(
        """
        SELECT restriction_type, posts_per_day, restricted_until
        FROM posting_restrictions
        WHERE wallet = $1
        """,
        wallet,
    )
    if restriction:
        rtype = (restriction["restriction_type"] or "").strip().upper()
        restricted_until = restriction["restricted_until"]
        now = datetime.utcnow()
        # Strip timezone from DB value so we can compare with naive utcnow()
        if restricted_until is not None and getattr(restricted_until, "tzinfo", None) is not None:
            restricted_until = restricted_until.replace(tzinfo=None)
        if rtype == "PERMANENT":
            result.update(
                {
                    "can_post": False,
                    "reason": "PERMANENTLY_DISABLED",
                    "rate_limit": 0,
                }
            )
            return result
        if rtype in ("SUSPENDED", "RATE_LIMITED") and restricted_until and restricted_until > now:
            if rtype == "SUSPENDED":
                result.update(
                    {
                        "can_post": False,
                        "reason": "SUSPENDED",
                        "rate_limit": 0,
                        "suspended_until": restricted_until,
                    }
                )
                return result
            result["rate_limit"] = int(restriction["posts_per_day"] or 20)
            result["suspended_until"] = restricted_until

    # Load trust and sanctions info (trust_scores uses "score" not "trust_score")
    row = await conn.fetchrow(
        """
        SELECT score AS trust_score, risk_level, daemon_is_sanctioned
        FROM trust_scores
        WHERE wallet = $1
        """,
        wallet,
    )
    trust_score = float(row["trust_score"]) if row and row["trust_score"] is not None else 0.0
    risk_level = (row["risk_level"] or "").upper() if row and row["risk_level"] else ""
    daemon_is_sanctioned = bool(row["daemon_is_sanctioned"]) if row and "daemon_is_sanctioned" in row else False

    # Layer 1: sanctions / high risk
    if daemon_is_sanctioned:
        result.update(
            {
                "can_post": False,
                "reason": "SANCTIONED",
                "auto_hide": True,
                "hide_reason": "SANCTIONED",
                "rate_limit": 0,
            }
        )
        return result

    if risk_level == "HIGH":
        result.update(
            {
                "auto_hide": True,
                "hide_reason": "HIGH_RISK",
            }
        )

    if trust_score < 20:
        result["warning_label"] = "LOW_TRUST_SCORE"

    # Layer 2: recent hides and rate limiting
    now = datetime.utcnow()
    window_start = now - timedelta(days=30)
    hides_row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE is_hidden) AS hidden_count
        FROM social_posts
        WHERE wallet = $1 AND created_at >= $2
        """,
        wallet,
        window_start,
    )
    hidden_count = int(hides_row["hidden_count"]) if hides_row and hides_row["hidden_count"] is not None else 0

    # Basic rate limiting defaults
    rate_limit = 20
    suspended_until = None

    if hidden_count >= 5:
        # Strong penalty: suspend 7 days
        suspended_until = now + timedelta(days=7)
        result.update(
            {
                "can_post": False,
                "reason": "SUSPENDED",
                "rate_limit": 0,
                "suspended_until": suspended_until,
            }
        )
    elif hidden_count >= 3:
        # Soft penalty: tighter rate limit
        rate_limit = 3
        result["rate_limit"] = rate_limit
    else:
        result["rate_limit"] = rate_limit

    return result


async def process_flag(
    post_id: int,
    flagger_wallet: str,
    reason: str,
    conn,
) -> Dict[str, Any]:
    """
    Process a community flag on a post.
    """
    flagger_wallet = (flagger_wallet or "").strip()
    if not flagger_wallet:
        return {"flagged": False, "post_hidden": False, "reason": "INVALID_WALLET"}

    # 1. Check flagger trust score (trust_scores uses "score" not "trust_score")
    ts_row = await conn.fetchrow(
        "SELECT score AS trust_score FROM trust_scores WHERE wallet = $1",
        flagger_wallet,
    )
    trust_score = float(ts_row["trust_score"]) if ts_row and ts_row["trust_score"] is not None else 0.0
    weight = calculate_flag_weight(trust_score)
    if weight <= 0:
        return {
            "flagged": False,
            "post_hidden": False,
            "reason": "INSUFFICIENT_TRUST_SCORE",
        }

    # 2. Check not already flagged by this wallet
    existing = await conn.fetchval(
        "SELECT 1 FROM social_flags WHERE post_id = $1 AND wallet = $2",
        post_id,
        flagger_wallet,
    )
    if existing:
        return {
            "flagged": False,
            "post_hidden": False,
            "reason": "ALREADY_FLAGGED",
        }

    # 3–6. Insert flag, update aggregate weight, hide if threshold reached
    await conn.execute(
        """
        INSERT INTO social_flags (post_id, wallet, reason, flag_weight)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (post_id, wallet) DO NOTHING
        """,
        post_id,
        flagger_wallet,
        reason,
        weight,
    )

    flags_row = await conn.fetchrow(
        """
        SELECT COALESCE(SUM(flag_weight), 0) AS total_weight
        FROM social_flags
        WHERE post_id = $1
        """,
        post_id,
    )
    total_weight = int(flags_row["total_weight"]) if flags_row and flags_row["total_weight"] is not None else 0

    post_hidden = False
    if total_weight >= 10:
        await conn.execute(
            """
            UPDATE social_posts
            SET is_hidden = TRUE,
                hide_reason = COALESCE(hide_reason, 'FLAGGED'),
                flag_weight = $2
            WHERE id = $1
            """,
            post_id,
            total_weight,
        )
        post_hidden = True
    else:
        await conn.execute(
            "UPDATE social_posts SET flag_weight = $2 WHERE id = $1",
            post_id,
            total_weight,
        )

    return {
        "flagged": True,
        "post_hidden": post_hidden,
        "flag_weight_added": weight,
        "total_weight": total_weight,
        "reason": "OK",
    }


async def check_appeal(
    post_id: int,
    conn,
) -> Dict[str, Any]:
    """
    Check if post qualifies for appeal unhide.
    3 trusted wallets (score 80+) must endorse.
    """
    # Count endorsements from high-trust wallets
    rows = await conn.fetch(
        """
        SELECT se.from_wallet
        FROM social_endorsements se
        JOIN trust_scores ts ON ts.wallet = se.from_wallet
        WHERE se.to_wallet = (
            SELECT wallet FROM social_posts WHERE id = $1
        )
          AND se.is_active = TRUE
          AND ts.score >= 80
        """,
        post_id,
    )
    endorsers = {r["from_wallet"] for r in rows}
    qualifies = len(endorsers) >= 3

    return {
        "qualifies": qualifies,
        "endorser_count": len(endorsers),
        "endorsers": list(endorsers),
    }

