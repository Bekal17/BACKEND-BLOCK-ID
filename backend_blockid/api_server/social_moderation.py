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

Defense layers against coordinated reporting:
- Layer 1: Reporter diversity (min 5 unconnected reporters to trigger hide)
- Layer 2: Reporter cooldown (daily limits by trust tier)
- Layer 3: Soft-hide (under_review) before hard hide

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

# Flag thresholds
FLAG_WEIGHT_UNDER_REVIEW = 15  # soft-hide threshold
FLAG_WEIGHT_HIDDEN = 30  # hard-hide threshold
FLAG_DIVERSITY_MIN = 5  # min unique unconnected reporters
DAILY_REPORT_LIMITS = {
    "HIGH": 3,  # score 80+
    "MED": 2,  # score 50-79
    "LOW": 1,  # score 20-49
}


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


async def check_reporter_cooldown(
    wallet: str,
    trust_score: float,
    conn,
) -> bool:
    """
    Returns True if wallet can still report today.
    Logic: daily limit by trust tier; count flags in last 24h; return count < limit.
    """
    if trust_score >= 80:
        limit = DAILY_REPORT_LIMITS["HIGH"]
    elif trust_score >= 50:
        limit = DAILY_REPORT_LIMITS["MED"]
    elif trust_score >= 20:
        limit = DAILY_REPORT_LIMITS["LOW"]
    else:
        return False

    count = await conn.fetchval(
        """
        SELECT COUNT(*)::int FROM social_flags
        WHERE wallet = $1
          AND created_at >= NOW() - INTERVAL '24 hours'
        """,
        wallet,
    )
    return (count or 0) < limit


async def check_reporter_diversity(post_id: int, conn) -> bool:
    """
    Returns True if reporters are sufficiently diverse (not a coordinated group).
    Logic: need FLAG_DIVERSITY_MIN+ reporters; group by follow/endorse edges;
    return True if independent_groups >= FLAG_DIVERSITY_MIN.
    """
    rows = await conn.fetch(
        "SELECT DISTINCT wallet FROM social_flags WHERE post_id = $1",
        post_id,
    )
    wallets = [r["wallet"] for r in rows]
    if len(wallets) < FLAG_DIVERSITY_MIN:
        return False

    # Build adjacency: fetch follows and endorsements between flaggers
    edges: list[tuple[str, str]] = []
    for tbl, col_a, col_b in [
        ("social_follows", "follower_wallet", "following_wallet"),
        ("social_endorsements", "from_wallet", "to_wallet"),
    ]:
        rrows = await conn.fetch(
            f"""
            SELECT {col_a} AS a, {col_b} AS b
            FROM {tbl}
            WHERE {col_a} = ANY($1::text[]) AND {col_b} = ANY($1::text[])
              AND {col_a} != {col_b}
            """,
            wallets,
        )
        for rr in rrows:
            a, b = rr["a"], rr["b"]
            if a and b:
                edges.append((a, b))
                edges.append((b, a))

    # Union-find
    parent: Dict[str, str] = {w: w for w in wallets}

    def find(x: str) -> str:
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]

    def union(a: str, b: str) -> None:
        pa, pb = find(a), find(b)
        if pa != pb:
            parent[pa] = pb

    for a, b in edges:
        union(a, b)

    roots = {find(w) for w in wallets}
    return len(roots) >= FLAG_DIVERSITY_MIN


async def apply_flag_consequence(
    post_id: int,
    flag_weight: int,
    conn,
) -> str:
    """
    Apply consequence based on flag_weight.
    Returns status: "visible" | "under_review" | "hidden"

    Under Review = post STILL VISIBLE with label.
    No auto-hidden timer — only organic flag_weight increase moves status.
    """
    if flag_weight >= FLAG_WEIGHT_HIDDEN:
        diverse = await check_reporter_diversity(post_id, conn)
        if diverse:
            await conn.execute(
                """
                UPDATE social_posts
                SET is_hidden = TRUE,
                    hide_reason = 'COMMUNITY_FLAG'
                WHERE id = $1
                """,
                post_id,
            )
            return "hidden"
        return "under_review"

    if flag_weight >= FLAG_WEIGHT_UNDER_REVIEW:
        diverse = await check_reporter_diversity(post_id, conn)
        if diverse:
            await conn.execute(
                """
                UPDATE social_posts
                SET hide_reason = 'UNDER_REVIEW'
                WHERE id = $1 AND is_hidden = FALSE
                """,
                post_id,
            )
            return "under_review"
        return "visible"

    return "visible"


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
    Uses 3 defense layers: cooldown, diversity, soft-hide.
    """
    flagger_wallet = (flagger_wallet or "").strip()
    if not flagger_wallet:
        return {"flagged": False, "reason": "INVALID_WALLET"}

    # 1. Get flagger trust_score
    ts_row = await conn.fetchrow(
        """
        SELECT score FROM trust_scores
        WHERE wallet = $1
        ORDER BY computed_at DESC NULLS LAST
        LIMIT 1
        """,
        flagger_wallet,
    )
    trust_score = float(ts_row["score"]) if ts_row and ts_row["score"] is not None else 0.0
    if trust_score < 20:
        return {"flagged": False, "reason": "insufficient_trust"}

    # 2. Layer 2 — Cooldown check
    if not await check_reporter_cooldown(flagger_wallet, trust_score, conn):
        return {"flagged": False, "reason": "daily_limit_reached"}

    # 3. Check not already flagged
    existing = await conn.fetchval(
        "SELECT 1 FROM social_flags WHERE post_id = $1 AND wallet = $2",
        post_id,
        flagger_wallet,
    )
    if existing:
        return {"flagged": False, "reason": "already_flagged"}

    # 4. Calculate weight
    weight = calculate_flag_weight(trust_score)

    # 5. Insert flag
    await conn.execute(
        """
        INSERT INTO social_flags (post_id, wallet, reason, flag_weight)
        VALUES ($1, $2, $3, $4)
        """,
        post_id,
        flagger_wallet,
        reason or None,
        weight,
    )

    # 6. Update post flag_weight
    row = await conn.fetchrow(
        """
        UPDATE social_posts
        SET flag_weight = COALESCE(flag_weight, 0) + $1
        WHERE id = $2
        RETURNING flag_weight
        """,
        weight,
        post_id,
    )
    new_flag_weight = int(row["flag_weight"]) if row and row["flag_weight"] is not None else weight

    # 7. Layer 1 + 3 — Apply consequence
    new_status = await apply_flag_consequence(post_id, new_flag_weight, conn)

    return {
        "flagged": True,
        "post_status": new_status,
        "flag_weight": new_flag_weight,
        "reason": reason or "",
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

