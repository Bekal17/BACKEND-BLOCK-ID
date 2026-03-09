"""
BlockID Review Queue Engine.

Holds suspicious wallets for manual review before publishing trust scores.
Used before batch_publish step.

Future upgrades:
* Email alert
* Telegram alert
* UI dashboard
* Auto-review ML
"""
from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from backend_blockid.database.pg_connection import get_conn, release_conn

SCAM_CODES = frozenset({
    "SCAM_CLUSTER_MEMBER",
    "SCAM_CLUSTER_MEMBER_SMALL",
    "SCAM_CLUSTER_MEMBER_LARGE",
    "DRAINER_INTERACTION",
    "DRAINER_FLOW",
    "DRAINER_FLOW_DETECTED",
    "RUG_PULL_DEPLOYER",
    "BLACKLISTED_CREATOR",
    "MEGA_DRAINER",
    "HIGH_RISK_TOKEN_INTERACTION",
})


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _get_previous_score_async(wallet: str) -> tuple[float | None, int | None]:
    """Return (score, timestamp) from wallet_history or trust_scores. Most recent."""
    conn = await get_conn()
    score, ts = None, None
    try:
        rows = await conn.fetch(
            "SELECT score, snapshot_at FROM wallet_history WHERE wallet = $1 ORDER BY snapshot_at DESC LIMIT 2",
            wallet,
        )
        if len(rows) >= 2:
            score = float(rows[1]["score"]) if rows[1]["score"] is not None else None
            ts = int(rows[1]["snapshot_at"]) if rows[1]["snapshot_at"] is not None else None
        if score is None:
            rows = await conn.fetch(
                "SELECT score, computed_at FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 2",
                wallet,
            )
            if len(rows) >= 2:
                score = float(rows[1]["score"]) if rows[1]["score"] is not None else None
                ts = int(rows[1]["computed_at"]) if rows[1]["computed_at"] is not None else None
    except Exception:
        pass
    finally:
        await release_conn(conn)
    return score, ts


async def _get_cluster_size_async(wallet: str) -> int:
    """Return cluster size for wallet from wallet_clusters or wallet_graph_clusters."""
    conn = await get_conn()
    size = 0
    try:
        for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, tbl):
                continue
            row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
            if row:
                cid = row["cluster_id"]
                count_row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE cluster_id = $1", cid)
                size = count_row["cnt"] or 0
                break
    except Exception:
        pass
    finally:
        await release_conn(conn)
    return size


def _has_scam_reason(reasons: list[dict] | list[str] | None) -> bool:
    if not reasons:
        return False
    codes = set()
    for r in reasons:
        if isinstance(r, dict):
            codes.add((r.get("code") or "").strip())
        else:
            codes.add(str(r).strip())
    return bool(codes & SCAM_CODES)


def _reason_codes(reasons: list[dict] | list[str] | None) -> list[str]:
    out = []
    if not reasons:
        return out
    for r in reasons:
        if isinstance(r, dict):
            c = (r.get("code") or r.get("reason_code") or "").strip()
        else:
            c = str(r).strip()
        if c:
            out.append(c)
    return out


async def check_for_review_async(
    wallet: str,
    score: float,
    confidence: float = 0.5,
    reasons: list[dict] | list[str] | None = None,
    *,
    cluster_size: int | None = None,
) -> bool:
    """
    Check if wallet should be queued for manual review.
    Returns True if added to queue.
    """
    reasons = reasons or []
    codes = _reason_codes(reasons)
    has_scam = _has_scam_reason(reasons)

    trigger = False
    reason_hint = ""

    if score < 20 and confidence < 0.7:
        trigger = True
        reason_hint = "low_score_low_confidence"

    prev_score, prev_ts = await _get_previous_score_async(wallet)
    if prev_score is not None and prev_ts is not None:
        now = int(time.time())
        if (now - prev_ts) <= 86400 and abs(score - prev_score) > 40:
            trigger = True
            reason_hint = reason_hint or "score_spike"

    if has_scam and not reason_hint:
        conn = await get_conn()
        try:
            row = await conn.fetchrow("SELECT 1 FROM review_queue WHERE wallet = $1", wallet)
            if not row:
                reason_hint = "new_scam_reason"
                trigger = True
        finally:
            await release_conn(conn)

    if cluster_size is None:
        cluster_size = await _get_cluster_size_async(wallet)
    if cluster_size < 5 and cluster_size > 0 and has_scam:
        trigger = True
        reason_hint = reason_hint or "small_cluster_scam"

    if not trigger:
        return False

    conn = await get_conn()
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS review_queue (
                wallet TEXT PRIMARY KEY,
                score DOUBLE PRECISION,
                confidence DOUBLE PRECISION,
                risk INTEGER,
                reasons TEXT,
                created_at INTEGER,
                status TEXT DEFAULT 'pending'
            )
            """
        )
        reasons_json = json.dumps(codes if codes else reasons)
        now = int(time.time())
        risk = 1 if score < 30 else (2 if score < 60 else 3)
        await conn.execute(
            """
            INSERT INTO review_queue
            (wallet, score, confidence, risk, reasons, created_at, status)
            VALUES ($1, $2, $3, $4, $5, $6, 'pending')
            ON CONFLICT(wallet) DO UPDATE SET
                score = EXCLUDED.score,
                confidence = EXCLUDED.confidence,
                risk = EXCLUDED.risk,
                reasons = EXCLUDED.reasons,
                created_at = EXCLUDED.created_at,
                status = 'pending'
            """,
            wallet, float(score), float(confidence), risk, reasons_json, now,
        )
        print(f"[review_queue] added wallet={wallet[:16]}... score={score} confidence={confidence} reason={reason_hint}")
    finally:
        await release_conn(conn)

    return True


def check_for_review(
    wallet: str,
    score: float,
    confidence: float = 0.5,
    reasons: list[dict] | list[str] | None = None,
    *,
    cluster_size: int | None = None,
) -> bool:
    """Sync wrapper for check_for_review_async."""
    return asyncio.get_event_loop().run_until_complete(
        check_for_review_async(wallet, score, confidence, reasons, cluster_size=cluster_size)
    )


async def is_pending_review_async(wallet: str) -> bool:
    """Return True if wallet is in review_queue with status=pending."""
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT 1 FROM review_queue WHERE wallet = $1 AND status = 'pending'",
            wallet,
        )
        return row is not None
    finally:
        await release_conn(conn)


def is_pending_review(wallet: str) -> bool:
    """Sync wrapper for is_pending_review_async."""
    return asyncio.get_event_loop().run_until_complete(is_pending_review_async(wallet))


async def list_pending_async() -> list[dict[str, Any]]:
    """Return list of pending review items."""
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT wallet, score, confidence, risk, reasons, created_at FROM review_queue WHERE status = 'pending' ORDER BY created_at DESC"
        )
        return [dict(r) for r in rows]
    finally:
        await release_conn(conn)


def list_pending() -> list[dict[str, Any]]:
    """Sync wrapper for list_pending_async."""
    return asyncio.get_event_loop().run_until_complete(list_pending_async())


async def approve_async(wallet: str) -> bool:
    """Set status=approved. Returns True if row updated."""
    conn = await get_conn()
    try:
        result = await conn.execute("UPDATE review_queue SET status = 'approved' WHERE wallet = $1", wallet)
        return "UPDATE 1" in result
    finally:
        await release_conn(conn)


def approve(wallet: str) -> bool:
    """Sync wrapper for approve_async."""
    return asyncio.get_event_loop().run_until_complete(approve_async(wallet))


async def reject_async(wallet: str) -> bool:
    """Set status=rejected. Returns True if row updated."""
    conn = await get_conn()
    try:
        result = await conn.execute("UPDATE review_queue SET status = 'rejected' WHERE wallet = $1", wallet)
        return "UPDATE 1" in result
    finally:
        await release_conn(conn)


def reject(wallet: str) -> bool:
    """Sync wrapper for reject_async."""
    return asyncio.get_event_loop().run_until_complete(reject_async(wallet))
