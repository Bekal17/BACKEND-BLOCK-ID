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

import json
import time
from typing import Any

from backend_blockid.database.connection import get_connection

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


def _get_previous_score(wallet: str) -> tuple[float | None, int | None]:
    """Return (score, timestamp) from wallet_history or trust_scores. Most recent."""
    conn = get_connection()
    cur = conn.cursor()
    score, ts = None, None
    try:
        cur.execute(
            "SELECT score, snapshot_at FROM wallet_history WHERE wallet = ? ORDER BY snapshot_at DESC LIMIT 2",
            (wallet,),
        )
        rows = cur.fetchall()
        if len(rows) >= 2:
            score = float(rows[1][0]) if rows[1][0] is not None else None
            ts = int(rows[1][1]) if rows[1][1] is not None else None
        if score is None:
            cur.execute(
                "SELECT score, computed_at FROM trust_scores WHERE wallet = ? ORDER BY computed_at DESC LIMIT 2",
                (wallet,),
            )
            rows = cur.fetchall()
            if len(rows) >= 2:
                score = float(rows[1][0]) if rows[1][0] is not None else None
                ts = int(rows[1][1]) if rows[1][1] is not None else None
    except Exception:
        pass
    finally:
        conn.close()
    return score, ts


def _get_cluster_size(wallet: str) -> int:
    """Return cluster size for wallet from wallet_clusters or wallet_graph_clusters."""
    conn = get_connection()
    cur = conn.cursor()
    size = 0
    try:
        for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
            if not cur.fetchone():
                continue
            cur.execute(f"SELECT cluster_id FROM {tbl} WHERE wallet = ? LIMIT 1", (wallet,))
            row = cur.fetchone()
            if row:
                cid = row[0]
                cur.execute(f"SELECT COUNT(*) FROM {tbl} WHERE cluster_id = ?", (cid,))
                size = cur.fetchone()[0] or 0
                break
    except Exception:
        pass
    finally:
        conn.close()
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


def check_for_review(
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

    # 1. score < 20 AND confidence < 0.7
    if score < 20 and confidence < 0.7:
        trigger = True
        reason_hint = "low_score_low_confidence"

    # 2. score changed > 40 in one day
    prev_score, prev_ts = _get_previous_score(wallet)
    if prev_score is not None and prev_ts is not None:
        now = int(time.time())
        if (now - prev_ts) <= 86400 and abs(score - prev_score) > 40:
            trigger = True
            reason_hint = reason_hint or "score_spike"

    # 3. new scam reason added (has scam code; treat as new if not in queue yet)
    if has_scam and not reason_hint:
        conn = get_connection()
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM review_queue WHERE wallet = ?", (wallet,))
            if not cur.fetchone():
                reason_hint = "new_scam_reason"
                trigger = True
        finally:
            conn.close()

    # 4. cluster size < 5 but flagged scam
    if cluster_size is None:
        cluster_size = _get_cluster_size(wallet)
    if cluster_size < 5 and cluster_size > 0 and has_scam:
        trigger = True
        reason_hint = reason_hint or "small_cluster_scam"

    if not trigger:
        return False

    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS review_queue (
                wallet TEXT PRIMARY KEY,
                score REAL,
                confidence REAL,
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
        cur.execute(
            """
            INSERT OR REPLACE INTO review_queue
            (wallet, score, confidence, risk, reasons, created_at, status)
            VALUES (?, ?, ?, ?, ?, ?, 'pending')
            """,
            (wallet, float(score), float(confidence), risk, reasons_json, now),
        )
        conn.commit()
        print(f"[review_queue] added wallet={wallet[:16]}... score={score} confidence={confidence} reason={reason_hint}")
    finally:
        conn.close()

    return True


def is_pending_review(wallet: str) -> bool:
    """Return True if wallet is in review_queue with status=pending."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT 1 FROM review_queue WHERE wallet = ? AND status = 'pending'",
            (wallet,),
        )
        return cur.fetchone() is not None
    finally:
        conn.close()


def list_pending() -> list[dict[str, Any]]:
    """Return list of pending review items."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT wallet, score, confidence, risk, reasons, created_at FROM review_queue WHERE status = 'pending' ORDER BY created_at DESC"
        )
        rows = cur.fetchall()
        cols = ["wallet", "score", "confidence", "risk", "reasons", "created_at"]
        return [dict(zip(cols, r)) for r in rows]
    finally:
        conn.close()


def approve(wallet: str) -> bool:
    """Set status=approved. Returns True if row updated."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE review_queue SET status = 'approved' WHERE wallet = ?", (wallet,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def reject(wallet: str) -> bool:
    """Set status=rejected. Returns True if row updated."""
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE review_queue SET status = 'rejected' WHERE wallet = ?", (wallet,))
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()
