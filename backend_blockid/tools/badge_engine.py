"""
Badge Engine for BlockID — reputation badge evolution and timeline.

Computes badge from score, records badge changes, provides evolution timeline.
For UI (app.blockidscore.fun) and Phantom plugin overlay.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.badge_rules import BADGES
from backend_blockid.database.connection import get_connection

logger = get_logger(__name__)


def get_badge(score: float) -> str:
    """Return badge name for given score (0–100)."""
    s = max(0, min(100, float(score)))
    for name, low, high in BADGES:
        if low <= s <= high:
            return name
    return BADGES[-1][0]  # fallback


def _ensure_wallet_badges_table(conn) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_badges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT NOT NULL,
            badge TEXT NOT NULL,
            timestamp INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_badges_wallet ON wallet_badges(wallet)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_badges_timestamp ON wallet_badges(timestamp)")
    conn.commit()


def record_badge_if_changed(
    wallet: str,
    old_score: float | None,
    new_score: float,
    timestamp: int | None = None,
    conn=None,
) -> bool:
    """
    Insert into wallet_badges when badge changes.
    Returns True if badge was recorded.
    """
    import time

    old_badge = get_badge(old_score) if old_score is not None else None
    new_badge = get_badge(new_score)
    if old_badge == new_badge:
        return False

    ts = timestamp if timestamp is not None else int(time.time())
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        _ensure_wallet_badges_table(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO wallet_badges (wallet, badge, timestamp) VALUES (?, ?, ?)",
            (wallet.strip(), new_badge, ts),
        )
        conn.commit()

        old_str = old_badge or "NONE"
        msg = f"[badge_change] wallet={wallet[:16]}... old={old_str} new={new_badge}"
        logger.info("badge_change", wallet=wallet[:16], old=old_str, new=new_badge)
        print(msg)
        return True
    finally:
        if own_conn:
            conn.close()


def get_badge_timeline(wallet: str, conn=None) -> list[dict[str, Any]]:
    """
    Load wallet_history scores, compute badge per timestamp.
    Returns list of {"date": "YYYY-MM-DD HH:MM", "badge": "TRUSTED", "score": 65}.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        cur = conn.cursor()
        # wallet_history may have score or (prior, posterior) from Bayesian inserts
        cur.execute("PRAGMA table_info(wallet_history)")
        cols = {row[1] for row in cur.fetchall()}
        if "posterior" in cols:
            cur.execute(
                """
                SELECT COALESCE(score, (1.0 - COALESCE(posterior, 0.5)) * 100) AS score, snapshot_at
                FROM wallet_history
                WHERE wallet = ?
                ORDER BY snapshot_at ASC
                """,
                (wallet.strip(),),
            )
        else:
            cur.execute(
                """
                SELECT COALESCE(score, 50) AS score, snapshot_at
                FROM wallet_history
                WHERE wallet = ?
                ORDER BY snapshot_at ASC
                """,
                (wallet.strip(),),
            )
        rows = cur.fetchall()

        timeline: list[dict[str, Any]] = []
        for r in rows:
            score = float(r["score"] if hasattr(r, "keys") else r[0] or 50)
            ts = int(r["snapshot_at"] if hasattr(r, "keys") else r[1] or 0)
            dt = datetime.utcfromtimestamp(ts)
            date_str = dt.strftime("%Y-%m-%d %H:%M")
            badge = get_badge(score)
            timeline.append({"date": date_str, "badge": badge, "score": int(round(score))})

        # Append current from trust_scores if different from last history
        cur.execute(
            "SELECT score FROM trust_scores WHERE wallet = ? LIMIT 1",
            (wallet.strip(),),
        )
        row = cur.fetchone()
        if row:
            score = float(row["score"] if hasattr(row, "keys") else row[0] or 50)
            if not timeline or abs(timeline[-1]["score"] - score) > 0.5:
                import time
                ts = int(time.time())
                dt = datetime.utcfromtimestamp(ts)
                timeline.append({
                    "date": dt.strftime("%Y-%m-%d %H:%M"),
                    "badge": get_badge(score),
                    "score": int(round(score)),
                })

        return timeline
    finally:
        if own_conn:
            conn.close()
