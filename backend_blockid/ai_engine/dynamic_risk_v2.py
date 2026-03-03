from __future__ import annotations

import time
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.connection import get_connection
from backend_blockid.ai_engine.priority_wallets import update_priority
from backend_blockid.utils.risk import score_to_risk

logger = get_logger(__name__)

DYNAMIC_RISK_THRESHOLD = 70


def _get_ml_score(cur: Any, wallet: str) -> float:
    cur.execute(
        "SELECT ml_score, score FROM trust_scores WHERE wallet = ? LIMIT 1",
        (wallet,),
    )
    row = cur.fetchone()
    if not row:
        return 50.0
    ml_score = row[0] if row[0] is not None else None
    score = row[1] if row[1] is not None else None
    return float(ml_score if ml_score is not None else (score if score is not None else 50.0))


def _get_prior_risk(cur: Any, wallet: str) -> float:
    cur.execute(
        "SELECT dynamic_risk FROM trust_scores WHERE wallet = ? LIMIT 1",
        (wallet,),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return 0.0
    return float(row[0])


def _get_reason_penalty(cur: Any, wallet: str) -> float:
    cur.execute(
        "SELECT weight, confidence_score FROM wallet_reasons WHERE wallet = ?",
        (wallet,),
    )
    rows = cur.fetchall()
    if not rows:
        return 0.0
    total = 0.0
    for weight, confidence in rows:
        w = float(weight or 0)
        c = float(confidence if confidence is not None else 1.0)
        total += w * c
    return total


def _get_neighbors(wallet: str, max_hop: int = 2) -> dict[str, int]:
    """
    BFS neighbors up to max_hop using transactions table (sender -> receiver).
    Returns {wallet: hop_distance}.
    """
    neighbors: dict[str, int] = {}
    visited = {wallet}
    frontier = {wallet}
    conn = get_connection()
    cur = conn.cursor()
    try:
        for hop in range(1, max_hop + 1):
            if not frontier:
                break
            next_frontier: set[str] = set()
            for w in frontier:
                cur.execute(
                    """
                    SELECT sender, receiver
                    FROM transactions
                    WHERE sender = ? OR receiver = ?
                    """,
                    (w, w),
                )
                for row in cur.fetchall():
                    sender = (row[0] if row else "") or ""
                    receiver = (row[1] if row else "") or ""
                    for candidate in (sender, receiver):
                        candidate = str(candidate).strip()
                        if not candidate or candidate == wallet or candidate in visited:
                            continue
                        next_frontier.add(candidate)
            for n in next_frontier:
                neighbors[n] = hop
            visited |= next_frontier
            frontier = next_frontier
    finally:
        conn.close()
    return neighbors


def _has_scam_neighbor(wallet: str) -> tuple[bool, bool]:
    neighbors = _get_neighbors(wallet, max_hop=2)
    if not neighbors:
        return False, False
    conn = get_connection()
    cur = conn.cursor()
    scam_set: set[str] = set()
    try:
        cur.execute("SELECT wallet FROM scam_wallets")
        scam_set = {str(r[0]).strip() for r in cur.fetchall() if r and r[0]}
    finally:
        conn.close()
    hop1 = any(w in scam_set and hop == 1 for w, hop in neighbors.items())
    hop2 = any(w in scam_set and hop == 2 for w, hop in neighbors.items())
    return hop1, hop2


def _get_last_tx_time_and_count(cur: Any, wallet: str) -> tuple[int, int]:
    cur.execute(
        """
        SELECT MAX(timestamp) FROM transactions
        WHERE sender = ? OR receiver = ?
        """,
        (wallet, wallet),
    )
    row = cur.fetchone()
    last_tx_time = int(row[0] if row and row[0] is not None else 0)

    cutoff = int(time.time()) - 86400
    cur.execute(
        """
        SELECT COUNT(*) FROM transactions
        WHERE (sender = ? OR receiver = ?)
          AND timestamp > ?
        """,
        (wallet, wallet, cutoff),
    )
    row = cur.fetchone()
    tx_count = int(row[0] if row and row[0] is not None else 0)
    return last_tx_time, tx_count


def compute_dynamic_risk(wallet: str) -> dict[str, float]:
    conn = get_connection()
    cur = conn.cursor()
    try:
        ml_score = _get_ml_score(cur, wallet)
        prior = _get_prior_risk(cur, wallet)
        # Bayesian update (weighted average)
        updated = (0.6 * prior) + (0.4 * ml_score)

        hop1, hop2 = _has_scam_neighbor(wallet)
        graph_penalty = -30.0 if hop1 else (-15.0 if hop2 else 0.0)

        last_tx_time, tx_count_24h = _get_last_tx_time_and_count(cur, wallet)
        days_inactive = 0
        if last_tx_time:
            days_inactive = max(0, (int(time.time()) - last_tx_time) // 86400)
        decay = -2.0 * days_inactive

        activity_boost = 2.0 * tx_count_24h

        dynamic_risk = updated + graph_penalty + decay + activity_boost
        dynamic_risk = max(0.0, min(100.0, dynamic_risk))

        return {
            "ml_score": float(ml_score),
            "prior": float(prior),
            "graph_penalty": float(graph_penalty),
            "decay": float(decay),
            "activity_boost": float(activity_boost),
            "dynamic_risk": float(dynamic_risk),
            "last_tx_time": float(last_tx_time),
            "tx_count_24h": float(tx_count_24h),
            "days_inactive": float(days_inactive),
        }
    finally:
        conn.close()


def update_wallet_score(wallet: str) -> dict[str, float]:
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())
    try:
        details = compute_dynamic_risk(wallet)
        ml_score = details["ml_score"]
        dynamic_risk = details["dynamic_risk"]
        reason_penalty = _get_reason_penalty(cur, wallet)
        final_score = (dynamic_risk - reason_penalty)
        final_score = max(0.0, min(100.0, final_score))
        risk_level = score_to_risk(int(round(final_score)))

        cur.execute(
            """
            INSERT INTO trust_scores (
                wallet, score, risk_level, ml_score, dynamic_risk, final_score, last_updated, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(wallet) DO UPDATE SET
                score=excluded.score,
                risk_level=excluded.risk_level,
                ml_score=excluded.ml_score,
                dynamic_risk=excluded.dynamic_risk,
                final_score=excluded.final_score,
                last_updated=excluded.last_updated,
                updated_at=excluded.updated_at
            """,
            (
                wallet,
                float(final_score),
                int(risk_level),
                float(ml_score),
                float(dynamic_risk),
                float(final_score),
                now,
                now,
            ),
        )
        conn.commit()

        # Priority updates based on dynamic risk and inactivity
        if dynamic_risk > DYNAMIC_RISK_THRESHOLD:
            update_priority(wallet, +20)
        if details["days_inactive"] >= 30:
            update_priority(wallet, -10)

        details["final_score"] = float(final_score)
        details["reason_penalty"] = float(reason_penalty)
        details["risk_level"] = float(risk_level)
        return details
    finally:
        conn.close()
