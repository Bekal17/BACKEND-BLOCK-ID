from __future__ import annotations

import asyncio
import time
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.ai_engine.priority_wallets import update_priority
from backend_blockid.utils.risk import score_to_risk

logger = get_logger(__name__)

DYNAMIC_RISK_THRESHOLD = 70


async def _get_ml_score(conn: Any, wallet: str) -> float:
    row = await conn.fetchrow(
        "SELECT ml_score, score FROM trust_scores WHERE wallet = $1 LIMIT 1",
        wallet,
    )
    if not row:
        return 50.0
    ml_score = row["ml_score"] if row["ml_score"] is not None else None
    score = row["score"] if row["score"] is not None else None
    return float(ml_score if ml_score is not None else (score if score is not None else 50.0))


async def _get_prior_risk(conn: Any, wallet: str) -> float:
    row = await conn.fetchrow(
        "SELECT dynamic_risk FROM trust_scores WHERE wallet = $1 LIMIT 1",
        wallet,
    )
    if not row or row["dynamic_risk"] is None:
        return 0.0
    return float(row["dynamic_risk"])


async def _get_reason_penalty(conn: Any, wallet: str) -> float:
    rows = await conn.fetch(
        "SELECT weight, confidence_score FROM wallet_reasons WHERE wallet = $1",
        wallet,
    )
    if not rows:
        return 0.0
    total = 0.0
    for row in rows:
        w = float(row["weight"] or 0)
        c = float(row["confidence_score"] if row["confidence_score"] is not None else 1.0)
        total += w * c
    return total


async def _get_neighbors(wallet: str, max_hop: int = 2) -> dict[str, int]:
    """
    BFS neighbors up to max_hop using transactions table (sender -> receiver).
    Returns {wallet: hop_distance}.
    """
    neighbors: dict[str, int] = {}
    visited = {wallet}
    frontier = {wallet}
    conn = await get_conn()
    try:
        for hop in range(1, max_hop + 1):
            if not frontier:
                break
            next_frontier: set[str] = set()
            for w in frontier:
                rows = await conn.fetch(
                    """
                    SELECT sender, receiver
                    FROM transactions
                    WHERE sender = $1 OR receiver = $1
                    """,
                    w,
                )
                for row in rows:
                    sender = (row["sender"] if row else "") or ""
                    receiver = (row["receiver"] if row else "") or ""
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
        await release_conn(conn)
    return neighbors


async def _has_scam_neighbor(wallet: str) -> tuple[bool, bool]:
    neighbors = await _get_neighbors(wallet, max_hop=2)
    if not neighbors:
        return False, False
    conn = await get_conn()
    scam_set: set[str] = set()
    try:
        rows = await conn.fetch("SELECT wallet FROM scam_wallets")
        scam_set = {str(r["wallet"]).strip() for r in rows if r and r.get("wallet")}
    finally:
        await release_conn(conn)
    hop1 = any(w in scam_set and hop == 1 for w, hop in neighbors.items())
    hop2 = any(w in scam_set and hop == 2 for w, hop in neighbors.items())
    return hop1, hop2


async def _get_last_tx_time_and_count(conn: Any, wallet: str) -> tuple[int, int]:
    row = await conn.fetchrow(
        """
        SELECT MAX(timestamp) as max_ts FROM transactions
        WHERE sender = $1 OR receiver = $1
        """,
        wallet,
    )
    last_tx_time = int(row["max_ts"] if row and row["max_ts"] is not None else 0)

    cutoff = int(time.time()) - 86400
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) as cnt FROM transactions
        WHERE (sender = $1 OR receiver = $1)
          AND timestamp > $2
        """,
        wallet, cutoff,
    )
    tx_count = int(row["cnt"] if row and row["cnt"] is not None else 0)
    return last_tx_time, tx_count


async def compute_dynamic_risk(wallet: str) -> dict[str, float]:
    conn = await get_conn()
    try:
        ml_score = await _get_ml_score(conn, wallet)
        prior = await _get_prior_risk(conn, wallet)
        last_tx_time, tx_count_24h = await _get_last_tx_time_and_count(conn, wallet)

        if prior == 0 and last_tx_time == 0:
            # New wallet with no transactions: neutral score, do not inherit ml_score
            updated = 50.0
            logger.info("dynamic_risk_new_wallet_neutral", wallet=wallet[:16])
        elif prior == 0:
            updated = ml_score
        else:
            updated = (0.6 * prior) + (0.4 * ml_score)

        logger.debug(
            "dynamic_risk_update",
            wallet=wallet[:16] + "...",
            ml_score=ml_score,
            prior=prior,
            updated=updated,
        )

        hop1, hop2 = await _has_scam_neighbor(wallet)
        graph_penalty = -30.0 if hop1 else (-15.0 if hop2 else 0.0)

        # Guard: wallets with no transactions should not be penalized
        if last_tx_time == 0:
            days_inactive = 0
        else:
            days_inactive = max(0, (int(time.time()) - last_tx_time) // 86400)

        logger.debug(
            "dynamic_risk_inactivity",
            wallet=wallet[:16] + "...",
            last_tx_time=last_tx_time,
            days_inactive=days_inactive,
        )

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
        await release_conn(conn)


async def update_wallet_score_async(wallet: str) -> dict[str, float]:
    conn = await get_conn()
    now = int(time.time())
    try:
        details = await compute_dynamic_risk(wallet)
        ml_score = details["ml_score"]
        dynamic_risk = details["dynamic_risk"]
        reason_penalty = await _get_reason_penalty(conn, wallet)
        final_score = (dynamic_risk + reason_penalty)
        final_score = max(0.0, min(100.0, final_score))
        risk_level = score_to_risk(int(round(final_score)))

        exists = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet = $1", wallet)
        if exists:
            await conn.execute(
                """
                UPDATE trust_scores SET
                    score = $2,
                    risk_level = $3,
                    ml_score = $4,
                    dynamic_risk = $5,
                    final_score = $6,
                    last_updated = $7,
                    updated_at = CURRENT_TIMESTAMP
                WHERE wallet = $1
                """,
                wallet,
                float(final_score),
                str(risk_level),
                float(ml_score),
                float(dynamic_risk),
                float(final_score),
                now,
            )
        else:
            await conn.execute(
                """
                INSERT INTO trust_scores (
                    wallet, score, risk_level, ml_score, dynamic_risk, final_score, last_updated, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP)
                """,
                wallet,
                float(final_score),
                str(risk_level),
                float(ml_score),
                float(dynamic_risk),
                float(final_score),
                now,
            )

        if dynamic_risk > DYNAMIC_RISK_THRESHOLD:
            await update_priority(wallet, +20)
        if details["days_inactive"] >= 30:
            await update_priority(wallet, -10)

        details["final_score"] = float(final_score)
        details["reason_penalty"] = float(reason_penalty)
        details["risk_level"] = risk_level
        return details
    finally:
        await release_conn(conn)


def update_wallet_score(wallet: str) -> dict[str, float]:
    """Sync wrapper for update_wallet_score_async."""
    return asyncio.get_event_loop().run_until_complete(update_wallet_score_async(wallet))
