"""
Real-time Dynamic Risk Engine for BlockID.

Updates wallet trust scores when new transactions are detected.
Rate-limited: max 1 update per wallet per 5 minutes.
TEST_MODE: skip propagation, use dummy risk update.

Uses PostgreSQL (asyncpg). All DB operations are async.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.database.repositories import (
    get_wallet_reasons,
    update_wallet_score,
    save_wallet_risk_probability,
)
from backend_blockid.ml.bayesian_risk import (
    get_prior,
    update_scam_probability,
    save_bayesian_history,
    LIKELIHOOD_TABLE,
)
from backend_blockid.ml.dynamic_risk import compute_dynamic_penalty
from backend_blockid.utils.risk import risk_level_from_reasons

logger = get_logger(__name__)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
RATE_LIMIT_SEC = 5 * 60  # 5 minutes


async def _ensure_wallet_last_update_table(conn) -> None:
    """Create wallet_last_update table if missing (rate limit cache)."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS wallet_last_update (
            wallet TEXT PRIMARY KEY,
            timestamp BIGINT NOT NULL
        )
    """)


async def _check_rate_limit(conn, wallet: str) -> bool:
    """Return True if wallet can be updated (not rate-limited)."""
    await _ensure_wallet_last_update_table(conn)
    row = await conn.fetchrow(
        "SELECT timestamp FROM wallet_last_update WHERE wallet=$1", wallet
    )
    if not row:
        return True
    ts = row.get("timestamp")
    return (int(time.time()) - int(ts or 0)) >= RATE_LIMIT_SEC


async def _record_update(conn, wallet: str) -> None:
    """Record wallet update timestamp for rate limiting."""
    now = int(time.time())
    await conn.execute("""
        INSERT INTO wallet_last_update (wallet, timestamp)
        VALUES ($1, $2)
        ON CONFLICT(wallet) DO UPDATE SET timestamp=$2
    """, wallet, now)


async def _save_wallet_history(wallet: str, score: int, risk_level: str, reason_codes: list[str]) -> None:
    """Insert snapshot into wallet_history."""
    conn = await get_conn()
    try:
        now = int(time.time())
        rc_json = json.dumps(reason_codes)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_history (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                score DOUBLE PRECISION,
                risk_level TEXT,
                reason_codes TEXT,
                snapshot_at BIGINT
            )
        """)
        await conn.execute(
            """
            INSERT INTO wallet_history (wallet, score, risk_level, reason_codes, snapshot_at)
            VALUES ($1, $2, $3, $4, $5)
            """,
            wallet, score, risk_level, rc_json, now,
        )
    finally:
        await release_conn(conn)


async def _get_current_score(conn, wallet: str) -> float:
    """Get current trust score for wallet, default 50."""
    row = await conn.fetchrow("SELECT score FROM trust_scores WHERE wallet=$1", wallet)
    if row and row.get("score") is not None:
        return float(row["score"])
    return 50.0


async def update_wallet_risk(wallet: str) -> dict[str, Any] | None:
    """
    Update risk score for a single wallet. Async, uses PostgreSQL.

    1. Recompute cluster link (propagation) - skipped for now, uses reasons only
    2. Get reasons from wallet_reasons
    3. Dynamic risk Bayesian update
    4. Update trust_scores
    5. Save wallet_history

    Returns dict with new_score, reasons, etc. or None if rate-limited/skipped.
    """
    if not wallet or not wallet.strip():
        return None

    wallet = wallet.strip()
    conn = await get_conn()
    try:
        if not await _check_rate_limit(conn, wallet):
            logger.info("realtime_risk_rate_limited", wallet=wallet[:16] + "...")
            return None

        if TEST_MODE:
            base_score = await _get_current_score(conn, wallet)
            dummy_penalty = -5
            final_score = max(0, min(100, int(base_score) + dummy_penalty))
            reason_codes = ["TEST_MODE_DUMMY"]
            risk_level = "medium" if final_score < 70 else "low"
            await update_wallet_score(wallet, final_score, risk_level, "{}")
            await _save_wallet_history(wallet, final_score, risk_level, reason_codes)
            await _record_update(conn, wallet)
            logger.info(
                "realtime_risk",
                wallet=wallet[:16] + "...",
                new_score=final_score,
                reasons=",".join(reason_codes),
            )
            return {"wallet": wallet, "new_score": final_score, "reasons": reason_codes}

        # Propagation skipped for async pg path (propagation_engine_v1 uses sync sqlite)
        # 2. Get reasons from wallet_reasons
        reasons = await get_wallet_reasons(wallet)
        if not reasons:
            reasons = [{"code": "NO_RISK_DETECTED", "weight": 0, "confidence": 1, "days_old": 0}]

        seen = {}
        for r in reasons:
            code = r.get("code") or ""
            if code and code not in seen:
                seen[code] = r
        reasons = list(seen.values())

        base_score = await _get_current_score(conn, wallet)

        prior = get_prior(wallet)
        prior = prior if prior is not None else 0.05
        posterior = update_scam_probability(prior, reasons)
        reasons_for_log = [
            {
                "code": r.get("code"),
                "likelihood": LIKELIHOOD_TABLE.get(r.get("code"), 0.05),
                "confidence": r.get("confidence", 1),
            }
            for r in reasons
        ]
        try:
            await save_wallet_risk_probability(wallet, prior, posterior, reasons_for_log)
        except Exception as e:
            logger.debug("save_wallet_risk_probability_skip", wallet=wallet[:16], error=str(e))
        try:
            save_bayesian_history(wallet, prior, posterior, [r.get("code", "") for r in reasons if r.get("code")])
        except Exception:
            pass

        penalty, reasons = compute_dynamic_penalty(reasons, wallet, cluster_size=0, flow_amount=0, tx_count=0)
        final_score = int(base_score) + penalty - int(posterior * 100)
        final_score = max(0, min(100, final_score))
        risk_level = risk_level_from_reasons(reasons)
        reason_codes = [r.get("code", "") for r in reasons if r.get("code")]

        await update_wallet_score(wallet, final_score, risk_level, "{}")
        await _save_wallet_history(wallet, final_score, risk_level, reason_codes)
        await _record_update(conn, wallet)

        reasons_str = ",".join(reason_codes) if reason_codes else "NONE"
        logger.info(
            "realtime_risk",
            wallet=wallet[:16] + "...",
            new_score=final_score,
            reasons=reasons_str,
        )
        return {"wallet": wallet, "new_score": final_score, "reasons": reason_codes}
    finally:
        await release_conn(conn)


def process_new_transaction(tx: dict[str, Any]) -> None:
    """
    Process a new transaction: update risk for from_wallet and to_wallet.
    Sync wrapper that runs async update_wallet_risk.
    """
    import asyncio
    from_wallet = (tx.get("from_wallet") or "").strip()
    to_wallet = (tx.get("to_wallet") or "").strip()

    processed = set()
    for w in (from_wallet, to_wallet):
        if w and w not in processed:
            processed.add(w)
            try:
                asyncio.get_event_loop().run_until_complete(update_wallet_risk(w))
            except RuntimeError:
                asyncio.run(update_wallet_risk(w))
            except Exception as e:
                logger.exception("realtime_risk_update_failed", wallet=w[:16] + "...", error=str(e))
