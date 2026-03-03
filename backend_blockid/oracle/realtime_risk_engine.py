"""
Real-time Dynamic Risk Engine for BlockID.

Updates wallet trust scores when new transactions are detected.
Rate-limited: max 1 update per wallet per 5 minutes.
TEST_MODE: skip propagation, use dummy risk update.

Future upgrades:
- Kafka queue for transactions
- Websocket alerts
- Phantom plugin instant warning
- Exchange API webhook
"""

from __future__ import annotations

import json
import os
import time
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.connection import get_connection
from backend_blockid.database.repositories import get_wallet_reasons, update_wallet_score, save_wallet_risk_probability
from backend_blockid.ml.bayesian_risk import get_prior, update_scam_probability, save_bayesian_history, LIKELIHOOD_TABLE
from backend_blockid.ml.dynamic_risk import compute_dynamic_penalty
from backend_blockid.utils.risk import risk_level_from_reasons

logger = get_logger(__name__)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
RATE_LIMIT_SEC = 5 * 60  # 5 minutes


def _ensure_wallet_last_update_table(conn) -> None:
    """Create wallet_last_update table if missing (rate limit cache)."""
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS wallet_last_update (
            wallet TEXT PRIMARY KEY,
            timestamp INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def _check_rate_limit(conn, wallet: str) -> bool:
    """Return True if wallet can be updated (not rate-limited)."""
    _ensure_wallet_last_update_table(conn)
    cur = conn.cursor()
    cur.execute(
        "SELECT timestamp FROM wallet_last_update WHERE wallet = ?",
        (wallet,),
    )
    row = cur.fetchone()
    if not row:
        return True
    ts = row["timestamp"] if hasattr(row, "keys") else row[0]
    return (int(time.time()) - int(ts)) >= RATE_LIMIT_SEC


def _record_update(conn, wallet: str) -> None:
    """Record wallet update timestamp for rate limiting."""
    cur = conn.cursor()
    now = int(time.time())
    cur.execute(
        """
        INSERT OR REPLACE INTO wallet_last_update (wallet, timestamp)
        VALUES (?, ?)
        """,
        (wallet, now),
    )
    conn.commit()


def _save_wallet_history(wallet: str, score: int, risk_level: str, reason_codes: list[str]) -> None:
    """Insert snapshot into wallet_history."""
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())
    rc_json = json.dumps(reason_codes)
    cur.execute(
        """
        INSERT INTO wallet_history (wallet, score, risk_level, reason_codes, snapshot_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (wallet, score, risk_level, rc_json, now),
    )
    conn.commit()
    conn.close()


def _get_current_score(conn, wallet: str) -> float:
    """Get current trust score for wallet, default 50."""
    cur = conn.cursor()
    cur.execute("SELECT score FROM trust_scores WHERE wallet = ?", (wallet,))
    row = cur.fetchone()
    if row and row[0] is not None:
        return float(row[0])
    return 50.0


def update_wallet_risk(wallet: str, conn=None) -> dict[str, Any] | None:
    """
    Update risk score for a single wallet.

    1. Recompute cluster link (propagation for this wallet)
    2. Run propagation_engine_v1 (wallet only)
    3. Run reason_aggregator (get reasons from DB)
    4. Dynamic risk Bayesian update
    5. Update trust_scores
    6. Save wallet_history

    Returns dict with new_score, reasons, etc. or None if rate-limited/skipped.
    """
    if not wallet or not wallet.strip():
        return None

    wallet = wallet.strip()
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        if not _check_rate_limit(conn, wallet):
            logger.info("realtime_risk_rate_limited", wallet=wallet[:16] + "...")
            return None

        # TEST_MODE: skip propagation, use dummy risk update
        if TEST_MODE:
            base_score = _get_current_score(conn, wallet)
            dummy_penalty = -5
            final_score = max(0, min(100, int(base_score) + dummy_penalty))
            reason_codes = ["TEST_MODE_DUMMY"]
            risk_level = "medium" if final_score < 70 else "low"
            update_wallet_score(wallet, final_score, risk_level, "{}")
            _save_wallet_history(wallet, final_score, risk_level, reason_codes)
            _record_update(conn, wallet)
            logger.info(
                "realtime_risk",
                wallet=wallet[:16] + "...",
                new_score=final_score,
                reasons=",".join(reason_codes),
            )
            print(f"[realtime_risk] wallet={wallet[:16]}... new_score={final_score} reasons={','.join(reason_codes)}")
            return {"wallet": wallet, "new_score": final_score, "reasons": reason_codes}

        # 1. Recompute cluster link + 2. Run propagation for this wallet only
        from backend_blockid.tools.propagation_engine_v1 import update_propagation_for_wallets

        update_propagation_for_wallets(conn, {wallet}, days_back=30)

        # 3. Get reasons (reason_aggregator logic: read from wallet_reasons)
        reasons = get_wallet_reasons(wallet)
        if not reasons:
            reasons = [{"code": "NO_RISK_DETECTED", "weight": 0, "confidence": 1, "days_old": 0}]

        # Deduplicate
        seen = {}
        for r in reasons:
            code = r.get("code") or ""
            if code and code not in seen:
                seen[code] = r
        reasons = list(seen.values())

        base_score = _get_current_score(conn, wallet)

        # 4. Dynamic risk Bayesian update
        prior = get_prior(wallet)
        if prior is None and TEST_MODE:
            prior = 0.05
        prior = prior if prior is not None else 0.05
        posterior = update_scam_probability(prior, reasons)
        reasons_for_log = [
            {"code": r.get("code"), "likelihood": LIKELIHOOD_TABLE.get(r.get("code"), 0.05), "confidence": r.get("confidence", 1)}
            for r in reasons
        ]
        try:
            save_wallet_risk_probability(wallet, prior, posterior, reasons_for_log)
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

        # 5. Update trust_scores
        update_wallet_score(wallet, final_score, risk_level, "{}")

        # 6. Save wallet_history
        _save_wallet_history(wallet, final_score, risk_level, reason_codes)
        _record_update(conn, wallet)

        reasons_str = ",".join(reason_codes) if reason_codes else "NONE"
        logger.info(
            "realtime_risk",
            wallet=wallet[:16] + "...",
            new_score=final_score,
            reasons=reasons_str,
        )
        print(f"[realtime_risk] wallet={wallet[:16]}... new_score={final_score} reasons={reasons_str}")

        return {"wallet": wallet, "new_score": final_score, "reasons": reason_codes}
    finally:
        if own_conn:
            conn.close()


def process_new_transaction(tx: dict[str, Any]) -> None:
    """
    Process a new transaction: update risk for from_wallet and to_wallet.

    tx: dict with keys from_wallet, to_wallet, amount, token (and optionally signature, etc).
    """
    from_wallet = (tx.get("from_wallet") or "").strip()
    to_wallet = (tx.get("to_wallet") or "").strip()

    processed = set()
    for w in (from_wallet, to_wallet):
        if w and w not in processed:
            processed.add(w)
            try:
                update_wallet_risk(w)
            except Exception as e:
                logger.exception("realtime_risk_update_failed", wallet=w[:16] + "...", error=str(e))
