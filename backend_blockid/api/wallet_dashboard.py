"""
Wallet dashboard API — single endpoint for BlockID Wallet Intelligence Dashboard.

GET /wallet/{wallet}/dashboard
Powers the entire dashboard with one request. For new wallets, triggers realtime pipeline.
"""

from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException

from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.oracle.realtime_wallet_pipeline import run_realtime_wallet_pipeline

router = APIRouter()


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _detect_tx_columns(conn) -> tuple[str, str, str]:
    """Return (from_col, to_col, amount_col) for transactions table."""
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
    )
    cols = {r["column_name"] for r in rows}
    if "from_wallet" in cols and "to_wallet" in cols:
        from_col, to_col = "from_wallet", "to_wallet"
    else:
        from_col, to_col = "sender", "receiver"
    amount_col = "amount" if "amount" in cols else "amount_lamports"
    return from_col, to_col, amount_col


@router.get("/wallet/{wallet}/needs-refresh")
async def check_needs_refresh(wallet: str) -> dict:
    """
    Check if wallet needs pipeline re-run.
    Returns {cached: bool, needs_refresh: bool, last_updated: int}
    - wallet not in DB → cached: False, needs_refresh: True
    - wallet in DB, no new tx since last_updated → needs_refresh: False
    - wallet in DB, has new tx → needs_refresh: True
    """
    wallet = wallet.strip()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT score, last_updated FROM trust_scores WHERE wallet = $1",
            wallet,
        )
        if row is None:
            return {"cached": False, "needs_refresh": True, "last_updated": 0}

        last_updated = int(row.get("last_updated") or 0)
        from_col, to_col, _ = await _detect_tx_columns(conn)
        new_tx = await conn.fetchval(
            f"SELECT COUNT(*) FROM transactions WHERE ({from_col}=$1 OR {to_col}=$2) AND timestamp > $3",
            wallet, wallet, last_updated,
        )
        has_new_tx = int(new_tx or 0) > 0
        return {
            "cached": True,
            "needs_refresh": has_new_tx,
            "last_updated": last_updated,
        }
    finally:
        await release_conn(conn)


@router.get("/wallet/{wallet}/dashboard")
async def get_wallet_dashboard(wallet: str) -> dict:
    """
    Return full dashboard data for a wallet. Triggers realtime pipeline if wallet is new.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT score, risk_level FROM trust_scores WHERE wallet=$1 ORDER BY computed_at DESC LIMIT 1",
            wallet,
        )
        if row is None:
            try:
                await run_realtime_wallet_pipeline(wallet)
            except Exception:
                pass

            row = await conn.fetchrow(
                "SELECT score, risk_level FROM trust_scores WHERE wallet=$1 ORDER BY computed_at DESC LIMIT 1",
                wallet,
            )

        trust_score = 0
        risk_tier = "unknown"
        if row is not None:
            trust_score = int(round(float(row.get("score") or 0)))
            risk_tier = str(row.get("risk_level") or "unknown").strip()

        wallet_age_days = 0
        if await _table_exists(conn, "wallet_profiles"):
            r = await conn.fetchrow(
                "SELECT first_seen_at FROM wallet_profiles WHERE wallet=$1", wallet
            )
            if r and r.get("first_seen_at") is not None:
                first_ts = int(r["first_seen_at"])
                wallet_age_days = max(0, (int(time.time()) - first_ts) // 86400)

        if wallet_age_days == 0 and await _table_exists(conn, "transactions"):
            from_col, to_col, _ = await _detect_tx_columns(conn)
            r = await conn.fetchrow(
                f"SELECT MIN(timestamp) AS min_ts FROM transactions WHERE ({from_col}=$1 OR {to_col}=$2) AND timestamp IS NOT NULL",
                wallet, wallet,
            )
            if r and r.get("min_ts") is not None:
                first_ts = int(r["min_ts"])
                wallet_age_days = max(0, (int(time.time()) - first_ts) // 86400)

        total_transactions = 0
        unique_counterparties = 0
        volume_30d = 0.0
        if await _table_exists(conn, "transactions"):
            from_col, to_col, amount_col = await _detect_tx_columns(conn)
            r = await conn.fetchrow(
                f"SELECT COUNT(*) AS cnt FROM transactions WHERE {from_col}=$1 OR {to_col}=$2",
                wallet, wallet,
            )
            total_transactions = int(r["cnt"] or 0) if r else 0

            r = await conn.fetchrow(
                f"""
                SELECT COUNT(DISTINCT counterparty)::int AS cnt FROM (
                    SELECT {to_col} AS counterparty FROM transactions WHERE {from_col}=$1
                    UNION
                    SELECT {from_col} AS counterparty FROM transactions WHERE {to_col}=$2
                ) sub
                WHERE counterparty IS NOT NULL AND counterparty != $3
                """,
                wallet, wallet, wallet,
            )
            unique_counterparties = int(r["cnt"] or 0) if r else 0

            cutoff = int(time.time()) - (30 * 86400)
            r = await conn.fetchrow(
                f"SELECT COALESCE(SUM({amount_col}), 0) AS vol FROM transactions WHERE ({from_col}=$1 OR {to_col}=$2) AND (timestamp IS NULL OR timestamp >= $3)",
                wallet, wallet, cutoff,
            )
            vol = r["vol"] if r and r.get("vol") is not None else 0
            volume_30d = float(vol) if vol else 0.0
            if amount_col == "amount_lamports":
                volume_30d = volume_30d / 1e9

        reasons: list[str] = []
        if await _table_exists(conn, "wallet_reasons"):
            rows = await conn.fetch(
                "SELECT reason_code FROM wallet_reasons WHERE wallet=$1 ORDER BY created_at DESC NULLS LAST LIMIT 20",
                wallet,
            )
            for r in rows:
                code = (r.get("reason_code") or "").strip()
                if code and code not in reasons:
                    reasons.append(code)

        risk_exposure = {
            "scam_cluster": 0,
            "high_risk_counterparties": 0,
            "suspicious_tokens": 0,
            "drainer_interaction": 0,
            "mixer_exposure": 0,
            "wash_trading": 0,
        }
        if await _table_exists(conn, "wallet_risk_probabilities"):
            cols = await conn.fetch(
                "SELECT column_name FROM information_schema.columns WHERE table_name='wallet_risk_probabilities'"
            )
            col_set = {r["column_name"] for r in cols}
            for key in risk_exposure:
                if key in col_set:
                    r = await conn.fetchrow(
                        f"SELECT COALESCE(SUM({key}), 0) AS s FROM wallet_risk_probabilities WHERE wallet=$1",
                        wallet,
                    )
                    risk_exposure[key] = int(r["s"] or 0) if r else 0

        behavior = {
            "long_term_holder": wallet_age_days > 90,
            "low_risk_network": risk_tier in ("LOW", "low", "1") or trust_score >= 70,
            "drainer_pattern": any("DRAINER" in c for c in reasons),
        }

        activity: list[dict] = []
        if await _table_exists(conn, "transactions"):
            from_col, to_col, _ = await _detect_tx_columns(conn)
            rows = await conn.fetch(
                f"""
                SELECT to_char(to_timestamp(timestamp), 'YYYY-MM-DD') AS d, COUNT(*)::int AS c
                FROM transactions
                WHERE ({from_col}=$1 OR {to_col}=$2) AND timestamp >= $3
                GROUP BY d
                ORDER BY d ASC
                """,
                wallet, wallet, int(time.time()) - (30 * 86400),
            )
            for r in rows:
                if r and r.get("d") and r.get("c") is not None:
                    activity.append({"date": str(r["d"]), "tx": int(r["c"])})

        badges: list[str] = []
        if await _table_exists(conn, "wallet_badges"):
            rows = await conn.fetch(
                "SELECT badge FROM wallet_badges WHERE wallet=$1 ORDER BY timestamp DESC LIMIT 20",
                wallet,
            )
            for r in rows:
                b = (r.get("badge") or "").strip()
                if b and b not in badges:
                    badges.append(b)

        return {
            "wallet": wallet,
            "trust_score": trust_score,
            "risk_tier": risk_tier,
            "profile": {
                "wallet_age_days": wallet_age_days,
                "total_transactions": total_transactions,
                "unique_counterparties": unique_counterparties,
                "volume_30d": round(volume_30d, 2),
            },
            "behavior": behavior,
            "alerts": [],
            "reasons": reasons,
            "risk_exposure": risk_exposure,
            "activity": activity,
            "badges": badges,
        }
    finally:
        await release_conn(conn)
