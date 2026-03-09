"""
Score Decay Engine for BlockID — wallet trust score recovery over time.

Wallets recover score when no suspicious activity for 30/60/90 days.
Max recovery capped at 80 unless verified clean history.

Future upgrades:
- Exponential decay model
- Behavior-based recovery
- Manual whitelist bonus
- Bayesian decay
"""

from __future__ import annotations

import asyncio
import csv
import json
import os
import time
from pathlib import Path

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.database.repositories import update_wallet_score

logger = get_logger(__name__)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"

DECAY_TABLE = [
    (90, 20),
    (60, 10),
    (30, 5),
]
MAX_RECOVERY_CAP = 80

SUSPICIOUS_REASON_CODES = frozenset({
    "SCAM_CLUSTER_MEMBER",
    "SCAM_CLUSTER_MEMBER_SMALL",
    "SCAM_CLUSTER_MEMBER_LARGE",
    "DRAINER_INTERACTION",
    "DRAINER_FLOW",
    "DRAINER_FLOW_DETECTED",
    "MEGA_DRAINER",
    "RUG_PULL_DEPLOYER",
    "BLACKLISTED_CREATOR",
    "HIGH_RISK_TOKEN_INTERACTION",
})

VERIFIED_CLEAN_CODES = frozenset({"CLEAN_HISTORY", "NO_RISK_DETECTED"})

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"
_SCAM_PROGRAMS_PATH = Path(__file__).resolve().parent.parent / "oracle" / "scam_programs.json"


async def _load_scam_wallets(conn) -> set[str]:
    scams: set[str] = set()
    try:
        table_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='scam_wallets')"
        )
        if table_exists:
            rows = await conn.fetch("SELECT wallet FROM scam_wallets")
            for r in rows:
                w = (r["wallet"] or "").strip() if r else ""
                if w:
                    scams.add(w)
    except Exception:
        pass
    if not scams and SCAM_WALLETS_CSV.exists():
        with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or "").strip()
                if w:
                    scams.add(w)
    return scams


async def _load_flagged_cluster(conn) -> set[str]:
    """Wallets with SCAM_CLUSTER_MEMBER or similar in wallet_reasons."""
    flagged: set[str] = set()
    try:
        rows = await conn.fetch(
            """
            SELECT DISTINCT wallet FROM wallet_reasons
            WHERE reason_code IN ('SCAM_CLUSTER_MEMBER','SCAM_CLUSTER_MEMBER_SMALL','SCAM_CLUSTER_MEMBER_LARGE')
            AND wallet IS NOT NULL
            """
        )
        for r in rows:
            w = (r["wallet"] or "").strip() if r else ""
            if w:
                flagged.add(w)
    except Exception:
        pass
    return flagged


def _load_drainer_program_ids() -> set[str]:
    path = os.getenv("SCAM_PROGRAMS_PATH", "").strip() or str(_SCAM_PROGRAMS_PATH)
    p = Path(path)
    if not p.is_file():
        return set()
    try:
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
        return {str(x).strip() for x in (data if isinstance(data, list) else []) if x}
    except Exception:
        return set()


async def _last_suspicious_timestamp(conn, wallet: str, scam_wallets: set[str], flagged: set[str], drainer_pids: set[str]) -> int | None:
    """
    Find timestamp of last suspicious tx for wallet.
    Suspicious = tx with scam_wallet counterparty, flagged cluster counterparty, or drainer program_id.
    """
    suspicious = scam_wallets | flagged

    cols_row = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
    )
    cols = {r["column_name"] for r in cols_row}
    if "from_wallet" not in cols or "to_wallet" not in cols or "timestamp" not in cols:
        return await _last_suspicious_from_reasons(conn, wallet)

    or_parts = []
    params: list = [wallet, wallet]
    param_idx = 3

    if suspicious:
        sus_list = list(suspicious)[:500]
        placeholders = ",".join(f"${param_idx + i}" for i in range(len(sus_list)))
        or_parts.append(f"(from_wallet IN ({placeholders}) OR to_wallet IN ({placeholders}))")
        params.extend(sus_list)
        params.extend(sus_list)
        param_idx += len(sus_list) * 2

    if drainer_pids:
        pids_list = list(drainer_pids)[:100]
        ph = ",".join(f"${param_idx + i}" for i in range(len(pids_list)))
        or_parts.append(f"(program_id IS NOT NULL AND program_id != '' AND program_id IN ({ph}))")
        params.extend(pids_list)
        param_idx += len(pids_list)

    if not or_parts:
        return await _last_suspicious_from_reasons(conn, wallet)

    condition = " OR ".join(or_parts)

    sql = f"SELECT MAX(timestamp) as max_ts FROM transactions WHERE (from_wallet = $1 OR to_wallet = $2) AND ({condition})"
    row = await conn.fetchrow(sql, *params)
    ts = row["max_ts"] if row and row["max_ts"] is not None else None
    if ts is not None:
        return int(ts)
    return await _last_suspicious_from_reasons(conn, wallet)


async def _last_suspicious_from_reasons(conn, wallet: str) -> int | None:
    """Fallback: use most recent created_at from suspicious wallet_reasons."""
    try:
        codes_list = list(SUSPICIOUS_REASON_CODES)
        placeholders = ",".join(f"${i+2}" for i in range(len(codes_list)))
        row = await conn.fetchrow(
            f"""
            SELECT MAX(created_at) as max_ts FROM wallet_reasons
            WHERE wallet = $1 AND reason_code IN ({placeholders})
            """,
            wallet, *codes_list,
        )
        return int(row["max_ts"]) if row and row["max_ts"] is not None else None
    except Exception:
        return None


async def _has_verified_clean_history(conn, wallet: str) -> bool:
    """True if wallet has only CLEAN_HISTORY or NO_RISK_DETECTED (no negative reasons)."""
    rows = await conn.fetch(
        "SELECT reason_code FROM wallet_reasons WHERE wallet = $1",
        wallet,
    )
    codes = {r["reason_code"] for r in rows if r}
    if not codes:
        return False
    return codes.issubset(VERIFIED_CLEAN_CODES)


def _get_decay_bonus(days_clean: float, current_score: int) -> int:
    """
    Return score bonus from decay table.
    TEST_MODE: fixed +5.
    """
    if TEST_MODE:
        return 5
    bonus = 0
    for threshold_days, add in DECAY_TABLE:
        if days_clean >= threshold_days:
            bonus = add
            break
    return bonus


async def apply_score_decay_async(wallet: str, conn=None) -> dict | None:
    """
    Apply score decay recovery for a single wallet.

    1. Find last suspicious tx timestamp
    2. Compute days since last scam interaction
    3. Increase score based on decay table
    4. Clamp 0–100 (max 80 unless verified clean)
    5. Save to trust_scores
    6. Save wallet_history

    Returns dict with old, new, days_clean or None if skipped.
    """
    if not wallet or not wallet.strip():
        return None
    wallet = wallet.strip()

    own_conn = conn is None
    if own_conn:
        conn = await get_conn()

    try:
        scam_wallets = await _load_scam_wallets(conn)
        flagged = await _load_flagged_cluster(conn)
        drainer_pids = _load_drainer_program_ids()

        last_ts = await _last_suspicious_timestamp(conn, wallet, scam_wallets, flagged, drainer_pids)
        if last_ts is None:
            return None
        now = int(time.time())
        days_clean = (now - last_ts) / 86400.0

        row = await conn.fetchrow("SELECT score FROM trust_scores WHERE wallet = $1", wallet)
        if row is None:
            return None
        current_score = int(row["score"] or 50)

        bonus = _get_decay_bonus(days_clean, current_score)
        if bonus <= 0:
            return None

        new_score = current_score + bonus
        cap = 100 if await _has_verified_clean_history(conn, wallet) else MAX_RECOVERY_CAP
        new_score = max(0, min(cap, new_score))

        if new_score <= current_score:
            return None

        risk_level = "low" if new_score >= 70 else ("medium" if new_score >= 40 else "high")
        await update_wallet_score(wallet, new_score, risk_level, "{}")

        await conn.execute(
            """
            INSERT INTO wallet_history (wallet, score, risk_level, reason_codes, snapshot_at)
            VALUES ($1, $2, $3, $4, $5)
            """,
            wallet, new_score, risk_level, json.dumps(["SCORE_DECAY_RECOVERY"]), now,
        )

        msg = f"[score_decay] wallet={wallet[:16]}... old={current_score} new={new_score} days_clean={int(days_clean)}"
        logger.info("score_decay", wallet=wallet[:16], old=current_score, new=new_score, days_clean=int(days_clean))
        print(msg)

        return {"wallet": wallet, "old": current_score, "new": new_score, "days_clean": int(days_clean)}
    finally:
        if own_conn:
            await release_conn(conn)


def apply_score_decay(wallet: str, conn=None) -> dict | None:
    """Sync wrapper for apply_score_decay_async."""
    return asyncio.get_event_loop().run_until_complete(apply_score_decay_async(wallet, conn))


async def run_decay_for_all_wallets_async(conn=None) -> int:
    """
    Run apply_score_decay for all wallets in trust_scores.
    Call before batch_publish (daily).
    Returns count of wallets updated.
    """
    own_conn = conn is None
    if own_conn:
        conn = await get_conn()

    try:
        rows = await conn.fetch("SELECT DISTINCT wallet FROM trust_scores WHERE wallet IS NOT NULL")
        wallets = [r["wallet"] for r in rows if r]
        updated = 0
        for w in wallets:
            if w and not w.startswith("TEST_"):
                result = await apply_score_decay_async(w, conn)
                if result:
                    updated += 1
        return updated
    finally:
        if own_conn:
            await release_conn(conn)


def run_decay_for_all_wallets(conn=None) -> int:
    """Sync wrapper for run_decay_for_all_wallets_async."""
    return asyncio.get_event_loop().run_until_complete(run_decay_for_all_wallets_async(conn))
