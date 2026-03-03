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

import json
import os
import time
from pathlib import Path

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.config import DB_PATH
from backend_blockid.database.connection import get_connection
from backend_blockid.database.repositories import update_wallet_score

logger = get_logger(__name__)

TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"

# Decay bonus: days_clean -> score increase
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


def _load_scam_wallets(conn) -> set[str]:
    scams: set[str] = set()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scam_wallets'")
        if cur.fetchone():
            cur.execute("SELECT wallet FROM scam_wallets")
            for r in cur.fetchall():
                w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip() if r else ""
                if w:
                    scams.add(w)
    except Exception:
        pass
    if not scams and SCAM_WALLETS_CSV.exists():
        import csv
        with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or "").strip()
                if w:
                    scams.add(w)
    return scams


def _load_flagged_cluster(conn) -> set[str]:
    """Wallets with SCAM_CLUSTER_MEMBER or similar in wallet_reasons."""
    flagged: set[str] = set()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT DISTINCT wallet FROM wallet_reasons
            WHERE reason_code IN ('SCAM_CLUSTER_MEMBER','SCAM_CLUSTER_MEMBER_SMALL','SCAM_CLUSTER_MEMBER_LARGE')
            AND wallet IS NOT NULL
            """
        )
        for r in cur.fetchall():
            w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip() if r else ""
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


def _last_suspicious_timestamp(conn, wallet: str, scam_wallets: set[str], flagged: set[str], drainer_pids: set[str]) -> int | None:
    """
    Find timestamp of last suspicious tx for wallet.
    Suspicious = tx with scam_wallet counterparty, flagged cluster counterparty, or drainer program_id.
    """
    cur = conn.cursor()
    suspicious = scam_wallets | flagged

    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}
    if "from_wallet" not in cols or "to_wallet" not in cols or "timestamp" not in cols:
        return _last_suspicious_from_reasons(conn, wallet)

    # Build query: wallet participated AND (counterparty in suspicious OR program_id in drainer)
    or_parts = []
    params: list = [wallet, wallet]

    if suspicious:
        placeholders = ",".join("?" * min(len(suspicious), 500))
        or_parts.append(f"(from_wallet IN ({placeholders}) OR to_wallet IN ({placeholders}))")
        sus_list = list(suspicious)[:500]
        params.extend(sus_list)
        params.extend(sus_list)

    if drainer_pids:
        ph = ",".join("?" * min(len(drainer_pids), 100))
        or_parts.append(f"(program_id IS NOT NULL AND program_id != '' AND program_id IN ({ph}))")
        params.extend(list(drainer_pids)[:100])

    if not or_parts:
        return _last_suspicious_from_reasons(conn, wallet)

    condition = " OR ".join(or_parts)

    sql = f"SELECT MAX(timestamp) FROM transactions WHERE (from_wallet = ? OR to_wallet = ?) AND ({condition})"
    cur.execute(sql, params)
    row = cur.fetchone()
    ts = row[0] if row and row[0] is not None else None
    if ts is not None:
        return int(ts)
    return _last_suspicious_from_reasons(conn, wallet)


def _last_suspicious_from_reasons(conn, wallet: str) -> int | None:
    """Fallback: use most recent created_at from suspicious wallet_reasons."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT MAX(created_at) FROM wallet_reasons
            WHERE wallet = ? AND reason_code IN ({})
            """.format(",".join("?" * len(SUSPICIOUS_REASON_CODES))),
            [wallet] + list(SUSPICIOUS_REASON_CODES),
        )
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _has_verified_clean_history(conn, wallet: str) -> bool:
    """True if wallet has only CLEAN_HISTORY or NO_RISK_DETECTED (no negative reasons)."""
    cur = conn.cursor()
    cur.execute(
        "SELECT reason_code FROM wallet_reasons WHERE wallet = ?",
        (wallet,),
    )
    codes = {r["reason_code"] if hasattr(r, "keys") else r[0] for r in cur.fetchall() if r}
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


def apply_score_decay(wallet: str, conn=None) -> dict | None:
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
        conn = get_connection()

    try:
        scam_wallets = _load_scam_wallets(conn)
        flagged = _load_flagged_cluster(conn)
        drainer_pids = _load_drainer_program_ids()

        last_ts = _last_suspicious_timestamp(conn, wallet, scam_wallets, flagged, drainer_pids)
        if last_ts is None:
            return None  # No suspicious history — nothing to recover from
        now = int(time.time())
        days_clean = (now - last_ts) / 86400.0

        cur = conn.cursor()
        cur.execute("SELECT score FROM trust_scores WHERE wallet = ?", (wallet,))
        row = cur.fetchone()
        current_score = int(row["score"] if hasattr(row, "keys") and row else row[0] or 50)
        if row is None:
            return None

        bonus = _get_decay_bonus(days_clean, current_score)
        if bonus <= 0:
            return None

        new_score = current_score + bonus
        cap = 100 if _has_verified_clean_history(conn, wallet) else MAX_RECOVERY_CAP
        new_score = max(0, min(cap, new_score))

        if new_score <= current_score:
            return None

        risk_level = "low" if new_score >= 70 else ("medium" if new_score >= 40 else "high")
        update_wallet_score(wallet, new_score, risk_level, "{}")

        # Save wallet_history
        cur.execute(
            """
            INSERT INTO wallet_history (wallet, score, risk_level, reason_codes, snapshot_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (wallet, new_score, risk_level, json.dumps(["SCORE_DECAY_RECOVERY"]), now),
        )
        conn.commit()

        msg = f"[score_decay] wallet={wallet[:16]}... old={current_score} new={new_score} days_clean={int(days_clean)}"
        logger.info("score_decay", wallet=wallet[:16], old=current_score, new=new_score, days_clean=int(days_clean))
        print(msg)

        return {"wallet": wallet, "old": current_score, "new": new_score, "days_clean": int(days_clean)}
    finally:
        if own_conn:
            conn.close()


def run_decay_for_all_wallets(conn=None) -> int:
    """
    Run apply_score_decay for all wallets in trust_scores.
    Call before batch_publish (daily).
    Returns count of wallets updated.
    """
    own_conn = conn is None
    if own_conn:
        conn = get_connection()

    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT wallet FROM trust_scores WHERE wallet IS NOT NULL")
        wallets = [r["wallet"] if hasattr(r, "keys") else r[0] for r in cur.fetchall() if r]
        updated = 0
        for w in wallets:
            if w and not w.startswith("TEST_"):
                result = apply_score_decay(w, conn)
                if result:
                    updated += 1
        return updated
    finally:
        if own_conn:
            conn.close()
