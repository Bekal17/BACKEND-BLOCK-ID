from __future__ import annotations

import csv
import time
from pathlib import Path
from typing import Iterable

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

PRIORITY = {
    "SCAM": 100,
    "WATCHLIST": 90,
    "NEW_WALLET": 80,
    "CLUSTER_1HOP": 70,
    "CLUSTER_2HOP": 60,
    "ACTIVE": 50,
    "NORMAL": 10,
}

SCAM_WALLETS_CSV = Path(__file__).resolve().parents[1] / "data" / "scam_wallets.csv"


async def _ensure_tables() -> None:
    conn = await get_conn()
    try:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS priority_wallets (
                wallet TEXT PRIMARY KEY,
                priority INTEGER,
                reason TEXT,
                hop_distance INTEGER,
                last_checked INTEGER,
                last_tx_time INTEGER,
                tx_count INTEGER DEFAULT 0
            )
            """
        )
        # Ensure columns exist (for older schemas)
        try:
            await conn.execute("ALTER TABLE priority_wallets ADD COLUMN last_tx_time INTEGER")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE priority_wallets ADD COLUMN tx_count INTEGER DEFAULT 0")
        except Exception:
            pass
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_priority_wallets_priority ON priority_wallets(priority DESC)"
        )
    finally:
        await release_conn(conn)


def _normalize_wallets(rows: Iterable[dict]) -> list[str]:
    wallets: list[str] = []
    for row in rows:
        w = (row.get("wallet") or row.get("address") or "").strip()
        if w:
            wallets.append(w)
    return wallets


async def add_wallet(wallet: str, reason: str, hop: int = 0) -> None:
    await _ensure_tables()
    priority = PRIORITY.get(reason, PRIORITY["NORMAL"])
    now = int(time.time())
    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO priority_wallets(wallet, priority, reason, hop_distance, last_checked)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT(wallet) DO UPDATE SET
                priority = CASE
                    WHEN EXCLUDED.priority > priority_wallets.priority THEN EXCLUDED.priority
                    ELSE priority_wallets.priority
                END,
                reason = CASE
                    WHEN EXCLUDED.priority > priority_wallets.priority THEN EXCLUDED.reason
                    ELSE priority_wallets.reason
                END,
                hop_distance = CASE
                    WHEN EXCLUDED.priority > priority_wallets.priority THEN EXCLUDED.hop_distance
                    ELSE priority_wallets.hop_distance
                END,
                last_checked = EXCLUDED.last_checked
            """,
            wallet, priority, reason, hop, now,
        )
    finally:
        await release_conn(conn)


async def get_priority_wallets(limit: int = 100) -> list[str]:
    await _ensure_tables()
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT wallet FROM priority_wallets
            ORDER BY priority DESC, last_checked ASC
            LIMIT $1
            """,
            limit,
        )
        wallets = [str(r["wallet"]) for r in rows if r and r.get("wallet")]
        now = int(time.time())
        if wallets:
            for w in wallets:
                await conn.execute(
                    "UPDATE priority_wallets SET last_checked = $1 WHERE wallet = $2",
                    now, w,
                )
        return wallets
    finally:
        await release_conn(conn)


async def update_priority(wallet: str, delta: int) -> None:
    await _ensure_tables()
    conn = await get_conn()
    try:
        now = int(time.time())
        await conn.execute(
            """
            UPDATE priority_wallets
            SET priority = COALESCE(priority, 0) + $1, last_checked = $2
            WHERE wallet = $3
            """,
            int(delta), now, wallet,
        )
    finally:
        await release_conn(conn)


async def remove_old_wallets(days: int = 30) -> None:
    await _ensure_tables()
    cutoff = int(time.time()) - (days * 86400)
    conn = await get_conn()
    try:
        await conn.execute(
            "DELETE FROM priority_wallets WHERE last_checked IS NOT NULL AND last_checked < $1",
            cutoff,
        )
    finally:
        await release_conn(conn)


async def age_priorities() -> int:
    """
    Reduce priority over time based on days since last_checked.
    priority -= days * 2, and an extra -20 if inactive > 30 days.
    Clamp priority 0–100.
    """
    await _ensure_tables()
    now = int(time.time())
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT wallet, priority, last_checked FROM priority_wallets")
        updated = 0
        for row in rows:
            wallet = row["wallet"]
            priority = int(row["priority"] or 0)
            last_checked = int(row["last_checked"] or 0)
            days_since = (now - last_checked) // 86400 if last_checked else 0
            new_priority = priority - (days_since * 2)
            if days_since > 30:
                new_priority -= 20
            new_priority = max(0, min(100, new_priority))
            if new_priority != priority:
                await conn.execute(
                    "UPDATE priority_wallets SET priority = $1 WHERE wallet = $2",
                    new_priority, wallet,
                )
                updated += 1
        return updated
    finally:
        await release_conn(conn)


async def boost_active_wallets() -> int:
    """
    Boost priority based on tx count in the last 24h.
    priority += tx_count * 2, clamp to 100.
    """
    await _ensure_tables()
    now = int(time.time())
    cutoff = now - 86400
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT wallet FROM priority_wallets")
        wallets = [r["wallet"] for r in rows if r and r.get("wallet")]
        boosted = 0
        for wallet in wallets:
            row = await conn.fetchrow(
                """
                SELECT COUNT(*) AS cnt, MAX(timestamp) AS max_ts
                FROM transactions
                WHERE (sender = $1 OR receiver = $1)
                  AND timestamp > $2
                """,
                wallet, cutoff,
            )
            tx_count = int(row["cnt"] if row and row.get("cnt") is not None else 0)
            last_tx_time = int(row["max_ts"] if row and row.get("max_ts") is not None else 0)
            if tx_count <= 0:
                continue
            p_row = await conn.fetchrow(
                "SELECT priority FROM priority_wallets WHERE wallet = $1", wallet
            )
            priority = int(p_row["priority"] if p_row and p_row.get("priority") is not None else 0)
            new_priority = min(100, priority + (tx_count * 2))
            await conn.execute(
                """
                UPDATE priority_wallets
                SET priority = $1, last_tx_time = $2, tx_count = $3
                WHERE wallet = $4
                """,
                new_priority, last_tx_time, tx_count, wallet,
            )
            boosted += 1
        return boosted
    finally:
        await release_conn(conn)


MAX_HELIUS_CALLS_PER_RUN = 200
WALLETS_PER_RUN = 100


async def get_wallets_with_budget(limit: int | None = None) -> list[str]:
    """
    Return priority wallets constrained by Helius call budget.
    Uses 1 call per wallet estimate, stops when budget reached.
    """
    await _ensure_tables()
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT wallet FROM priority_wallets
            ORDER BY priority DESC, last_checked ASC
            """
        )
        wallets = [str(r["wallet"]) for r in rows if r and r.get("wallet")]
    finally:
        await release_conn(conn)

    target_limit = limit if limit is not None else WALLETS_PER_RUN
    selected: list[str] = []
    calls = 0
    for w in wallets:
        if calls >= MAX_HELIUS_CALLS_PER_RUN or len(selected) >= target_limit:
            break
        selected.append(w)
        calls += 1
    return selected


async def get_cluster_neighbors(wallet: str, max_hop: int = 2) -> dict[str, int]:
    """
    BFS neighbors up to max_hop using transactions table (from_wallet -> to_wallet).
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
            placeholders = ",".join(f"${i+1}" for i in range(len(frontier)))
            params = list(frontier)
            out_rows = await conn.fetch(
                f"SELECT to_wallet FROM transactions WHERE from_wallet IN ({placeholders})",
                *params,
            )
            out_wallets = [r["to_wallet"] for r in out_rows if r and r.get("to_wallet")]
            in_rows = await conn.fetch(
                f"SELECT from_wallet FROM transactions WHERE to_wallet IN ({placeholders})",
                *params,
            )
            in_wallets = [r["from_wallet"] for r in in_rows if r and r.get("from_wallet")]
            next_frontier = set(out_wallets + in_wallets)
            next_frontier.discard(wallet)
            next_frontier -= visited
            for w in next_frontier:
                neighbors[w] = hop
            visited |= next_frontier
            frontier = next_frontier
    except Exception as e:
        logger.warning("priority_wallets_neighbors_error", wallet=wallet[:16], error=str(e))
    finally:
        await release_conn(conn)
    return neighbors


async def populate_priority_wallets() -> None:
    """
    Auto insert wallets into priority list using rules:
    1) scam_wallets.csv -> SCAM
    2) 1-hop neighbors -> CLUSTER_1HOP
    3) 2-hop neighbors -> CLUSTER_2HOP
    4) new wallets (<3 days) -> NEW_WALLET
    5) active wallets (tx_count > 50) -> ACTIVE
    """
    await _ensure_tables()
    # 1) scam wallets
    scam_wallets: list[str] = []
    if SCAM_WALLETS_CSV.exists():
        with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
            scam_wallets = _normalize_wallets(csv.DictReader(f))
    for w in scam_wallets:
        await add_wallet(w, "SCAM", hop=0)

    # 2/3) cluster neighbors (1-hop, 2-hop)
    for w in scam_wallets:
        neighbors = await get_cluster_neighbors(w, max_hop=2)
        for n, hop in neighbors.items():
            if hop == 1:
                await add_wallet(n, "CLUSTER_1HOP", hop=1)
            elif hop == 2:
                await add_wallet(n, "CLUSTER_2HOP", hop=2)

    # 4) new wallets (<3 days)
    now = int(time.time())
    three_days_ago = now - (3 * 86400)
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            """
            SELECT wallet FROM wallet_meta
            WHERE wallet IS NOT NULL
              AND (
                wallet_age_days IS NOT NULL AND wallet_age_days < 3
                OR (first_tx_ts IS NOT NULL AND first_tx_ts >= $1)
              )
            """,
            three_days_ago,
        )
        for row in rows:
            w = (row["wallet"] if row else "") or ""
            w = str(w).strip()
            if w:
                await add_wallet(w, "NEW_WALLET", hop=0)
    except Exception as e:
        logger.warning("priority_wallets_new_wallets_error", error=str(e))

    # 5) active wallets (tx_count > 50)
    try:
        rows = await conn.fetch(
            """
            SELECT wallet FROM (
                SELECT from_wallet AS wallet FROM transactions
                UNION ALL
                SELECT to_wallet AS wallet FROM transactions
            ) AS combined
            WHERE wallet IS NOT NULL
            GROUP BY wallet
            HAVING COUNT(*) > 50
            """
        )
        for row in rows:
            w = (row["wallet"] if row else "") or ""
            w = str(w).strip()
            if w:
                await add_wallet(w, "ACTIVE", hop=0)
    except Exception as e:
        logger.warning("priority_wallets_active_wallets_error", error=str(e))
    finally:
        await release_conn(conn)
