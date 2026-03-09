"""
BlockID Smart Wallet Scan Prioritizer.

Decides which wallets to scan first for Helius incremental fetch.
Reduces API cost while keeping high-risk wallets updated.

Priority formula:
  priority = 0.3*suspicious + 0.2*(1-trust/100) + 0.2*cluster_risk
           + 0.1*tx_volume + 0.1*followers + 0.1*days_since_scan

Future upgrades:
  - ML-based prioritization
  - Exchange hot-wallet priority
  - Whale wallet tracking
  - Cross-chain tracking
"""
from __future__ import annotations

import csv
import os
import time
from pathlib import Path
from typing import Any

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn

logger = get_logger(__name__)

MAX_WALLETS_PER_RUN = int(os.getenv("MAX_WALLETS_PER_RUN", "500").strip() or "500")
TEST_WALLETS_CSV = Path(__file__).resolve().parent.parent / "data" / "test_wallets.csv"
WALLET_SOURCES = [
    Path(__file__).resolve().parent.parent / "data" / "wallets.csv",
    Path(__file__).resolve().parent.parent / "data" / "test_wallets.csv",
    Path(__file__).resolve().parent.parent / "data" / "manual_wallets.csv",
]

HIGH_RISK_CODES = frozenset({
    "SCAM_CLUSTER_MEMBER",
    "SCAM_CLUSTER_MEMBER_SMALL",
    "SCAM_CLUSTER_MEMBER_LARGE",
    "RUG_PULL_DEPLOYER",
    "BLACKLISTED_CREATOR",
    "DRAINER_FLOW_DETECTED",
    "DRAINER_FLOW",
    "MEGA_DRAINER",
})
RECENT_DAYS = 7
RECENT_SEC = RECENT_DAYS * 86400
DAYS_SINCE_CAP = 30


async def _get_candidates(test_mode: bool) -> list[str]:
    """Return candidate wallets. TEST_MODE → only test_wallets.csv."""
    wallets: set[str] = set()

    if test_mode and TEST_WALLETS_CSV.exists():
        with open(TEST_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or row.get("address") or "").strip()
                if w:
                    wallets.add(w)
        return sorted(wallets)

    conn = await get_conn()
    try:
        table_exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='tracked_wallets')"
        )
        if table_exists:
            rows = await conn.fetch("SELECT wallet FROM tracked_wallets WHERE wallet IS NOT NULL")
            for r in rows:
                w = (r["wallet"] or "").strip() if r else ""
                if w:
                    wallets.add(w)
    except Exception:
        pass
    finally:
        await release_conn(conn)

    for path in WALLET_SOURCES:
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        w = (row.get("wallet") or row.get("address") or "").strip()
                        if w:
                            wallets.add(w)
            except Exception:
                pass
    return sorted(wallets)


async def _scam_cluster_ids(conn: Any) -> set[int | str]:
    """Cluster IDs that contain at least one scam wallet."""
    rows = await conn.fetch("""
        SELECT DISTINCT wc.cluster_id FROM wallet_clusters wc
        INNER JOIN scam_wallets sw ON wc.wallet = sw.wallet
    """)
    return {r["cluster_id"] for r in rows if r["cluster_id"] is not None}


async def _compute_factors(
    conn: Any,
    wallet: str,
    scam_clusters: set[int | str],
    max_tx_count: int,
) -> tuple[float, float, float, float, float, float, str]:
    """
    Return (suspicious, trust_factor, cluster_risk, tx_volume, followers, days_since, top_reason).
    All factors normalized 0–1.
    """
    suspicious = 0.0
    trust_factor = 0.0
    cluster_risk = 0.0
    tx_volume = 0.0
    followers = 0.0
    days_since = 0.5
    reasons: list[str] = []

    now = int(time.time())
    cutoff_recent = now - RECENT_SEC

    placeholders = ",".join(f"${i+2}" for i in range(len(HIGH_RISK_CODES)))
    params = [wallet] + list(HIGH_RISK_CODES) + [cutoff_recent]
    row = await conn.fetchrow(f"""
        SELECT 1 FROM wallet_reasons
        WHERE wallet = $1 AND reason_code IN ({placeholders})
        AND (created_at IS NULL OR created_at >= ${len(HIGH_RISK_CODES)+2})
        LIMIT 1
    """, *params)
    if row:
        suspicious = 1.0
        reasons.append("recent_suspicious")

    row = await conn.fetchrow("SELECT score FROM trust_scores WHERE wallet = $1", wallet)
    if row:
        score = float(row["score"] or 50)
        trust_factor = max(0.0, 1.0 - score / 100.0)
        if score < 40:
            reasons.append("low_trust_score")

    row = await conn.fetchrow("SELECT cluster_id FROM wallet_clusters WHERE wallet = $1 LIMIT 1", wallet)
    if row:
        cid = row["cluster_id"]
        if cid in scam_clusters:
            cluster_risk = 1.0
            reasons.append("recent_scam_cluster")

    row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM transactions WHERE wallet = $1", wallet)
    count = int(row["cnt"] or 0) if row else 0
    if max_tx_count > 0:
        tx_volume = min(1.0, count / max_tx_count)
        if count > 100:
            reasons.append("high_tx_volume")

    row = await conn.fetchrow(
        "SELECT last_scan_ts FROM wallet_scan_meta WHERE wallet = $1",
        wallet,
    )
    if row:
        last = int(row["last_scan_ts"] or 0)
        days = (now - last) // 86400 if last else DAYS_SINCE_CAP
        days_since = min(1.0, days / DAYS_SINCE_CAP)
        if days > 7:
            reasons.append("not_scanned_recently")
    else:
        days_since = 1.0
        reasons.append("never_scanned")

    top_reason = reasons[0] if reasons else "default"
    return suspicious, trust_factor, cluster_risk, tx_volume, followers, days_since, top_reason


async def get_prioritized_wallets(
    max_wallets: int | None = None,
    test_mode: bool = False,
) -> list[str]:
    wallets: list[str] = []

    wallets += await get_scam_wallets(limit=50000)
    wallets += await get_new_wallets(limit=50000)
    wallets += await get_active_wallets(limit=50000)
    wallets += await get_old_wallets(limit=50000)

    seen: set[str] = set()
    ordered: list[str] = []
    for w in wallets:
        if w not in seen:
            ordered.append(w)
            seen.add(w)

    limit = max_wallets if max_wallets is not None else None
    if test_mode:
        limit = min(limit, 50) if limit is not None else 50
    return ordered[:limit] if limit is not None else ordered


async def get_prioritized_wallets_with_scores(
    max_wallets: int | None = None,
    test_mode: bool | None = None,
) -> list[tuple[str, float, str]]:
    """Return [(wallet, score, reason), ...] for top N wallets (for CLI display)."""
    if test_mode is None:
        test_mode = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
    n = max_wallets or MAX_WALLETS_PER_RUN

    candidates = await _get_candidates(test_mode)
    if not candidates:
        return []

    conn = await get_conn()
    try:
        scam_clusters = await _scam_cluster_ids(conn)
        row = await conn.fetchrow("SELECT MAX(c) as max_c FROM (SELECT COUNT(*) AS c FROM transactions GROUP BY wallet) sub")
        max_tx_count = max(1, int(row["max_c"] if row and row["max_c"] is not None else 1))

        scored: list[tuple[str, float, str]] = []
        for w in candidates:
            try:
                sus, trust, cluster, tx, follow, days, reason = await _compute_factors(
                    conn, w, scam_clusters, max_tx_count
                )
                priority = (
                    0.3 * sus + 0.2 * trust + 0.2 * cluster
                    + 0.1 * tx + 0.1 * follow + 0.1 * days
                )
                priority = max(0.0, min(1.0, priority))
                scored.append((w, priority, reason))
            except Exception:
                scored.append((w, 0.5, "error"))
    finally:
        await release_conn(conn)

    scored.sort(key=lambda x: -x[1])
    return scored[:n]


async def update_scan_timestamp(wallet: str) -> None:
    """Record that wallet was scanned (call after successful Helius fetch)."""
    await _update_scan_timestamp_async(wallet)


async def _update_scan_timestamp_async(wallet: str) -> None:
    """Record that wallet was scanned (call after successful Helius fetch)."""
    ts = int(time.time())
    conn = await get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_scan_meta (
                wallet TEXT PRIMARY KEY,
                last_scan_ts INTEGER
            )
        """)
        await conn.execute(
            """
            INSERT INTO wallet_scan_meta (wallet, last_scan_ts) VALUES ($1, $2)
            ON CONFLICT(wallet) DO UPDATE SET last_scan_ts = EXCLUDED.last_scan_ts
            """,
            wallet, ts,
        )
    finally:
        await release_conn(conn)


async def get_scam_wallets(limit: int = 50000) -> list[str]:
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT wallet FROM scam_wallets LIMIT $1", limit)
        return [r["wallet"] for r in rows if r.get("wallet")]
    except Exception:
        return []
    finally:
        await release_conn(conn)


async def get_new_wallets(limit: int = 50000) -> list[str]:
    conn = await get_conn()
    try:
        cutoff = int(time.time()) - (7 * 86400)
        rows = await conn.fetch(
            "SELECT wallet FROM wallet_meta WHERE first_tx_ts >= $1 LIMIT $2",
            cutoff, limit
        )
        return [r["wallet"] for r in rows if r.get("wallet")]
    except Exception:
        return []
    finally:
        await release_conn(conn)


async def get_active_wallets(limit: int = 50000) -> list[str]:
    conn = await get_conn()
    try:
        cutoff = int(time.time()) - (24 * 3600)
        rows = await conn.fetch(
            "SELECT DISTINCT wallet FROM transactions WHERE timestamp >= $1 LIMIT $2",
            cutoff, limit
        )
        return [r["wallet"] for r in rows if r.get("wallet")]
    except Exception:
        return []
    finally:
        await release_conn(conn)


async def get_old_wallets(limit: int = 50000) -> list[str]:
    conn = await get_conn()
    try:
        cutoff = int(time.time()) - (30 * 86400)
        rows = await conn.fetch(
            "SELECT wallet FROM wallet_scan_meta WHERE last_scan_ts < $1 ORDER BY last_scan_ts ASC LIMIT $2",
            cutoff, limit
        )
        return [r["wallet"] for r in rows if r.get("wallet")]
    except Exception:
        return []
    finally:
        await release_conn(conn)
