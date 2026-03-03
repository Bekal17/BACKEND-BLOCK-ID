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
from backend_blockid.database.connection import get_connection

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
DAYS_SINCE_CAP = 30  # cap days_since_scan factor


def _get_candidates(test_mode: bool) -> list[str]:
    """Return candidate wallets. TEST_MODE → only test_wallets.csv."""
    wallets: set[str] = set()

    if test_mode and TEST_WALLETS_CSV.exists():
        with open(TEST_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or row.get("address") or "").strip()
                if w:
                    wallets.add(w)
        return sorted(wallets)

    # 1. tracked_wallets
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tracked_wallets'")
        if cur.fetchone():
            cur.execute("SELECT wallet FROM tracked_wallets WHERE wallet IS NOT NULL")
            for r in cur.fetchall():
                w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip() if r else ""
                if w:
                    wallets.add(w)
    except Exception:
        pass

    # 2. CSV sources
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
    conn.close()
    return sorted(wallets)


def _scam_cluster_ids(cur: Any) -> set[int | str]:
    """Cluster IDs that contain at least one scam wallet."""
    cur.execute("""
        SELECT DISTINCT wc.cluster_id FROM wallet_clusters wc
        INNER JOIN scam_wallets sw ON wc.wallet = sw.wallet
    """)
    return {r[0] for r in cur.fetchall() if r[0] is not None}


def _compute_factors(
    conn: Any,
    cur: Any,
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
    followers = 0.0  # no data source; use 0
    days_since = 0.5  # default mid if never scanned
    reasons: list[str] = []

    now = int(time.time())
    cutoff_recent = now - RECENT_SEC

    # 1. Recent suspicious activity (wallet_reasons with HIGH_RISK, created_at recent)
    cur.execute("""
        SELECT 1 FROM wallet_reasons
        WHERE wallet = ? AND reason_code IN ({})
        AND (created_at IS NULL OR created_at >= ?)
        LIMIT 1
    """.format(",".join("?" * len(HIGH_RISK_CODES))), (wallet, *HIGH_RISK_CODES, cutoff_recent))
    if cur.fetchone():
        suspicious = 1.0
        reasons.append("recent_suspicious")

    # 2. Trust score low (<40) → (1 - score/100)
    cur.execute("SELECT score FROM trust_scores WHERE wallet = ?", (wallet,))
    row = cur.fetchone()
    if row:
        score = float(row[0] if hasattr(row, "keys") else row[0] or 50)
        trust_factor = max(0.0, 1.0 - score / 100.0)
        if score < 40:
            reasons.append("low_trust_score")

    # 3. Cluster near scam
    cur.execute("SELECT cluster_id FROM wallet_clusters WHERE wallet = ? LIMIT 1", (wallet,))
    row = cur.fetchone()
    if row:
        cid = row[0] if hasattr(row, "keys") else row[0]
        if cid in scam_clusters:
            cluster_risk = 1.0
            reasons.append("recent_scam_cluster")

    # 4. Tx volume (normalized by max)
    cur.execute("SELECT COUNT(*) FROM transactions WHERE wallet = ?", (wallet,))
    row = cur.fetchone()
    count = int(row[0] if hasattr(row, "keys") else row[0] or 0)
    if max_tx_count > 0:
        tx_volume = min(1.0, count / max_tx_count)
        if count > 100:
            reasons.append("high_tx_volume")

    # 5. Followers — no data, keep 0

    # 6. Days since last scan
    cur.execute(
        "SELECT last_scan_ts FROM wallet_scan_meta WHERE wallet = ?",
        (wallet,),
    )
    row = cur.fetchone()
    if row:
        last = int(row[0] if hasattr(row, "keys") else row[0] or 0)
        days = (now - last) // 86400 if last else DAYS_SINCE_CAP
        days_since = min(1.0, days / DAYS_SINCE_CAP)
        if days > 7:
            reasons.append("not_scanned_recently")
    else:
        days_since = 1.0  # never scanned = highest priority
        reasons.append("never_scanned")

    top_reason = reasons[0] if reasons else "default"
    return suspicious, trust_factor, cluster_risk, tx_volume, followers, days_since, top_reason


def get_prioritized_wallets(
    max_wallets: int | None = None,
    test_mode: bool = False,
) -> list[str]:
    wallets: list[str] = []

    wallets += get_scam_wallets(limit=50000)
    wallets += get_new_wallets(limit=50000)
    wallets += get_active_wallets(limit=50000)
    wallets += get_old_wallets(limit=50000)

    # remove duplicates keep order
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


def get_prioritized_wallets_with_scores(
    max_wallets: int | None = None,
    test_mode: bool | None = None,
) -> list[tuple[str, float, str]]:
    """Return [(wallet, score, reason), ...] for top N wallets (for CLI display)."""
    if test_mode is None:
        test_mode = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
    n = max_wallets or MAX_WALLETS_PER_RUN

    candidates = _get_candidates(test_mode)
    if not candidates:
        return []

    conn = get_connection()
    cur = conn.cursor()
    scam_clusters = _scam_cluster_ids(cur)
    cur.execute("SELECT MAX(c) FROM (SELECT COUNT(*) AS c FROM transactions GROUP BY wallet)")
    row = cur.fetchone()
    max_tx_count = max(1, int(row[0] if row and row[0] is not None else 1))

    scored: list[tuple[str, float, str]] = []
    for w in candidates:
        try:
            sus, trust, cluster, tx, follow, days, reason = _compute_factors(
                conn, cur, w, scam_clusters, max_tx_count
            )
            priority = (
                0.3 * sus + 0.2 * trust + 0.2 * cluster
                + 0.1 * tx + 0.1 * follow + 0.1 * days
            )
            priority = max(0.0, min(1.0, priority))
            scored.append((w, priority, reason))
        except Exception:
            scored.append((w, 0.5, "error"))
    conn.close()

    scored.sort(key=lambda x: -x[1])
    return scored[:n]


def update_scan_timestamp(wallet: str) -> None:
    """Record that wallet was scanned (call after successful Helius fetch)."""
    ts = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO wallet_scan_meta (wallet, last_scan_ts) VALUES (?, ?)",
        (wallet, ts),
    )
    conn.commit()
    conn.close()
