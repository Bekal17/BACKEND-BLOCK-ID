from __future__ import annotations

import csv
import time
from pathlib import Path

from backend_blockid.database.connection import get_connection

SCAM_WALLETS_CSV = Path("backend_blockid/data/scam_wallets.csv")


def _ensure_priority_table() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS priority_wallets (
            wallet TEXT PRIMARY KEY,
            priority INTEGER,
            reason TEXT,
            hop_distance INTEGER,
            last_checked INTEGER
        )
        """
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_priority_wallets_priority ON priority_wallets(priority DESC)"
    )
    conn.commit()
    conn.close()


def _upsert_priority(wallet: str, priority: int, reason: str, hop: int) -> None:
    now = int(time.time())
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO priority_wallets(wallet, priority, reason, hop_distance, last_checked)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            priority = CASE
                WHEN excluded.priority > priority_wallets.priority THEN excluded.priority
                ELSE priority_wallets.priority
            END,
            reason = CASE
                WHEN excluded.priority > priority_wallets.priority THEN excluded.reason
                ELSE priority_wallets.reason
            END,
            hop_distance = CASE
                WHEN excluded.priority > priority_wallets.priority THEN excluded.hop_distance
                ELSE priority_wallets.hop_distance
            END,
            last_checked = excluded.last_checked
        """,
        (wallet, priority, reason, hop, now),
    )
    conn.commit()
    conn.close()


def seed_from_scam_csv() -> list[str]:
    wallets: list[str] = []
    if not SCAM_WALLETS_CSV.exists():
        return wallets
    with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            w = (row.get("wallet") or "").strip()
            if w:
                wallets.append(w)
    for w in wallets:
        _upsert_priority(w, priority=100, reason="SCAM", hop=0)
    return wallets


def get_cluster_neighbors(wallet: str, max_hop: int = 2) -> dict[str, int]:
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


def seed_neighbors(scam_wallets: list[str]) -> int:
    seeded = 0
    for w in scam_wallets:
        neighbors = get_cluster_neighbors(w, max_hop=2)
        for n, hop in neighbors.items():
            if hop == 1:
                _upsert_priority(n, priority=70, reason="CLUSTER_1HOP", hop=1)
            elif hop == 2:
                _upsert_priority(n, priority=60, reason="CLUSTER_2HOP", hop=2)
            seeded += 1
    return seeded


def _count_priority_wallets() -> int:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM priority_wallets")
    row = cur.fetchone()
    conn.close()
    return int(row[0] if row and row[0] is not None else 0)


def main() -> None:
    _ensure_priority_table()
    scam_wallets = seed_from_scam_csv()
    neighbors_seeded = seed_neighbors(scam_wallets)

    print("Scam wallets seeded:", len(scam_wallets))
    print("Neighbors seeded:", neighbors_seeded)
    print("Priority wallets total:", _count_priority_wallets())

def print_priority_summary():
    conn = get_connection()

    total = conn.execute(
        "SELECT COUNT(*) FROM priority_wallets"
    ).fetchone()[0]

    by_reason = conn.execute("""
        SELECT reason, COUNT(*)
        FROM priority_wallets
        GROUP BY reason
        ORDER BY COUNT(*) DESC
    """).fetchall()

    print("\nPriority Wallet Summary")
    print("Total:", total)
    for r in by_reason:
        print(r)

print_priority_summary()

if __name__ == "__main__":
    main()
