"""
Build wallet interaction graph from transactions and save cluster assignments.

Reads transactions, builds networkx graph, clusters via connected components,
persists to wallet_graph_clusters. Run once per day (not every minute).

Usage:
    py -m backend_blockid.tools.build_wallet_graph
    py -m backend_blockid.tools.build_wallet_graph --days-back 30 --min-amount 0.001
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import networkx as nx

from backend_blockid.database.pg_connection import get_conn, release_conn


async def _get_table_columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name=$1",
        table,
    )
    return {r["column_name"] for r in rows}


async def main_async() -> int:
    ap = argparse.ArgumentParser(description="Build wallet graph and clusters from transactions.")
    ap.add_argument("--days-back", type=int, default=30, help="Include txs within last N days")
    ap.add_argument("--min-amount", type=float, default=0.001, help="Skip transfers below this SOL")
    args = ap.parse_args()

    conn = await get_conn()
    try:
        cols = await _get_table_columns(conn, "transactions")

        cutoff_ts = int(time.time()) - (args.days_back * 86400)

        if "from_wallet" in cols and "to_wallet" in cols:
            rows = await conn.fetch(
                """
                SELECT from_wallet, to_wallet, amount
                FROM transactions
                WHERE timestamp >= $1 AND from_wallet IS NOT NULL AND to_wallet IS NOT NULL
                """,
                cutoff_ts,
            )
        else:
            rows = await conn.fetch(
                """
                SELECT sender AS from_wallet, receiver AS to_wallet,
                       amount_lamports / 1e9 AS amount
                FROM transactions
                WHERE timestamp >= $1 AND sender IS NOT NULL AND receiver IS NOT NULL
                """,
                cutoff_ts,
            )
    finally:
        await release_conn(conn)

    G = nx.Graph()
    for r in rows:
        amt = float(r["amount"] or 0)
        if amt < args.min_amount:
            continue
        frm = (r["from_wallet"] or "").strip()
        to = (r["to_wallet"] or "").strip()
        if not frm or not to or frm == to:
            continue
        if G.has_edge(frm, to):
            G[frm][to]["weight"] += amt
        else:
            G.add_edge(frm, to, weight=amt)

    wallets = list(G.nodes())
    edges = G.number_of_edges()
    clusters_list = list(nx.connected_components(G))
    num_clusters = len(clusters_list)

    interaction_count = {w: 0 for w in wallets}
    total_volume = {w: 0.0 for w in wallets}
    for u, v, d in G.edges(data=True):
        w_amt = d.get("weight", 0)
        interaction_count[u] += 1
        interaction_count[v] += 1
        total_volume[u] += w_amt
        total_volume[v] += w_amt

    conn = await get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_graph_clusters (
                wallet TEXT PRIMARY KEY,
                cluster_id INTEGER,
                interaction_count INTEGER,
                total_volume DOUBLE PRECISION
            )
        """)

        cluster_id = 0
        for comp in clusters_list:
            cluster_id += 1
            for w in comp:
                await conn.execute(
                    """
                    INSERT INTO wallet_graph_clusters
                    (wallet, cluster_id, interaction_count, total_volume)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT(wallet) DO UPDATE SET
                        cluster_id = EXCLUDED.cluster_id,
                        interaction_count = EXCLUDED.interaction_count,
                        total_volume = EXCLUDED.total_volume
                    """,
                    w, cluster_id, interaction_count.get(w, 0), total_volume.get(w, 0.0),
                )
    finally:
        await release_conn(conn)

    print(f"[graph] wallets={len(wallets)} edges={edges} clusters={num_clusters}")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
