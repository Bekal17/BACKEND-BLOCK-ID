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
import sys
import time
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import networkx as nx

from backend_blockid.database.connection import get_connection


def main() -> int:
    ap = argparse.ArgumentParser(description="Build wallet graph and clusters from transactions.")
    ap.add_argument("--days-back", type=int, default=30, help="Include txs within last N days")
    ap.add_argument("--min-amount", type=float, default=0.001, help="Skip transfers below this SOL")
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    # Schema detection: new (from_wallet/to_wallet/amount) vs old (sender/receiver/amount_lamports)
    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}

    cutoff_ts = int(time.time()) - (args.days_back * 86400)

    def _amt(r):
        return float(r["amount"] or 0) if hasattr(r, "keys") else float(r[2] or 0)

    if "from_wallet" in cols and "to_wallet" in cols:
        cur.execute(
            """
            SELECT from_wallet, to_wallet, amount
            FROM transactions
            WHERE timestamp >= ? AND from_wallet IS NOT NULL AND to_wallet IS NOT NULL
            """,
            (cutoff_ts,),
        )
    else:
        cur.execute(
            """
            SELECT sender AS from_wallet, receiver AS to_wallet,
                   amount_lamports / 1e9 AS amount
            FROM transactions
            WHERE timestamp >= ? AND sender IS NOT NULL AND receiver IS NOT NULL
            """,
            (cutoff_ts,),
        )

    rows = cur.fetchall()
    conn.close()

    # Build graph
    G = nx.Graph()
    for r in rows:
        amt = _amt(r)
        if amt < args.min_amount:
            continue
        frm = (r["from_wallet"] if hasattr(r, "keys") else r[0]).strip()
        to = (r["to_wallet"] if hasattr(r, "keys") else r[1]).strip()
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

    # Per-wallet stats
    interaction_count = {w: 0 for w in wallets}
    total_volume = {w: 0.0 for w in wallets}
    for u, v, d in G.edges(data=True):
        w_amt = d.get("weight", 0)
        interaction_count[u] += 1
        interaction_count[v] += 1
        total_volume[u] += w_amt
        total_volume[v] += w_amt

    # Assign cluster_id sequentially and persist
    conn = get_connection()
    cur = conn.cursor()

    cluster_id = 0
    for comp in clusters_list:
        cluster_id += 1
        for w in comp:
            cur.execute(
                """
                INSERT OR REPLACE INTO wallet_graph_clusters
                (wallet, cluster_id, interaction_count, total_volume)
                VALUES (?, ?, ?, ?)
                """,
                (w, cluster_id, interaction_count.get(w, 0), total_volume.get(w, 0.0)),
            )

    conn.commit()
    conn.close()

    print(f"[graph] wallets={len(wallets)} edges={edges} clusters={num_clusters}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
