"""
BlockID Wallet Cluster Heatmap — visualize interaction graph and trust scores.

Debug tool. Do NOT run in production pipeline.
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import matplotlib.pyplot as plt
import networkx as nx

from backend_blockid.database.connection import get_connection

_CHARTS_DIR = Path(__file__).resolve().parent.parent / "charts"
MIN_AMOUNT = 0.001
LIMIT_TXS = 5000


def _load_cluster_wallets(cur, cluster_id: int) -> set[str]:
    """Load wallets for cluster from wallet_cluster_members, wallet_graph_clusters, or wallet_clusters."""
    wallets: set[str] = set()

    for table, col in [
        ("wallet_cluster_members", "wallet"),
        ("wallet_graph_clusters", "wallet"),
        ("wallet_clusters", "wallet"),
    ]:
        try:
            cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not cur.fetchone():
                continue
            cur.execute(
                f"SELECT {col} FROM {table} WHERE cluster_id = ?",
                (cluster_id,),
            )
            for r in cur.fetchall():
                w = (r[0] if r else "").strip()
                if w:
                    wallets.add(w)
            if wallets:
                return wallets
        except Exception:
            continue
    return wallets


def _load_scam_wallets(cur) -> set[str]:
    scam: set[str] = set()
    try:
        cur.execute("SELECT wallet FROM scam_wallets")
        for r in cur.fetchall():
            w = (r[0] if r else "").strip()
            if w:
                scam.add(w)
    except Exception:
        pass
    return scam


def plot_cluster_heatmap(cluster_id: int) -> Path | None:
    """
    Load cluster wallets, transactions, trust scores; build graph; draw heatmap.
    Returns path to saved image or None.
    """
    conn = get_connection()
    cur = conn.cursor()

    cluster_wallets = _load_cluster_wallets(cur, cluster_id)
    if not cluster_wallets:
        conn.close()
        print(f"[heatmap] cluster={cluster_id} wallets=0 (no cluster data)")
        return None

    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}
    placeholders = ",".join("?" * len(cluster_wallets))

    if "from_wallet" in cols and "to_wallet" in cols:
        cur.execute(
            f"""
            SELECT from_wallet, to_wallet, amount
            FROM transactions
            WHERE from_wallet IN ({placeholders})
              AND to_wallet IN ({placeholders})
              AND amount >= ?
            LIMIT ?
            """,
            (*cluster_wallets, *cluster_wallets, MIN_AMOUNT, LIMIT_TXS),
        )
    else:
        cur.execute(
            f"""
            SELECT sender AS from_wallet, receiver AS to_wallet, amount_lamports / 1e9 AS amount
            FROM transactions
            WHERE sender IN ({placeholders})
              AND receiver IN ({placeholders})
              AND amount_lamports / 1e9 >= ?
            LIMIT ?
            """,
            (*cluster_wallets, *cluster_wallets, MIN_AMOUNT, LIMIT_TXS),
        )

    tx_rows = cur.fetchall()

    cur.execute(
        "SELECT wallet, score FROM trust_scores WHERE wallet IN (" + placeholders + ")",
        tuple(cluster_wallets),
    )
    score_rows = cur.fetchall()
    scores = {r[0]: float(r[1] or 50) for r in score_rows}

    scam_wallets = _load_scam_wallets(cur)
    conn.close()

    G = nx.Graph()
    for r in tx_rows:
        frm = (r[0] if r else "").strip()
        to = (r[1] if r else "").strip()
        amt = float(r[2] or 0)
        if not frm or not to or frm == to or amt < MIN_AMOUNT:
            continue
        w = math.log(amt + 1)
        if G.has_edge(frm, to):
            G[frm][to]["weight"] += w
        else:
            G.add_edge(frm, to, weight=w)

    if G.number_of_nodes() == 0:
        for w in cluster_wallets:
            G.add_node(w)

    node_list = list(G.nodes())
    colors = [scores.get(n, 50) for n in node_list]
    size_map = {n: 100 + scores.get(n, 50) * 5 for n in node_list}

    fig, ax = plt.subplots(figsize=(10, 8))
    pos = nx.spring_layout(G, seed=42)

    nx.draw_networkx_nodes(
        G,
        pos,
        node_color=colors,
        cmap=plt.cm.RdYlGn,
        vmin=0,
        vmax=100,
        node_size=[size_map[n] for n in node_list],
        ax=ax,
    )

    scam_nodes = [n for n in node_list if n in scam_wallets]
    if scam_nodes:
        nx.draw_networkx_nodes(
            G,
            pos,
            nodelist=scam_nodes,
            node_color=[scores.get(n, 50) for n in scam_nodes],
            cmap=plt.cm.RdYlGn,
            vmin=0,
            vmax=100,
            node_size=[size_map[n] for n in scam_nodes],
            linewidths=3,
            edgecolors="black",
            ax=ax,
        )

    nx.draw_networkx_edges(G, pos, ax=ax)

    sm = plt.cm.ScalarMappable(cmap=plt.cm.RdYlGn, norm=plt.Normalize(vmin=0, vmax=100))
    sm.set_array([])
    fig.colorbar(sm, ax=ax, label="Trust Score")

    ax.set_title(f"BlockID Wallet Cluster Heatmap - Cluster {cluster_id}")
    ax.axis("off")
    plt.tight_layout()

    _CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _CHARTS_DIR / f"cluster_{cluster_id}.png"
    plt.savefig(out_path, dpi=100)
    plt.close()

    print(f"[heatmap] cluster={cluster_id} wallets={G.number_of_nodes()} edges={G.number_of_edges()} saved={out_path}")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot wallet cluster heatmap (debug only).")
    ap.add_argument("--cluster-id", type=int, required=True, help="Cluster ID to visualize")
    args = ap.parse_args()

    path = plot_cluster_heatmap(args.cluster_id)
    return 0 if path else 1


if __name__ == "__main__":
    sys.exit(main())
