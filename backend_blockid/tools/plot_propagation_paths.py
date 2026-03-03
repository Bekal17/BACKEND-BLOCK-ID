"""
BlockID Scam Propagation Path Visualizer.

Shows scam transaction flow with directional arrows. Debug tool only.
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from collections import deque
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
DAYS_BACK = 30


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


def _load_transactions(cur, cutoff_ts: int) -> list[tuple[str, str, float]]:
    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}

    if "from_wallet" in cols and "to_wallet" in cols and "timestamp" in cols:
        cur.execute(
            """
            SELECT from_wallet, to_wallet, amount
            FROM transactions
            WHERE timestamp >= ? AND amount >= ?
              AND from_wallet IS NOT NULL AND to_wallet IS NOT NULL
            """,
            (cutoff_ts, MIN_AMOUNT),
        )
    else:
        cur.execute(
            """
            SELECT sender AS from_wallet, receiver AS to_wallet,
                   amount_lamports / 1e9 AS amount
            FROM transactions
            WHERE timestamp >= ? AND amount_lamports / 1e9 >= ?
              AND sender IS NOT NULL AND receiver IS NOT NULL
            """,
            (cutoff_ts, MIN_AMOUNT),
        )

    rows = cur.fetchall()
    out = []
    for r in rows:
        frm = (r[0] if r else "").strip()
        to = (r[1] if r else "").strip()
        amt = float(r[2] or 0)
        if frm and to and frm != to:
            out.append((frm, to, amt))
    return out


def _bfs_paths(G: nx.DiGraph, root: str, max_depth: int) -> set[tuple[str, ...]]:
    """BFS from root up to max_depth. Return unique paths (tuples of nodes)."""
    paths: set[tuple[str, ...]] = set()
    visited_at_depth: dict[str, int] = {}
    queue: deque[tuple[str, tuple[str, ...], int]] = deque([(root, (root,), 0)])

    while queue:
        node, path, depth = queue.popleft()
        paths.add(path)
        if depth >= max_depth:
            continue
        for succ in G.successors(node):
            if succ not in path:
                new_path = path + (succ,)
                queue.append((succ, new_path, depth + 1))

    return paths


def _pick_scam_or_risky(cur, scam_wallets: set[str]) -> str | None:
    """Return scam wallet to plot. Prefer given wallet; else pick lowest-scored scam or risky."""
    if scam_wallets:
        return next(iter(scam_wallets))
    try:
        cur.execute(
            """
            SELECT wallet, score FROM trust_scores
            ORDER BY score ASC
            LIMIT 1
            """
        )
        r = cur.fetchone()
        if r:
            return (r[0] if r else "").strip()
    except Exception:
        pass
    return None


def plot_propagation(scam_wallet: str, max_depth: int = 3) -> Path | None:
    """
    Load transactions, build DiGraph, BFS from scam wallet, draw propagation paths.
    Returns path to saved image or None.
    """
    conn = get_connection()
    cur = conn.cursor()

    cutoff_ts = int(time.time()) - (DAYS_BACK * 86400)
    tx_rows = _load_transactions(cur, cutoff_ts)

    G = nx.DiGraph()
    edge_data: dict[tuple[str, str], float] = {}
    for frm, to, amt in tx_rows:
        if G.has_edge(frm, to):
            edge_data[(frm, to)] += amt
        else:
            G.add_edge(frm, to)
            edge_data[(frm, to)] = amt

    for (u, v), amt in edge_data.items():
        G[u][v]["weight"] = amt

    if scam_wallet not in G:
        in_edges = [(u, v) for u, v in G.edges() if v == scam_wallet]
        out_edges = [(u, v) for u, v in G.edges() if u == scam_wallet]
        if not in_edges and not out_edges:
            G.add_node(scam_wallet)

    paths = _bfs_paths(G, scam_wallet, max_depth)

    sub_nodes: set[str] = set()
    for path in paths:
        sub_nodes.update(path)

    if not sub_nodes:
        sub_nodes = {scam_wallet}
    H = G.subgraph(sub_nodes).copy()

    cur.execute(
        "SELECT wallet, score FROM trust_scores WHERE wallet IN (" + ",".join("?" * len(sub_nodes)) + ")",
        tuple(sub_nodes),
    )
    score_rows = cur.fetchall()
    scores = {r[0]: float(r[1] or 50) for r in score_rows}

    scam_wallets = _load_scam_wallets(cur)
    conn.close()

    tx_count = {n: H.in_degree(n) + H.out_degree(n) for n in H.nodes()}
    node_list = list(H.nodes())
    colors = [scores.get(n, 50) for n in node_list]
    sizes = [100 + tx_count.get(n, 0) for n in node_list]

    fig, ax = plt.subplots(figsize=(12, 10))
    pos = nx.spring_layout(H, seed=42)

    nx.draw_networkx_nodes(
        H,
        pos,
        node_color=colors,
        cmap=plt.cm.RdYlGn,
        vmin=0,
        vmax=100,
        node_size=sizes,
        ax=ax,
    )

    scam_nodes = [n for n in node_list if n in scam_wallets]
    if scam_nodes:
        nx.draw_networkx_nodes(
            H,
            pos,
            nodelist=scam_nodes,
            node_color=[scores.get(n, 50) for n in scam_nodes],
            cmap=plt.cm.RdYlGn,
            vmin=0,
            vmax=100,
            node_size=[100 + tx_count.get(n, 0) for n in scam_nodes],
            linewidths=3,
            edgecolors="red",
            ax=ax,
        )

    widths = []
    for u, v in H.edges():
        w = H[u][v].get("weight", 0)
        widths.append(max(0.5, math.log(w + 1) * 0.8))

    nx.draw_networkx_edges(
        H,
        pos,
        ax=ax,
        arrows=True,
        arrowsize=12,
        width=widths,
        edge_color="gray",
        connectionstyle="arc3,rad=0.1",
    )

    sm = plt.cm.ScalarMappable(cmap=plt.cm.RdYlGn, norm=plt.Normalize(vmin=0, vmax=100))
    sm.set_array([])
    fig.colorbar(sm, ax=ax, label="Trust Score")

    ax.set_title(f"BlockID Scam Propagation Paths from {scam_wallet[:16]}...")
    ax.axis("off")
    plt.tight_layout()

    _CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = scam_wallet.replace("/", "_").replace("\\", "_")[:48]
    out_path = _CHARTS_DIR / f"propagation_{safe_name}.png"
    plt.savefig(out_path, dpi=100)
    plt.close()

    print(
        f"[propagation_plot] scam_wallet={scam_wallet[:16]}... paths={len(paths)} "
        f"nodes={H.number_of_nodes()} edges={H.number_of_edges()} saved={out_path}"
    )
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot scam propagation paths (debug only).")
    ap.add_argument("--wallet", type=str, default=None, help="Scam wallet address (auto-pick if omitted)")
    ap.add_argument("--max-depth", type=int, default=3, help="BFS max depth from scam wallet")
    args = ap.parse_args()

    conn = get_connection()
    cur = conn.cursor()
    scam_wallets = _load_scam_wallets(cur)

    wallet = (args.wallet or "").strip()
    if not wallet:
        wallet = _pick_scam_or_risky(cur, scam_wallets)
    conn.close()

    if not wallet:
        print("[propagation_plot] No scam wallet or trust_scores; specify --wallet")
        return 1

    path = plot_propagation(wallet, max_depth=args.max_depth)
    return 0 if path else 1


if __name__ == "__main__":
    sys.exit(main())
