"""
STEP 1 Graph Clustering for BlockID — behavioral fingerprint features from wallet graph.

Reads transactions (from, to) and scam_wallets (wallet), builds a NetworkX graph,
computes per-wallet features: neighbor_count, scam_neighbor_count, cluster_size,
distance_to_scam (shortest path to nearest known scam wallet). Writes backend_blockid/data/graph_cluster_features.csv.

Run from project root: py backend_blockid/oracle/graph_clustering.py
"""

from __future__ import annotations
import csv
from pathlib import Path

import networkx as nx
import pandas as pd

from backend_blockid.config.env import load_blockid_env
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# Paths relative to project root (script lives in backend_blockid/oracle/)
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
TRANSACTIONS_CSV = _DATA_DIR / "transactions.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"
OUTPUT_CSV = _DATA_DIR / "graph_cluster_features.csv"

# Sentinel for "no path to any scam wallet"
NO_PATH_TO_SCAM = -1


def load_transactions(path: Path) -> pd.DataFrame:
    """Load transactions CSV with columns from, to."""
    return pd.read_csv(path)


def load_scam_wallets(path: Path) -> set[str]:
    """Load scam wallet addresses from CSV (column wallet)."""
    out: set[str] = set()
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if w:
                out.add(w)
    return out


def build_graph(df: pd.DataFrame) -> nx.Graph:
    """Build undirected graph from edges (from, to)."""
    G = nx.Graph()
    # Use column names that may differ (from/to are CSV headers)
    from_col = "from" if "from" in df.columns else df.columns[0]
    to_col = "to" if "to" in df.columns else df.columns[1] if len(df.columns) > 1 else df.columns[0]
    for _, row in df.iterrows():
        u = str(row.get(from_col, "") or "").strip()
        v = str(row.get(to_col, "") or "").strip()
        if u and v and u != "nan" and v != "nan":
            G.add_edge(u, v)
    return G


def distance_to_nearest_scam(G: nx.Graph, scam_wallets: set[str]) -> dict[str, int]:
    """
    For each node, shortest path length (number of edges) to the nearest scam wallet.
    Scam wallets get 0. Nodes with no path to any scam get NO_PATH_TO_SCAM (-1).
    """
    # Multi-source BFS: for each scam in graph, compute distances; keep minimum per node.
    INF = 1 << 30
    dist: dict[str, int] = {n: INF for n in G.nodes()}
    scam_in_graph = [s for s in scam_wallets if G.has_node(s)]
    for scam in scam_in_graph:
        dist[scam] = 0
        lengths = nx.single_source_shortest_path_length(G, scam)
        for node, d in lengths.items():
            if d < dist[node]:
                dist[node] = d
    for node in dist:
        if dist[node] == INF:
            dist[node] = NO_PATH_TO_SCAM
    return dist


def compute_cluster_features(G: nx.Graph, scam_wallets: set[str]) -> pd.DataFrame:
    """Compute per-wallet features: neighbor_count, scam_neighbor_count, cluster_size, distance_to_scam."""
    dist_to_scam = distance_to_nearest_scam(G, scam_wallets)
    rows = []
    for node in G.nodes():
        neighbors = list(G.neighbors(node))
        scam_neighbors = sum(1 for n in neighbors if n in scam_wallets)
        try:
            comp = nx.node_connected_component(G, node)
            cluster_size = len(comp)
        except (AttributeError, TypeError, KeyError):
            # Fallback: find component containing node (older NetworkX or API change)
            for c in nx.connected_components(G):
                if node in c:
                    cluster_size = len(c)
                    break
            else:
                cluster_size = 1
        rows.append({
            "wallet": node,
            "neighbor_count": len(neighbors),
            "scam_neighbor_count": scam_neighbors,
            "cluster_size": cluster_size,
            "distance_to_scam": dist_to_scam.get(node, NO_PATH_TO_SCAM),
        })
    return pd.DataFrame(rows)


def main() -> int:
    load_blockid_env()
    logger.info("module_start", module="graph_clustering")

    if not TRANSACTIONS_CSV.exists():
        print("[graph_clustering] ERROR: transactions not found:", TRANSACTIONS_CSV)
        return 1

    print("[graph_clustering] loading transactions from", TRANSACTIONS_CSV)
    tx_df = load_transactions(TRANSACTIONS_CSV)

    print("[graph_clustering] loading scam wallets from", SCAM_WALLETS_CSV)
    scam_wallets = load_scam_wallets(SCAM_WALLETS_CSV)

    G = build_graph(tx_df)
    print("[graph_clustering] building graph (nodes =", G.number_of_nodes(), ", edges =", G.number_of_edges(), ")")

    if G.number_of_nodes() == 0:
        print("[graph_clustering] WARN: empty graph; writing empty output")
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(columns=["wallet", "neighbor_count", "scam_neighbor_count", "cluster_size", "distance_to_scam"]).to_csv(
            OUTPUT_CSV, index=False
        )
        print("[graph_clustering] saved:", OUTPUT_CSV)
        return 0

    print("[graph_clustering] computing cluster features...")
    features_df = compute_cluster_features(G, scam_wallets)

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    features_df.to_csv(OUTPUT_CSV, index=False)
    print("[graph_clustering] saved", len(features_df), "rows to", OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
