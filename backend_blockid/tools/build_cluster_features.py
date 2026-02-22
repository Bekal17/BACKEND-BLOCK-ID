#!/usr/bin/env python3
"""
Build cluster_features.csv from transactions.csv.

Reads transactions (from, to), builds wallet interaction graph,
computes: cluster_size, tx_count, unique_counterparties, avg_tx_value,
is_connected_to_known_scam (when scam_wallets.csv exists).

Output: backend_blockid/data/cluster_features.csv

Usage:
  py -m backend_blockid.tools.build_cluster_features
"""

from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path

import pandas as pd

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

_TOOLS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _TOOLS_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"

TRANSACTIONS_CSV = _DATA_DIR / "transactions.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"
OUTPUT_CSV = _DATA_DIR / "cluster_features.csv"


def load_transactions(path: Path) -> pd.DataFrame:
    """Load transactions CSV with from, to (and optional amount/value)."""
    if not path.exists():
        return pd.DataFrame()
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


def build_adjacency(df: pd.DataFrame) -> dict[str, set[str]]:
    """Build undirected graph as adjacency dict."""
    adj: dict[str, set[str]] = defaultdict(set)
    from_col = "from" if "from" in df.columns else df.columns[0]
    to_col = "to" if "to" in df.columns else df.columns[1] if len(df.columns) > 1 else df.columns[0]
    for _, row in df.iterrows():
        u = str(row.get(from_col, "") or "").strip()
        v = str(row.get(to_col, "") or "").strip()
        if u and v and u != "nan" and v != "nan":
            adj[u].add(v)
            adj[v].add(u)
    return dict(adj)


def connected_components(adj: dict[str, set[str]]) -> dict[str, int]:
    """Return node -> component_id mapping. Same id = same component."""
    visited: set[str] = set()
    node_to_comp: dict[str, int] = {}
    comp_id = 0
    for start in adj:
        if start in visited:
            continue
        stack = [start]
        while stack:
            n = stack.pop()
            if n in visited:
                continue
            visited.add(n)
            node_to_comp[n] = comp_id
            for nb in adj.get(n, set()):
                if nb not in visited:
                    stack.append(nb)
        comp_id += 1
    return node_to_comp


def component_sizes(node_to_comp: dict[str, int]) -> dict[int, int]:
    """Count nodes per component."""
    sizes: dict[int, int] = defaultdict(int)
    for cid in node_to_comp.values():
        sizes[cid] += 1
    return dict(sizes)


def build_cluster_features(
    transactions_path: Path = TRANSACTIONS_CSV,
    scam_wallets_path: Path = SCAM_WALLETS_CSV,
    output_path: Path = OUTPUT_CSV,
) -> bool:
    """
    Build cluster_features.csv. Returns True on success, False on failure.
    """
    if not transactions_path.exists():
        logger.warning("build_cluster_features_transactions_missing", path=str(transactions_path))
        return False

    df = load_transactions(transactions_path)
    if df.empty:
        _DATA_DIR.mkdir(parents=True, exist_ok=True)
        empty_df = pd.DataFrame(columns=[
            "wallet", "cluster_size", "tx_count", "unique_counterparties",
            "avg_tx_value", "is_connected_to_known_scam",
        ])
        empty_df.to_csv(output_path, index=False)
        logger.info("cluster_features_generated", path=str(output_path), rows=0)
        return True

    scam_wallets = load_scam_wallets(scam_wallets_path)
    adj = build_adjacency(df)
    node_to_comp = connected_components(adj)
    comp_sizes = component_sizes(node_to_comp)

    # Per-wallet metrics
    from_col = "from" if "from" in df.columns else df.columns[0]
    to_col = "to" if "to" in df.columns else df.columns[1] if len(df.columns) > 1 else df.columns[0]
    amount_col = "amount" if "amount" in df.columns else ("value" if "value" in df.columns else None)

    tx_count: dict[str, int] = {}
    unique_counterparties: dict[str, set[str]] = {}
    tx_values: dict[str, list[float]] = {}

    for _, row in df.iterrows():
        u = str(row.get(from_col, "") or "").strip()
        v = str(row.get(to_col, "") or "").strip()
        if not u or not v or u == "nan" or v == "nan":
            continue
        tx_count[u] = tx_count.get(u, 0) + 1
        tx_count[v] = tx_count.get(v, 0) + 1
        if u not in unique_counterparties:
            unique_counterparties[u] = set()
        unique_counterparties[u].add(v)
        if v not in unique_counterparties:
            unique_counterparties[v] = set()
        unique_counterparties[v].add(u)
        if amount_col and amount_col in row:
            try:
                val = float(row[amount_col])
                tx_values.setdefault(u, []).append(val)
                tx_values.setdefault(v, []).append(val)
            except (TypeError, ValueError):
                pass

    rows = []
    for node in adj:
        neighbors = adj[node]
        comp_id = node_to_comp.get(node, -1)
        cluster_size = comp_sizes.get(comp_id, 1)

        vals = tx_values.get(node, [])
        avg_tx_value = sum(vals) / len(vals) if vals else 0.0
        is_connected = 1 if any(n in scam_wallets for n in neighbors) else 0

        rows.append({
            "wallet": node,
            "cluster_size": cluster_size,
            "tx_count": tx_count.get(node, 0),
            "unique_counterparties": len(unique_counterparties.get(node, set())),
            "avg_tx_value": round(avg_tx_value, 6),
            "is_connected_to_known_scam": is_connected,
        })

    result = pd.DataFrame(rows)
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    result.to_csv(output_path, index=False)
    logger.info("cluster_features_generated", path=str(output_path), rows=len(result))
    return True


def main() -> int:
    import sys
    ok = build_cluster_features()
    if ok:
        print(f"[build_cluster_features] Saved to {OUTPUT_CSV}")
    else:
        print(f"[build_cluster_features] ERROR: {TRANSACTIONS_CSV} not found", file=sys.stderr)
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    raise SystemExit(main())
