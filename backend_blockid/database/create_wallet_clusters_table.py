"""
Create the wallet_graph_clusters table for graph-based clustering.

Stores per-wallet cluster assignment from connected components.
Schema: wallet (PK), cluster_id, interaction_count, total_volume.
Run before build_wallet_graph.py.

Usage:
    py -m backend_blockid.database.create_wallet_clusters_table
"""
from __future__ import annotations

import sys
from pathlib import Path

if __name__ == "__main__" and __package__ is None:
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from backend_blockid.database.connection import get_connection


def main() -> int:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_graph_clusters (
            wallet TEXT PRIMARY KEY,
            cluster_id INTEGER NOT NULL,
            interaction_count INTEGER NOT NULL DEFAULT 0,
            total_volume REAL NOT NULL DEFAULT 0.0
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_wallet_graph_clusters_cluster ON wallet_graph_clusters(cluster_id)")

    conn.commit()
    conn.close()

    print("[create_wallet_clusters_table] wallet_graph_clusters table ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
