import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

from backend_blockid.database.connection import get_connection


def load_cluster_data(cluster_id: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT wallet, score
        FROM trust_scores
        WHERE wallet IN (
            SELECT wallet FROM wallet_clusters WHERE cluster_id=?
        )
        """,
        (cluster_id,),
    )

    rows = cur.fetchall()
    conn.close()

    return pd.DataFrame(rows, columns=["wallet", "score"])


def build_graph(cluster_id: str) -> nx.Graph:
    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT e.sender_wallet, e.receiver_wallet, e.tx_count
        FROM wallet_graph_edges e
        JOIN wallet_cluster_members c
            ON e.sender_wallet = c.wallet
        WHERE c.cluster_id=?
        """,
        (cluster_id,),
    )

    edges = cur.fetchall()
    conn.close()

    import networkx as nx
    G = nx.Graph()
    G.add_weighted_edges_from(edges)
    return G


def visualize_cluster(cluster_id: str) -> None:
    df = load_cluster_data(cluster_id)
    G = build_graph(cluster_id)

    score_map = dict(zip(df.wallet, df.score))

    colors = []
    for node in G.nodes():
        s = score_map.get(node, 50)
        colors.append(s)

    fig, ax = plt.subplots(figsize=(10, 8))

    pos = nx.spring_layout(G, seed=42)

    nodes = nx.draw_networkx_nodes(
        G,
        pos,
        node_color=colors,
        cmap=plt.cm.RdYlGn,
        node_size=200,
        ax=ax,
    )

    nx.draw_networkx_edges(G, pos, ax=ax)

    sm = plt.cm.ScalarMappable(
        cmap=plt.cm.RdYlGn,
        norm=plt.Normalize(vmin=0, vmax=100),
    )
    sm.set_array([])

    fig.colorbar(sm, ax=ax, label="Trust Score")

    ax.set_title(f"Cluster Risk Heatmap {cluster_id}")
    ax.axis("off")

    plt.show()


if __name__ == "__main__":
    visualize_cluster("cluster_1")

