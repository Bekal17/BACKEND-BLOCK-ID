from backend_blockid.database.connection import get_connection

conn = get_connection()
cur = conn.cursor()

# Insert cluster members
cluster_id = "demo_cluster"

wallets = [
    "wallet_A",
    "wallet_B",
    "wallet_C",
    "wallet_D"
]

for w in wallets:
    cur.execute(
        "INSERT OR IGNORE INTO wallet_cluster_members (wallet, cluster_id) VALUES (?,?)",
        (w, cluster_id)
    )

# Insert graph edges
edges = [
    ("wallet_A", "wallet_B"),
    ("wallet_B", "wallet_C"),
    ("wallet_C", "wallet_D"),
]

for s, r in edges:
    cur.execute(
        "INSERT OR IGNORE INTO wallet_graph_edges (sender_wallet, receiver_wallet, tx_count, total_volume, last_seen_timestamp) VALUES (?,?,?,?,?)",
        (s, r, 3, 1000, 1700000000)
    )

conn.commit()
conn.close()

print("Demo graph inserted")