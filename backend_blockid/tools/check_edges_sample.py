from backend_blockid.database.connection import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT sender_wallet, receiver_wallet, tx_count
FROM wallet_graph_edges
LIMIT 10
""")

print("\nSample edges:")
for r in cur.fetchall():
    print(dict(r))

conn.close()