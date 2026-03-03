from backend_blockid.database.connection import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("PRAGMA table_info(wallet_graph_edges)")
rows = cur.fetchall()

print("\nwallet_graph_edges columns:")
for r in rows:
    print(dict(r))   # <<< ini penting

conn.close()