from backend_blockid.database.connection import get_connection

cluster_id = "cluster_1"

conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT wallet
FROM wallet_cluster_members
WHERE cluster_id=?
""", (cluster_id,))

print(f"\nWallets in {cluster_id}:")
for r in cur.fetchall():
    print(r["wallet"])

conn.close()