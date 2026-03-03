from backend_blockid.database.connection import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT cluster_id, COUNT(*) as cnt
FROM wallet_cluster_members
GROUP BY cluster_id
ORDER BY cnt DESC
""")

print("\nCLUSTERS:")
for r in cur.fetchall():
    print(dict(r))

conn.close()