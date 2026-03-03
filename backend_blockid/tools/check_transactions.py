from backend_blockid.database.connection import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("SELECT COUNT(*) as cnt FROM transactions")
print("Total transactions:", cur.fetchone()["cnt"])

cur.execute("SELECT * FROM transactions LIMIT 5")
rows = cur.fetchall()
print("\nSample transactions:")
for r in rows:
    print(dict(r))

conn.close()