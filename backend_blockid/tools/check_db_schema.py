from backend_blockid.database.connection import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("PRAGMA table_info(trust_scores)")
print(cur.fetchall())

conn.close()