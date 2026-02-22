import sqlite3

conn = sqlite3.connect(r"D:\BACKENDBLOCKID\blockid.db")
cur = conn.cursor()

cur.execute("PRAGMA table_info(trust_scores)")
print(cur.fetchall())

conn.close()