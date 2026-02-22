import sqlite3

conn = sqlite3.connect(r"D:/BACKENDBLOCKID/blockid.db")
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("TABLES:", cur.fetchall())

cur.execute("PRAGMA table_info(trust_scores)")
print("trust_scores:", cur.fetchall())

cur.execute("PRAGMA table_info(wallet_history)")
print("wallet_history:", cur.fetchall())

conn.close()