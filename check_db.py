import sqlite3

conn = sqlite3.connect("blockid.db")
cur = conn.cursor()

print("Tables:")
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
print(cur.fetchall())

print("\nTracked wallets:")
try:
    cur.execute("SELECT * FROM tracked_wallets;")
    print(cur.fetchall())
except Exception as e:
    print("Error:", e)
