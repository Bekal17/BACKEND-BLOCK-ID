import sqlite3

db = r"D:/BACKENDBLOCKID/blockid.db"
conn = sqlite3.connect(db)
cur = conn.cursor()

cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [r[0] for r in cur.fetchall()]

for t in tables:
    try:
        cur.execute(f"ALTER TABLE {t} ADD COLUMN confidence_score REAL")
        print("added confidence_score to", t)
    except Exception as e:
        # column already exists
        pass

conn.commit()
conn.close()

print("DONE")