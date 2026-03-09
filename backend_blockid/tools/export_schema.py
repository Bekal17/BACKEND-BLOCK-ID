import sqlite3

conn = sqlite3.connect("blockid.db")
cur = conn.cursor()

cur.execute("""
SELECT sql
FROM sqlite_master
WHERE type='table'
AND name NOT LIKE 'sqlite_%'
""")

rows = cur.fetchall()

with open("schema.sql", "w", encoding="utf-8") as f:
    for row in rows:
        if row[0]:
            f.write(row[0] + ";\n\n")

conn.close()

print("Schema exported to schema.sql")