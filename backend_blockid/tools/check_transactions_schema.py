from backend_blockid.database.connection import get_connection

conn = get_connection()

rows = conn.execute("PRAGMA table_info(transactions);").fetchall()

print("\nTransactions table schema:\n")

for r in rows:
    print({
        "cid": r[0],
        "name": r[1],
        "type": r[2],
        "notnull": r[3],
        "default": r[4],
        "pk": r[5],
    })