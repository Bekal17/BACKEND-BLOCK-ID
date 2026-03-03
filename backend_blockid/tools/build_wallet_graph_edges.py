from backend_blockid.database.connection import get_connection
import time

conn = get_connection()
cur = conn.cursor()

print("Building wallet_graph_edges...")

# Detect schema: new (from_wallet/to_wallet/amount) vs old (sender/receiver/amount_lamports)
cur.execute("PRAGMA table_info(transactions)")
cols = {row[1] for row in cur.fetchall()}
if "from_wallet" in cols and "to_wallet" in cols:
    cur.execute("""
        SELECT from_wallet, to_wallet, COUNT(*) AS tx_count,
               SUM(CAST(amount * 1e9 AS INTEGER)) AS total_volume,
               MAX(timestamp) AS last_seen
        FROM transactions
        WHERE from_wallet IS NOT NULL AND to_wallet IS NOT NULL
        GROUP BY from_wallet, to_wallet
    """)
else:
    cur.execute("""
        SELECT sender AS from_wallet, receiver AS to_wallet, COUNT(*) AS tx_count,
               SUM(amount_lamports) AS total_volume,
               MAX(timestamp) AS last_seen
        FROM transactions
        WHERE sender IS NOT NULL AND receiver IS NOT NULL
        GROUP BY sender, receiver
    """)

rows = cur.fetchall()

for r in rows:
    cur.execute("""
    INSERT OR REPLACE INTO wallet_graph_edges
    (sender_wallet, receiver_wallet, tx_count, total_volume, last_seen_timestamp)
    VALUES (?, ?, ?, ?, ?)
    """, (
        r["from_wallet"],
        r["to_wallet"],
        r["tx_count"],
        r["total_volume"] or 0,
        r["last_seen"] or int(time.time())
    ))

conn.commit()
conn.close()

print("Graph edges populated:", len(rows))
