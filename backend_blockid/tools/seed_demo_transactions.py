from backend_blockid.database.connection import get_connection
import time


conn = get_connection()
cur = conn.cursor()

wallets = [
    "wallet_A",
    "wallet_B",
    "wallet_C",
    "wallet_D",
]

txs = [
    ("wallet_A", "wallet_B", 5),
    ("wallet_B", "wallet_C", 2),
    ("wallet_C", "wallet_D", 3),
]

now = int(time.time())

for i, (s, r, amt) in enumerate(txs, start=1):
    # Use schema: (wallet, signature, sender, receiver, amount_lamports, timestamp, slot, created_at)
    cur.execute(
        """
        INSERT INTO transactions
        (wallet, signature, sender, receiver, amount_lamports, timestamp, slot, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            s,  # store under sender wallet
            f"demo_sig_{i}",
            s,
            r,
            amt,
            now,
            0,
            now,
        ),
    )

conn.commit()
conn.close()

print("Demo transactions inserted")

