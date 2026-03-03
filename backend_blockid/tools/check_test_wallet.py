from backend_blockid.database.connection import get_connection

wallet = "8X35rQUK2u9hfn8rMPwwr6ZSEUhbmfDPEapp589XyoM1"

conn = get_connection()
cur = conn.cursor()

cur.execute("""
SELECT wallet, is_test_wallet
FROM trust_scores
WHERE wallet=?
""", (wallet,))

row = cur.fetchone()

if row:
    print("Wallet:", row[0])
    print("is_test_wallet:", row[1])
else:
    print("Wallet not found in DB")

conn.close()