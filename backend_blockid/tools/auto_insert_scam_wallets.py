import pandas as pd
import sqlite3
import time
from pathlib import Path

CSV = Path("backend_blockid/data/scam_wallets.csv")
DB = Path(r"D:/BACKENDBLOCKID/blockid.db")

def main():
    if not CSV.exists():
        print("scam_wallets.csv not found")
        return

    df = pd.read_csv(CSV)

    conn = sqlite3.connect(DB)
    cur = conn.cursor()

    inserted = 0

    for _, r in df.iterrows():
        wallet = r["wallet"]

        cur.execute("SELECT wallet FROM scam_wallets WHERE wallet=?", (wallet,))
        if cur.fetchone():
            continue

        cur.execute("""
        INSERT INTO scam_wallets(wallet, source, label, detected_at, notes)
        VALUES(?,?,?,?,?)
        """, (
            wallet,
            r.get("source","unknown"),
            r.get("label","scam"),
            int(time.time()),
            r.get("notes","")
        ))

        inserted += 1

    conn.commit()
    conn.close()

    print("Inserted scam wallets:", inserted)

if __name__ == "__main__":
    main()