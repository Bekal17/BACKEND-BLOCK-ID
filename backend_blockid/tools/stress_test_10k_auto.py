from __future__ import annotations

import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solana.rpc.api import Client

from backend_blockid.oracle.publish_one_wallet import main as publish_one

RPC_URL = (os.getenv("SOLANA_RPC_URL") or "").strip()
ORACLE = (os.getenv("ORACLE_PUBKEY") or "").strip()
OUT = Path("backend_blockid/data/stress_wallets.csv")

TOTAL_WALLETS = 10_000
START_WORKERS = 10
MIN_WORKERS = 1
START_DELAY = 0.02
MAX_DELAY = 1.0


def generate_wallets(n: int) -> list[str]:
    wallets: list[str] = []
    for _ in range(n):
        wallets.append(str(Keypair().pubkey()))
    return wallets


def save_wallets(wallets: list[str]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["wallet"])
        for x in wallets:
            w.writerow([x])


def get_balance(client: Client, pubkey: str) -> float:
    return client.get_balance(Pubkey.from_string(pubkey)).value / 1_000_000_000


def run() -> None:
    if (os.getenv("SOLANA_DEVNET") or "").strip() != "1":
        print("ABORT: devnet only")
        return

    if not RPC_URL:
        raise ValueError("SOLANA_RPC_URL required")
    if not ORACLE:
        raise ValueError("ORACLE_PUBKEY required")

    client = Client(RPC_URL)
    start_balance = get_balance(client, ORACLE)

    wallets = generate_wallets(TOTAL_WALLETS)
    save_wallets(wallets)

    workers = START_WORKERS
    delay = START_DELAY
    success = 0
    failed = 0
    start = time.time()

    i = 0
    while i < len(wallets):
        batch = wallets[i : i + workers]
        errors = 0

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(publish_one, wallet=w, score=70, risk=1) for w in batch]

            for f in as_completed(futures):
                try:
                    f.result()
                    success += 1
                except Exception:
                    errors += 1
                    failed += 1

        if errors > 0:
            workers = max(MIN_WORKERS, workers // 2)
            delay = min(MAX_DELAY, delay * 2)
            print(f"[RATE LIMIT] errors={errors} -> workers={workers} delay={delay}")
        else:
            workers += 1
            delay = max(0.01, delay * 0.9)

        time.sleep(delay)
        i += len(batch)

    end = time.time()
    end_balance = get_balance(client, ORACLE)

    print("==== RESULT ====")
    print("wallets:", TOTAL_WALLETS)
    print("success:", success)
    print("failed:", failed)
    print("TPS:", TOTAL_WALLETS / (end - start))
    print("SOL spent:", start_balance - end_balance)


if __name__ == "__main__":
    run()
