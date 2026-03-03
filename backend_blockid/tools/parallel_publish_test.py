from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solana.rpc.api import Client

from backend_blockid.oracle.publish_one_wallet import main as publish_one

RPC_URL = os.getenv("SOLANA_RPC_URL", "").strip()
ORACLE = os.getenv("ORACLE_PUBKEY", "").strip()

WALLET_COUNT = 100
WORKERS = 5
DELAY = 0.05


def get_balance(client: Client, pubkey: str) -> float:
    return client.get_balance(Pubkey.from_string(pubkey)).value / 1_000_000_000


def generate_wallets(n: int) -> list[str]:
    wallets: list[str] = []
    for _ in range(n):
        kp = Keypair()
        wallets.append(str(kp.pubkey()))
    return wallets


def run() -> None:
    if (os.getenv("SOLANA_DEVNET") or "").strip() not in ("1", "true", "yes", "on"):
        print("[WARN] SOLANA_DEVNET is not set; aborting to keep devnet-only safety.")
        return
    if not RPC_URL:
        raise ValueError("SOLANA_RPC_URL is required")
    if not ORACLE:
        raise ValueError("ORACLE_PUBKEY is required")

    client = Client(RPC_URL)
    start_balance = get_balance(client, ORACLE)
    wallets = generate_wallets(WALLET_COUNT)

    start = time.time()
    success = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = []
        for w in wallets:
            futures.append(ex.submit(publish_one, wallet=w, score=70, risk=1))
            time.sleep(DELAY)

        for f in as_completed(futures):
            try:
                f.result()
                success += 1
            except Exception:
                failed += 1

    end = time.time()
    end_balance = get_balance(client, ORACLE)

    print("=== RESULT ===")
    print("wallets:", WALLET_COUNT)
    print("success:", success)
    print("failed:", failed)
    print("TPS:", WALLET_COUNT / (end - start))
    print("SOL spent:", start_balance - end_balance)


if __name__ == "__main__":
    run()
