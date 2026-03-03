from __future__ import annotations

import csv
import os
import random
import time
from pathlib import Path

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solana.rpc.api import Client

from backend_blockid.oracle.publish_one_wallet import main as publish_one
from backend_blockid.oracle.solana_publisher import _load_keypair
from backend_blockid.oracle.rpc_manager import get_rpc_manager

N_WALLETS = 1_000
START_DELAY = 0.05
MIN_DELAY = 0.01
MAX_DELAY = 0.5
ERROR_BACKOFF = 1.5

OUT = Path("backend_blockid/data/stress_wallets.csv")
LAMPORTS_PER_SOL = 1_000_000_000


def _rpc_url() -> str:
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url
    if (os.getenv("SOLANA_DEVNET") or "").strip() in ("1", "true", "yes", "on"):
        return "https://api.devnet.solana.com"
    return "https://api.mainnet-beta.solana.com"


def _client() -> Client:
    try:
        return get_rpc_manager().get_client()
    except Exception:
        return Client(_rpc_url())


def _oracle_pubkey() -> str:
    oracle_key = (os.getenv("ORACLE_PRIVATE_KEY") or "").strip()
    if not oracle_key:
        raise ValueError("ORACLE_PRIVATE_KEY is required for stress test cost tracking")
    keypair = _load_keypair(oracle_key)
    return str(keypair.pubkey())


def _get_balance_sol(client: Client, pubkey_str: str) -> float:
    resp = client.get_balance(Pubkey.from_string(pubkey_str))
    value = getattr(resp, "value", None) or (
        getattr(resp, "result", None) and getattr(resp.result, "value", None)
    )
    lamports = int(value or 0)
    return lamports / LAMPORTS_PER_SOL


def generate_wallets(n: int) -> list[str]:
    wallets: list[str] = []
    for _ in range(n):
        kp = Keypair()
        wallets.append(str(kp.pubkey()))
    return wallets


def save_wallets(wallets: list[str]) -> None:
    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["wallet", "score", "risk"])
        for wal in wallets:
            score = random.randint(40, 90)
            risk = score_to_risk(score)
            w.writerow([wal, score, risk])


def score_to_risk(score: int) -> int:
    if score >= 80:
        return 0
    if score >= 60:
        return 1
    if score >= 40:
        return 2
    return 3


def run_test() -> None:
    if (os.getenv("SOLANA_DEVNET") or "").strip() not in ("1", "true", "yes", "on"):
        print("[WARN] SOLANA_DEVNET is not set; use devnet for stress testing.")

    wallets = generate_wallets(N_WALLETS)
    save_wallets(wallets)

    client = _client()
    oracle_pubkey_str = _oracle_pubkey()
    starting_balance = _get_balance_sol(client, oracle_pubkey_str)

    delay = START_DELAY
    success = 0
    failed = 0
    start = time.time()

    for i, w in enumerate(wallets, 1):
        try:
            publish_one(wallet=w, score=60, risk=1)
            success += 1
            # speed up if stable
            delay = max(MIN_DELAY, delay * 0.95)
        except Exception as e:
            failed += 1
            print("RPC ERROR:", e)
            # slow down if RPC error
            delay = min(MAX_DELAY, delay * ERROR_BACKOFF)

        time.sleep(delay)

        if i % 100 == 0:
            elapsed = time.time() - start
            tps = success / elapsed if elapsed else 0
            success_rate = (success / i) * 100 if i else 0
            current_balance = _get_balance_sol(client, oracle_pubkey_str)
            spent = max(0.0, starting_balance - current_balance)
            print(
                "[STATS] wallets=%d success=%d failed=%d delay=%.3fs tps=%.2f success_rate=%.1f%% spent=%.6f SOL"
                % (i, success, failed, delay, tps, success_rate, spent)
            )

    elapsed = time.time() - start
    final_balance = _get_balance_sol(client, oracle_pubkey_str)
    spent = max(0.0, starting_balance - final_balance)
    print("\nFINAL:")
    print("Success:", success)
    print("Failed:", failed)
    print("TPS:", success / elapsed if elapsed else 0)
    print("Success rate:", (success / (success + failed) * 100) if (success + failed) else 0)
    print("SOL spent:", round(spent, 6))


if __name__ == "__main__":
    run_test()
