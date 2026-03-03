from __future__ import annotations

import shutil
import time
from pathlib import Path

import requests

from backend_blockid.config.env import get_solana_rpc_url, load_blockid_env
from backend_blockid.database.connection import get_connection
from backend_blockid.tools.backup_db import BACKUPS_DIR


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _table_exists(conn, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


def check_rpc() -> None:
    load_blockid_env()
    url = get_solana_rpc_url()
    start = time.time()
    resp = requests.post(
        url,
        json={"jsonrpc": "2.0", "id": 1, "method": "getSlot"},
        timeout=5,
    )
    elapsed_ms = int((time.time() - start) * 1000)
    if resp.status_code != 200:
        raise Exception(f"RPC down (status={resp.status_code})")
    body = resp.json()
    if "result" not in body:
        raise Exception("RPC error (no result)")
    print(f"RPC OK ({elapsed_ms} ms)")


def check_db() -> None:
    conn = get_connection()
    res = conn.execute("PRAGMA integrity_check").fetchone()
    if not res or res[0] != "ok":
        conn.close()
        raise Exception("DB corrupted")
    required_tables = ["trust_scores", "transactions", "priority_wallets", "wallet_reasons"]
    for table in required_tables:
        if not _table_exists(conn, table):
            conn.close()
            raise Exception(f"Missing table: {table}")
    conn.close()
    print("DB OK")


def check_disk() -> None:
    root = _project_root()
    total, used, free = shutil.disk_usage(str(root))
    if free < 1_000_000_000:
        raise Exception("Low disk space (<1GB free)")
    backups_size = 0
    if BACKUPS_DIR.exists():
        backups_size = sum(p.stat().st_size for p in BACKUPS_DIR.glob("*") if p.is_file())
    backups_mb = round(backups_size / 1_000_000, 2)
    print(f"Disk OK (free={round(free / 1_000_000_000, 2)} GB, backups={backups_mb} MB)")


def check_wallet_queue() -> None:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM priority_wallets")
    count = cur.fetchone()[0] or 0
    conn.close()
    status = "OK"
    if count < 10 or count > 5000:
        status = "WARN"
    print(f"Priority wallets: {count} ({status})")


def run_health_check() -> None:
    try:
        check_rpc()
        check_db()
        check_disk()
        check_wallet_queue()
    except Exception as e:
        try:
            from backend_blockid.tools.telegram_alert import send_alert
            send_alert(f"BlockID health check failed: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    run_health_check()
