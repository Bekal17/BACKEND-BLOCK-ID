from __future__ import annotations

import os
import time
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()
TOKEN = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
CHAT_ID = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()


def _enabled() -> bool:
    return bool(TOKEN and CHAT_ID)


def send_alert(message: str) -> None:
    if not _enabled():
        print("Telegram not configured")
        return
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    try:
        requests.post(url, json=data, timeout=5)
    except Exception as e:
        print("Telegram send failed:", e)


def send_pipeline_summary(stats: dict[str, Any]) -> None:
    wallets_scored = stats.get("wallets_scored", 0)
    rpc_latency_avg = stats.get("rpc_latency_avg", None)
    pda_failures = stats.get("pda_failures", 0)
    errors = stats.get("errors", 0)
    lines = [
        "🚀 BlockID Pipeline Complete",
        f"Wallets scored: {wallets_scored}",
        f"Avg RPC latency: {rpc_latency_avg if rpc_latency_avg is not None else '—'} ms",
        f"PDA failures: {pda_failures}",
        f"Errors: {errors}",
    ]
    send_alert("\n".join(lines))


if __name__ == "__main__":
    send_pipeline_summary(
        {
            "wallets_scored": 100,
            "rpc_latency_avg": 850,
            "pda_failures": 0,
            "errors": 0,
        }
    )
