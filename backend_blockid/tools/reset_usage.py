#!/usr/bin/env python3
"""
Reset monthly API usage. Run nightly via scheduler.

Usage:
  py -m backend_blockid.tools.reset_usage

Resets api_usage counters when a new billing period starts (e.g. 1st of month).
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from backend_blockid.database.connection import get_connection


def reset_usage() -> int:
    conn = get_connection()
    cur = conn.cursor()
    now = int(time.time())
    cur.execute(
        "UPDATE api_usage SET wallet_checks = 0, batch_checks = 0, reports_generated = 0, webhook_events = 0, last_reset = ?",
        (now,),
    )
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def main() -> int:
    n = reset_usage()
    print(f"[reset_usage] Reset {n} api_usage row(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
