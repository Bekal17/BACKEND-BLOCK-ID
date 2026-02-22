"""
Auto evidence collector — scans wallet transactions and inserts reason evidence.

Delegates to scan_wallet_transactions.

Usage:
  py -m backend_blockid.oracle.auto_evidence_collector
"""

from __future__ import annotations

import sys

from backend_blockid.api_server.db_wallet_tracking import init_db, load_active_wallets
from backend_blockid.blockid_logging import get_logger
from backend_blockid.oracle.scan_wallet_transactions import main as scan_wallet_transactions

logger = get_logger(__name__)

MAX_WALLETS = 10


def main() -> int:
    logger.info("module_start", module="auto_evidence_collector")
    init_db()
    wallets = load_active_wallets()

    if len(wallets) > MAX_WALLETS:
        logger.info(
            "auto_evidence_wallet_limit",
            total_wallets=len(wallets),
            limited_to=MAX_WALLETS,
        )
        wallets = wallets[:MAX_WALLETS]

    for i, wallet in enumerate(wallets):
        logger.info(
            "auto_evidence_scan_wallet",
            wallet=wallet,
            index=i,
            total=len(wallets),
        )

    original_argv = sys.argv[:]
    try:
        sys.argv = [original_argv[0], "--limit", str(MAX_WALLETS)]
        return scan_wallet_transactions()
    finally:
        sys.argv = original_argv


if __name__ == "__main__":
    raise SystemExit(main())
