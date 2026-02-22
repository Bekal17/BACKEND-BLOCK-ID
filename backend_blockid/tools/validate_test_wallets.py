#!/usr/bin/env python3
"""
Validate test_wallets.csv: Solana base58, duplicates, empty labels.

Loads backend_blockid/data/test_wallets.csv
Checks: valid wallet format, duplicates, empty labels.
Prints summary. Exits with error if any invalid wallet found.

Usage:
  py -m backend_blockid.tools.validate_test_wallets
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

_TOOLS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _TOOLS_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"
TEST_WALLETS_CSV = _DATA_DIR / "test_wallets.csv"

SEP = "=" * 44
SEP_THIN = "-" * 44


def _log(msg: str) -> None:
    print(f"[validate_test_wallets] {msg}")


def _is_valid_solana_base58(wallet: str) -> bool:
    """Return True if wallet is valid Solana base58 public key."""
    wallet = (wallet or "").strip()
    if not wallet:
        return False
    try:
        from solders.pubkey import Pubkey
        Pubkey.from_string(wallet)
        return True
    except Exception:
        return False


def load_and_validate(path: Path) -> tuple[list[dict], list[dict], list[str], set[str]]:
    """
    Load CSV and validate. Returns (all_rows, invalid_rows, duplicate_wallets, unique_labels).
    invalid_rows: rows with invalid wallet
    duplicate_wallets: wallet addresses that appear more than once
    """
    all_rows: list[dict] = []
    invalid_rows: list[dict] = []
    seen_wallets: dict[str, int] = {}

    if not path.exists():
        return all_rows, invalid_rows, [], set()

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # 2 = header is line 1
            wallet = (row.get("wallet") or "").strip()
            label = (row.get("label") or "").strip()
            rec = {"wallet": wallet, "label": label, "row": i}
            all_rows.append(rec)

            if not _is_valid_solana_base58(wallet):
                invalid_rows.append(rec)

            if wallet:
                seen_wallets[wallet] = seen_wallets.get(wallet, 0) + 1

    duplicate_wallets = [w for w, c in seen_wallets.items() if c > 1]
    unique_labels = {r["label"] for r in all_rows if r["label"]}

    return all_rows, invalid_rows, duplicate_wallets, unique_labels


def main() -> int:
    _log(SEP)
    _log("Validate test_wallets.csv")
    _log(SEP)

    if not TEST_WALLETS_CSV.exists():
        _log(f"ERROR: {TEST_WALLETS_CSV} not found")
        return 1

    all_rows, invalid_rows, duplicate_wallets, unique_labels = load_and_validate(TEST_WALLETS_CSV)

    total = len(all_rows)
    valid = total - len(invalid_rows)
    invalid = len(invalid_rows)
    empty_labels = sum(1 for r in all_rows if not r["label"])
    has_invalid = len(invalid_rows) > 0

    # Summary table
    _log("")
    border = "+" + "-" * 22 + "+" + "-" * 18 + "+"
    _log(border)
    _log(f"| {'Metric':<20} | {'Value':<16} |")
    _log(border)
    _log(f"| {'Total wallets':<20} | {str(total):<16} |")
    _log(f"| {'Valid wallets':<20} | {str(valid):<16} |")
    _log(f"| {'Invalid wallets':<20} | {str(invalid):<16} |")
    _log(f"| {'Empty labels':<20} | {str(empty_labels):<16} |")
    _log(f"| {'Unique labels':<20} | {str(len(unique_labels)):<16} |")
    _log(border)

    if duplicate_wallets:
        _log("")
        _log(SEP_THIN)
        _log("DUPLICATE WALLETS")
        _log(SEP_THIN)
        for w in duplicate_wallets[:10]:
            _log(f"  {w[:40]}...")
        if len(duplicate_wallets) > 10:
            _log(f"  ... and {len(duplicate_wallets) - 10} more")
        _log("")

    if invalid_rows:
        _log(SEP_THIN)
        _log("INVALID WALLETS (not valid Solana base58)")
        _log(SEP_THIN)
        for r in invalid_rows[:10]:
            _log(f"  Row {r['row']}: {r['wallet'][:40]}... label={r['label']!r}")
        if len(invalid_rows) > 10:
            _log(f"  ... and {len(invalid_rows) - 10} more")
        _log("")

    if empty_labels and empty_labels != total:
        _log(SEP_THIN)
        _log("EMPTY LABELS (sample)")
        _log(SEP_THIN)
        count = 0
        for r in all_rows:
            if not r["label"] and count < 5:
                _log(f"  Row {r['row']}: {r['wallet'][:30]}...")
                count += 1
        _log("")

    _log(SEP)
    if unique_labels:
        _log(f"Unique labels: {', '.join(sorted(unique_labels))}")
    _log(SEP)

    if has_invalid:
        _log("RESULT: FAILED (invalid wallet found)")
        logger.error("validate_test_wallets_invalid", count=invalid)
        return 1

    _log("RESULT: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
