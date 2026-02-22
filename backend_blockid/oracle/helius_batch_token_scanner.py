"""
Batch scan token mints via Helius getAsset, extract ML features, save to CSV.

Usage:
    py backend_blockid/oracle/helius_batch_token_scanner.py

Reads mints from backend_blockid/data/suspicious_tokens.csv (column: mint or token_mint).
Writes backend_blockid/data/token_features.csv.
Skips mints already in token_features.csv. Creates data dir/file if missing.
"""

from __future__ import annotations

import csv
import time
from pathlib import Path

from dotenv import load_dotenv

# Paths: script lives in backend_blockid/oracle/, data in backend_blockid/data/, .env at repo root
_SCRIPT_DIR = Path(__file__).resolve().parent
_ROOT = _SCRIPT_DIR.parents[2]  # repo root for .env
_DATA_DIR = _SCRIPT_DIR.parent / "data"
INPUT_CSV = _DATA_DIR / "suspicious_tokens.csv"
OUTPUT_CSV = _DATA_DIR / "token_features.csv"

load_dotenv(_ROOT / ".env")

# CSV columns for output
OUTPUT_COLUMNS = [
    "mint",
    "name",
    "symbol",
    "mint_authority",
    "freeze_authority",
    "supply",
    "decimals",
    "metadata_missing",
    "scam_flag",
    "creator_wallets",
]

# Delay between RPC calls (seconds)
RPC_DELAY = 0.2


def _load_input_mints() -> list[str]:
    """Read unique mints from input CSV. Column: mint or token_mint."""
    mints: list[str] = []
    seen: set[str] = set()
    if not INPUT_CSV.exists():
        return mints
    with open(INPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            mint = (row.get("mint") or row.get("token_mint") or "").strip()
            if mint and mint not in seen:
                seen.add(mint)
                mints.append(mint)
    return mints


def _load_processed_mints() -> set[str]:
    """Return set of mints already present in token_features.csv."""
    out: set[str] = set()
    if not OUTPUT_CSV.exists():
        return out
    with open(OUTPUT_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            m = (row.get("mint") or "").strip()
            if m:
                out.add(m)
    return out


def _token_to_row(mint: str, token: dict) -> dict[str, str | int | bool | None]:
    """Build one output row from analyze_token() result."""
    creators = token.get("creator_authorities") or []
    creator_wallets = ";".join(str(c) for c in creators) if creators else ""
    scam_flag = bool(
        token.get("mint_authority_exists")
        or token.get("freeze_authority_exists")
        or token.get("metadata_missing")
    )
    return {
        "mint": mint,
        "name": token.get("name") or "",
        "symbol": token.get("symbol") or "",
        "mint_authority": token.get("mint_authority") or "",
        "freeze_authority": token.get("freeze_authority") or "",
        "supply": token.get("supply") if token.get("supply") is not None else "",
        "decimals": token.get("decimals") if token.get("decimals") is not None else "",
        "metadata_missing": token.get("metadata_missing", True),
        "scam_flag": scam_flag,
        "creator_wallets": creator_wallets,
    }


def _ensure_output_file() -> None:
    """Create data dir and output CSV with header if file missing."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS).writeheader()


def _append_row(row: dict) -> None:
    """Append one row to token_features.csv."""
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writerow(row)


def main() -> int:
    from backend_blockid.oracle.helius_extract_fields import analyze_token, fetch_asset

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not INPUT_CSV.exists():
        print("[helius_batch] ERROR: input not found:", INPUT_CSV)
        return 1

    mints = _load_input_mints()
    processed = _load_processed_mints()
    to_process = [m for m in mints if m not in processed]

    print("[helius_batch] input mints:", len(mints), "| already processed:", len(processed), "| to process:", len(to_process))
    if not to_process:
        print("[helius_batch] nothing to do")
        return 0

    _ensure_output_file()

    for i, mint in enumerate(to_process, 1):
        print("[helius_batch] processing mint", i, "/", len(to_process), mint)
        asset = fetch_asset(mint)
        if asset is None:
            print("[helius_batch] error: getAsset failed for", mint)
            time.sleep(RPC_DELAY)
            continue

        token = analyze_token(asset)
        row = _token_to_row(mint, token)
        _append_row(row)
        print("[helius_batch] saved row for", mint)
        time.sleep(RPC_DELAY)

    print("[helius_batch] done. output:", OUTPUT_CSV)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
