"""
Extract scam creator wallets from token_features.csv and append to scam_wallets.csv.

Usage:
    py backend_blockid/oracle/auto_insert_scam_wallets.py

Reads backend_blockid/data/token_features.csv (scam_flag==True), extracts creator_wallets,
appends unique wallets to backend_blockid/data/scam_wallets.csv (wallet,reason,source).
Creates data dir and CSV if missing.
"""

from __future__ import annotations

import ast
import csv
from pathlib import Path

# Paths: script in backend_blockid/oracle/, data in backend_blockid/data/
_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
TOKEN_FEATURES_CSV = _DATA_DIR / "token_features.csv"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

REASON = "scam_token_creator"
SOURCE = "helius_detector"


def _parse_creator_wallets(raw: str) -> list[str]:
    """Parse creator_wallets: list string (ast.literal_eval) or semicolon-separated."""
    if not raw or not isinstance(raw, str):
        return []
    s = raw.strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            val = ast.literal_eval(s)
            if isinstance(val, list):
                return [str(x).strip() for x in val if str(x).strip()]
            return []
        except (ValueError, SyntaxError):
            pass
    return [x.strip() for x in s.split(";") if x.strip()]


def _load_existing_wallets() -> set[str]:
    """Return set of wallet addresses already in scam_wallets.csv (first column)."""
    out: set[str] = set()
    if not SCAM_WALLETS_CSV.exists():
        return out
    with open(SCAM_WALLETS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
            if w:
                out.add(w)
    return out


def main() -> int:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not TOKEN_FEATURES_CSV.exists():
        print("[scam_insert] ERROR: token_features.csv not found:", TOKEN_FEATURES_CSV)
        return 1

    print("[scam_insert] reading token_features.csv")
    scam_wallets_to_add: list[str] = []
    with open(TOKEN_FEATURES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            scam_flag = row.get("scam_flag")
            if scam_flag is None:
                continue
            if str(scam_flag).strip().lower() not in ("true", "1", "yes"):
                continue
            raw = row.get("creator_wallets") or ""
            wallets = _parse_creator_wallets(raw)
            for w in wallets:
                scam_wallets_to_add.append(w)

    existing = _load_existing_wallets()
    new_wallets = [w for w in scam_wallets_to_add if w not in existing]
    # preserve order, remove duplicates in new_wallets
    seen_new: set[str] = set()
    unique_new: list[str] = []
    for w in new_wallets:
        if w not in seen_new:
            seen_new.add(w)
            unique_new.append(w)

    for w in unique_new:
        print("[scam_insert] found wallet", w)

    if not unique_new:
        print("[scam_insert] total inserted: 0 (no new wallets)")
        return 0

    write_header = not SCAM_WALLETS_CSV.exists()
    with open(SCAM_WALLETS_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["wallet", "reason", "source"])
        for wallet in unique_new:
            w.writerow([wallet, REASON, SOURCE])
            print("[scam_insert] inserted wallet", wallet)

    print("[scam_insert] total inserted:", len(unique_new))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
