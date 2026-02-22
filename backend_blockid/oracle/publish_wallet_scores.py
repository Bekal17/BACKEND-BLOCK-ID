"""
Publish ML wallet_scores.csv into BlockID Anchor oracle PDA accounts.

Usage:
    py backend_blockid/oracle/publish_wallet_scores.py

Reads backend_blockid/data/wallet_scores.csv (wallet, risk_score, scam_probability, reason_code).
For each wallet: trust_score = 100 - risk_score; calls publish_one_wallet.py; logs tx signature.
Skips wallets already published today. Retries 3 times on RPC timeout.
"""

from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

# Paths: script in backend_blockid/oracle/, publish_one_wallet.py next to this script
SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parents[2]
PUBLISH_SCRIPT = SCRIPT_DIR / "publish_one_wallet.py"
_DATA_DIR = SCRIPT_DIR.parent / "data"
WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"
SUBPROCESS_TIMEOUT = 120
MAX_RETRIES = 3


def _published_today_path() -> Path:
    """Path to CSV of wallets published today (wallet column)."""
    today = date.today().isoformat()
    return _DATA_DIR / f"published_scores_{today}.csv"


def _load_published_today() -> set[str]:
    """Load set of wallets already published today."""
    path = _published_today_path()
    out: set[str] = set()
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or (list(row.values())[0] if row else "") or "").strip()
            if w:
                out.add(w)
    return out


def _append_published_today(wallet: str) -> None:
    """Append wallet to today's published list (create file with header if missing)."""
    path = _published_today_path()
    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(["wallet"])
        w.writerow([wallet])


def _call_publish_one_wallet(wallet: str, trust_score: int, reason_code: str) -> tuple[bool, str | None]:
    """
    Call publish_one_wallet.py for one wallet and score.
    Returns (success, tx_signature or None). Retries up to MAX_RETRIES on timeout.
    """
    cmd = [sys.executable, str(PUBLISH_SCRIPT), wallet, str(trust_score)]
    for attempt in range(MAX_RETRIES):
        try:
            result = subprocess.run(
                cmd,
                cwd=str(SCRIPT_DIR),
                capture_output=True,
                text=True,
                timeout=SUBPROCESS_TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            print("[publish] tx failed (RPC timeout)", "wallet=", wallet[:16] + "...", "attempt=", attempt + 1, "reason_code=", reason_code)
            if attempt < MAX_RETRIES - 1:
                continue
            return False, None
        except Exception as e:
            print("[publish] tx failed", "wallet=", wallet[:16] + "...", "error=", e, "reason_code=", reason_code)
            return False, None

        if result.returncode != 0:
            print("[publish] tx failed", "wallet=", wallet[:16] + "...", "returncode=", result.returncode, "reason_code=", reason_code)
            if attempt < MAX_RETRIES - 1:
                continue
            return False, None

        # Parse tx_signature from stdout (e.g. "tx_signature=...")
        signature: str | None = None
        for line in (result.stdout or "").splitlines():
            m = re.search(r"tx_signature=(.+)", line)
            if m:
                signature = m.group(1).strip()
                break
        return True, signature
    return False, None


def main() -> int:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    if not WALLET_SCORES_CSV.exists():
        print("[publish] ERROR: wallet_scores.csv not found:", WALLET_SCORES_CSV)
        return 1

    if not PUBLISH_SCRIPT.exists():
        raise FileNotFoundError(f"publish_one_wallet.py not found: {PUBLISH_SCRIPT}")

    published_today = _load_published_today()
    rows: list[dict] = []
    with open(WALLET_SCORES_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if w:
                rows.append({
                    "wallet": w,
                    "risk_score": row.get("risk_score"),
                    "scam_probability": row.get("scam_probability"),
                    "reason_code": (row.get("reason_code") or "").strip(),
                })

    to_publish = [r for r in rows if r["wallet"] not in published_today]
    if not to_publish:
        print("[publish] no wallets to publish (all already published today)")
        return 0

    print("[publish] publishing", len(to_publish), "wallets (skipped", len(rows) - len(to_publish), "already today)")

    for r in to_publish:
        wallet = r["wallet"]
        reason_code = r["reason_code"]
        try:
            risk = int(float(r["risk_score"] or 0))
        except (TypeError, ValueError):
            risk = 0
        trust_score = max(0, min(100, 100 - risk))

        print("[publish] wallet", wallet, "trust_score=", trust_score, "reason_code=", reason_code)
        ok, signature = _call_publish_one_wallet(wallet, trust_score, reason_code)
        if ok:
            print("[publish] tx success", "wallet=", wallet, "signature=", signature or "(none)", "reason_code=", reason_code)
            _append_published_today(wallet)
        else:
            print("[publish] tx failed", "wallet=", wallet, "reason_code=", reason_code)

    print("[publish] done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
