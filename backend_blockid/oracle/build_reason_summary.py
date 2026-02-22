"""
Aggregate wallet_reason_evidence into wallet reason summary CSV.

Loads all evidence from DB, groups by wallet and reason_code, counts frequency,
computes reason_risk_score from weighted sum, saves to reason_codes.csv.

Usage:
  py -m backend_blockid.oracle.build_reason_summary
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

from backend_blockid.blockid_logging import get_logger
from backend_blockid.api_server.db_wallet_tracking import init_db, list_reason_evidence

logger = get_logger(__name__)

_SCRIPT_DIR = Path(__file__).resolve().parent
_DATA_DIR = _SCRIPT_DIR.parent / "data"
OUTPUT_CSV = _DATA_DIR / "reason_codes.csv"

# Reason code → risk weight (higher = more severe)
REASON_RISK_WEIGHTS: dict[str, int] = {
    "SCAM_CLUSTER_MEMBER": 40,
    "DRAINER_INTERACTION": 20,
    "HIGH_VALUE_OUTFLOW": 10,
    "NEW_WALLET": 5,
    "LOW_ACTIVITY": 3,
}
DEFAULT_WEIGHT = 0


def _compute_reason_risk_score(reason_freq: dict[str, int]) -> int:
    """Sum of (weight[code] * count) for each reason code."""
    total = 0
    for code, count in reason_freq.items():
        weight = REASON_RISK_WEIGHTS.get(code, DEFAULT_WEIGHT)
        total += weight * count
    return total


def load_all_evidence() -> list[dict]:
    """Load all wallet_reason_evidence rows from DB."""
    return list_reason_evidence(wallet=None, reason_code=None, limit=500_000)


def build_summary(evidence: list[dict]) -> pd.DataFrame:
    """
    Group by wallet and reason_code, count frequency, compute risk score.
    Returns DataFrame with: wallet, reason_codes, reason_freq, top_reasons, reason_risk_score.
    """
    if not evidence:
        return pd.DataFrame(columns=["wallet", "reason_codes", "reason_freq", "top_reasons", "reason_risk_score"])

    # (wallet, reason_code) -> count
    wallet_reasons: dict[str, Counter[str]] = {}
    for row in evidence:
        w = (row.get("wallet") or "").strip()
        code = (row.get("reason_code") or "").strip()
        if not w or not code:
            continue
        if w not in wallet_reasons:
            wallet_reasons[w] = Counter()
        wallet_reasons[w][code] += 1

    rows = []
    for wallet, freq in wallet_reasons.items():
        reason_freq = dict(freq)
        ordered = sorted(reason_freq.keys(), key=lambda k: -reason_freq[k])
        top_reasons = ",".join(ordered[:10])
        risk_score = _compute_reason_risk_score(reason_freq)
        rows.append({
            "wallet": wallet,
            "reason_codes": json.dumps(ordered),
            "reason_freq": json.dumps(reason_freq),
            "top_reasons": top_reasons,
            "reason_risk_score": risk_score,
        })

    return pd.DataFrame(rows)


def main() -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Aggregate wallet_reason_evidence into reason_codes.csv")
    ap.add_argument("--output", type=Path, default=OUTPUT_CSV, help="Output CSV path")
    args = ap.parse_args()

    logger.info("build_reason_summary_start")
    init_db()

    evidence = load_all_evidence()
    logger.info("build_reason_summary_loaded", rows=len(evidence))

    if not evidence:
        logger.warning("build_reason_summary_empty")
        print("[build_reason_summary] No evidence in wallet_reason_evidence; run scan_wallet_transactions first.")
        return 0

    df = build_summary(evidence)
    if df.empty:
        logger.warning("build_reason_summary_no_valid_rows")
        print("[build_reason_summary] No valid evidence rows to aggregate.")
        return 0

    out_path = args.output
    if not out_path.is_absolute():
        out_path = _DATA_DIR / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    logger.info("build_reason_summary_saved", path=str(out_path), wallets=len(df))
    print(f"[build_reason_summary] Saved {len(df)} wallets to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
