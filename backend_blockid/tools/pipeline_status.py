#!/usr/bin/env python3
"""
BlockID pipeline status — pretty console table of key metrics.

Reads from:
  - wallet_scores.csv
  - reason_penalties.csv
  - reason_codes.csv / wallet_reason_codes.csv
  - tracked_wallets (db_wallet_tracking)

Outputs: wallets analyzed, suspicious wallets, drainers detected,
         average score, highest risk wallet, publish success rate.

Usage:
  py -m backend_blockid.tools.pipeline_status
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

_TOOLS_DIR = Path(__file__).resolve().parent
_BACKEND_DIR = _TOOLS_DIR.parent
_DATA_DIR = _BACKEND_DIR / "data"

WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"
REASON_PENALTIES_CSV = _DATA_DIR / "reason_penalties.csv"
REASON_CODES_CSV = _DATA_DIR / "reason_codes.csv"
WALLET_REASON_CODES_CSV = _DATA_DIR / "wallet_reason_codes.csv"

# Thresholds
SUSPICIOUS_RISK_THRESHOLD = 30  # risk_score >= 30 = suspicious
SUSPICIOUS_SCAM_PROB_THRESHOLD = 0.3  # scam_probability >= 0.3 = suspicious

SEP = "=" * 52
SEP_THIN = "-" * 52


def _log(msg: str) -> None:
    print(f"[pipeline_status] {msg}")


def _load_wallet_scores(path: Path) -> list[dict]:
    """Load wallet_scores.csv. Returns list of {wallet, risk_score, scam_probability, trust_score, reason_code}."""
    rows: list[dict] = []
    if not path.exists():
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            try:
                risk = int(float(row.get("risk_score", 0)))
            except (TypeError, ValueError):
                risk = 0
            try:
                scam = float(row.get("scam_probability", 0))
            except (TypeError, ValueError):
                scam = 0.0
            trust = max(0, min(100, 100 - risk))
            rows.append({
                "wallet": w,
                "risk_score": risk,
                "scam_probability": scam,
                "trust_score": trust,
                "reason_code": (row.get("reason_code") or "").strip(),
            })
    return rows


def _load_reason_codes(path: Path) -> list[dict]:
    """Load reason_codes.csv or wallet_reason_codes.csv. Returns rows with parsed reason_codes."""
    rows: list[dict] = []
    if not path.exists():
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            raw = row.get("reason_codes") or row.get("top_3_reasons") or ""
            codes: list[str] = []
            if raw.startswith("["):
                try:
                    codes = json.loads(raw.replace("'", '"'))
                except (json.JSONDecodeError, TypeError):
                    pass
            elif raw:
                codes = [s.strip() for s in raw.split(",") if s.strip()]
            rows.append({"wallet": w, "reason_codes": codes})
    return rows


def _load_reason_penalties(path: Path) -> list[dict]:
    """Load reason_penalties.csv."""
    rows: list[dict] = []
    if not path.exists():
        return rows
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            try:
                penalty = int(float(row.get("penalty_score", 0)))
            except (TypeError, ValueError):
                penalty = 0
            rows.append({"wallet": w, "penalty_score": penalty})
    return rows


def _count_drainers(reason_rows: list[dict]) -> int:
    """Count wallets with DRAINER_INTERACTION in reason codes."""
    count = 0
    for r in reason_rows:
        if "DRAINER_INTERACTION" in r.get("reason_codes", []):
            count += 1
    return count


def _get_tracked_publish_stats() -> tuple[int, int]:
    """Return (published_count, total_tracked) from tracked_wallets. (0,0) if DB unavailable."""
    try:
        from backend_blockid.api_server.db_wallet_tracking import init_db, list_wallets
        init_db()
        wallets = list_wallets()
        total = len(wallets)
        published = sum(1 for w in wallets if w.get("last_score") is not None)
        return published, total
    except Exception as e:
        logger.debug("pipeline_status_db_unavailable", error=str(e))
        return 0, 0


def main() -> int:
    _log(SEP)
    _log("Pipeline Status")
    _log(SEP)

    # Wallets analyzed
    scores = _load_wallet_scores(WALLET_SCORES_CSV)
    wallets_analyzed = len(scores)

    # Suspicious wallets (high risk or high scam prob)
    suspicious = sum(
        1 for r in scores
        if r["risk_score"] >= SUSPICIOUS_RISK_THRESHOLD
        or r["scam_probability"] >= SUSPICIOUS_SCAM_PROB_THRESHOLD
    )

    # Drainers detected (from reason_codes or wallet_reason_codes)
    reason_rows: list[dict] = []
    for p in [REASON_CODES_CSV, WALLET_REASON_CODES_CSV]:
        reason_rows.extend(_load_reason_codes(p))
    drainers = _count_drainers(reason_rows)

    # Reason penalties (used for context)
    penalty_rows = _load_reason_penalties(REASON_PENALTIES_CSV)

    # Average score (trust 0-100, higher = better)
    avg_score = sum(r["trust_score"] for r in scores) / len(scores) if scores else 0.0

    # Highest risk wallet (highest risk_score = lowest trust)
    highest_risk = ""
    if scores:
        worst = max(scores, key=lambda r: r["risk_score"])
        highest_risk = f"{worst['wallet'][:20]}... (risk={worst['risk_score']})"

    # Publish success rate
    published, total_tracked = _get_tracked_publish_stats()
    if total_tracked > 0:
        rate = 100.0 * published / total_tracked
        publish_rate = f"{rate:.1f}% ({published}/{total_tracked})"
    else:
        publish_rate = "N/A (no tracked wallets)"

    # Drainers: also include high-penalty wallets from reason_penalties (penalty >= 20 ~ DRAINER weight)
    drainer_wallets = {r["wallet"] for r in reason_rows if "DRAINER_INTERACTION" in r.get("reason_codes", [])}
    for r in penalty_rows:
        if r.get("penalty_score", 0) >= 20:
            drainer_wallets.add(r["wallet"])
    drainers = len(drainer_wallets)

    # Build table
    table_rows = [
        ("Wallets analyzed", str(wallets_analyzed)),
        ("Suspicious wallets", str(suspicious)),
        ("Drainers detected", str(drainers)),
        ("Average score", f"{avg_score:.1f}"),
        ("Highest risk wallet", (highest_risk or "—")[:44]),
        ("Publish success rate", publish_rate),
    ]

    label_w = 22
    value_w = 32

    _log("")
    border = "+" + "-" * (label_w + 2) + "+" + "-" * (value_w + 2) + "+"
    _log(border)
    _log(f"| {'Metric':<{label_w}} | {'Value':<{value_w}} |")
    _log(border)
    for label, value in table_rows:
        _log(f"| {label:<{label_w}} | {str(value):<{value_w}} |")
    _log(border)
    _log("")
    _log(f"Sources: {WALLET_SCORES_CSV.name}, {REASON_PENALTIES_CSV.name}, tracked_wallets")
    _log(SEP)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
