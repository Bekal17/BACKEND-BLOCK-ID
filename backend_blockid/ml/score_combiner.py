"""
Combine ML scores with reason penalties into final wallet scores.

Loads:
  - wallet_ml_scores.csv (wallet, ml_score)
  - reason_penalties.csv (wallet, penalty_score)
  - reason_codes.csv (wallet, reason_codes or top_reasons) [optional, for reason_codes column]

Output:
  - wallet_scores.csv (wallet, score, risk_level, reason_codes)

Formula: final_score = ml_score - penalty, clamped 0–100
Risk levels: score > 80 → LOW; 50–80 → MEDIUM; < 50 → HIGH

Usage:
  py -m backend_blockid.ml.score_combiner
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

_ML_DIR = Path(__file__).resolve().parent
_DATA_DIR = _ML_DIR.parent / "data"
WALLET_ML_SCORES_CSV = _DATA_DIR / "wallet_ml_scores.csv"
REASON_PENALTIES_CSV = _DATA_DIR / "reason_penalties.csv"
REASON_CODES_CSV = _DATA_DIR / "reason_codes.csv"
WALLET_SCORES_CSV = _DATA_DIR / "wallet_scores.csv"


def _score_to_risk_level(score: float) -> str:
    """score > 80 → LOW; 50–80 → MEDIUM; < 50 → HIGH."""
    s = max(0.0, min(100.0, score))
    if s > 80:
        return "LOW"
    if s >= 50:
        return "MEDIUM"
    return "HIGH"


def _load_ml_scores(path: Path) -> dict[str, float]:
    """Load wallet_ml_scores.csv; return {wallet: ml_score}."""
    out: dict[str, float] = {}
    if not path.exists():
        logger.warning("score_combiner_ml_scores_missing", path=str(path))
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            try:
                score = float(row.get("ml_score") or row.get("score", 0))
            except (TypeError, ValueError):
                score = 0.0
            out[w] = max(0.0, min(100.0, score))
    logger.info("score_combiner_ml_scores_loaded", path=str(path), count=len(out))
    return out


def _load_reason_penalties(path: Path) -> dict[str, int]:
    """Load reason_penalties.csv; return {wallet: penalty_score}. Missing = 0."""
    out: dict[str, int] = {}
    if not path.exists():
        logger.debug("score_combiner_penalties_missing", path=str(path))
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            try:
                p = int(row.get("penalty_score", 0) or 0)
            except (TypeError, ValueError):
                p = 0
            out[w] = max(0, p)
    logger.info("score_combiner_penalties_loaded", path=str(path), count=len(out))
    return out


def _load_reason_codes(path: Path) -> dict[str, str]:
    """Load reason_codes.csv; return {wallet: "CODE1,CODE2,..."}. Missing = ""."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            w = (row.get("wallet") or "").strip()
            if not w:
                continue
            raw = row.get("reason_codes") or row.get("top_reasons") or row.get("top_3_reasons") or ""
            if isinstance(raw, str) and raw.strip().startswith("["):
                try:
                    arr = json.loads(raw)
                    codes = ",".join(str(c).strip() for c in arr) if isinstance(arr, list) else raw
                except json.JSONDecodeError:
                    codes = raw.replace('"', "").strip("[]").replace(",", ", ")
            else:
                codes = raw.replace('"', "").strip()
            out[w] = codes
    return out


def run(
    ml_scores_path: Path | None = None,
    penalties_path: Path | None = None,
    reason_codes_path: Path | None = None,
    output_path: Path | None = None,
) -> int:
    """
    Combine ML scores and penalties; write wallet_scores.csv.
    Returns number of rows written.
    """
    ml_path = ml_scores_path or WALLET_ML_SCORES_CSV
    pen_path = penalties_path or REASON_PENALTIES_CSV
    rc_path = reason_codes_path or REASON_CODES_CSV
    out_path = output_path or WALLET_SCORES_CSV

    if not ml_path.exists():
        logger.error("score_combiner_no_ml_scores", path=str(ml_path))
        return 0

    ml_scores = _load_ml_scores(ml_path)
    penalties = _load_reason_penalties(pen_path)
    reason_codes_map = _load_reason_codes(rc_path)

    rows: list[dict] = []
    for wallet, ml_score in ml_scores.items():
        penalty = penalties.get(wallet, 0)
        final_score = max(0.0, min(100.0, ml_score - penalty))
        risk_level = _score_to_risk_level(final_score)
        reason_codes = reason_codes_map.get(wallet, "")

        rows.append({
            "wallet": wallet,
            "score": round(final_score, 2),
            "risk_level": risk_level,
            "reason_codes": reason_codes,
        })

    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["wallet", "score", "risk_level", "reason_codes"])
        w.writeheader()
        w.writerows(rows)

    logger.info("score_combiner_done", path=str(out_path), rows=len(rows))
    print(f"[score_combiner] wrote {len(rows)} rows to {out_path}")
    return len(rows)


def main() -> int:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not WALLET_ML_SCORES_CSV.exists():
        print(f"[score_combiner] ERROR: {WALLET_ML_SCORES_CSV} not found")
        return 1
    n = run()
    return 0 if n > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
