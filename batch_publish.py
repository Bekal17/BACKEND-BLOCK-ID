"""
BlockID batch publish: load active wallets from Step 2 tracking DB, run analytics
(scan -> risk -> trust), publish trust score for each via publish_one_wallet,
then update last_score/last_risk in DB.

Cron-ready: run with `python batch_publish.py` (calls run_batch_once()).
Uses backend_blockid.analytics and backend_blockid.api_server.db_wallet_tracking.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time

# Run from project root so backend_blockid is importable
if __name__ == "__main__" and __package__ is None:
    _root = os.path.abspath(os.path.dirname(__file__))
    if _root not in sys.path:
        sys.path.insert(0, _root)

from pathlib import Path

from backend_blockid.analytics.analytics_pipeline import run_wallet_analysis
from backend_blockid.api_server.db_wallet_tracking import (
    init_db,
    load_active_wallets,
    update_wallet_score,
)
from backend_blockid.blockid_logging import get_logger
from backend_blockid.database import get_database

logger = get_logger(__name__)

DEFAULT_SCORE = 75
BATCH_DELAY_SEC = float(os.getenv("BATCH_DELAY_SEC", "1.5").strip() or "1.5")


def _default_score() -> int:
    """Score to use when wallet has no last_score. From env BATCH_DEFAULT_SCORE or SCORE or 75."""
    raw = (os.getenv("BATCH_DEFAULT_SCORE") or os.getenv("SCORE") or str(DEFAULT_SCORE)).strip()
    try:
        return max(0, min(100, int(raw)))
    except ValueError:
        return DEFAULT_SCORE


def _risk_label_to_u8(risk_label: str) -> int:
    """Map analytics risk_label to oracle u8: LOW=0, MEDIUM=1, HIGH=2."""
    m = (risk_label or "").strip().upper()
    if m == "LOW":
        return 0
    if m == "MEDIUM":
        return 1
    if m == "HIGH":
        return 2
    return 0


def _publish_wallet(
    wallet: str,
    score: int,
    risk_level: int | None = None,
) -> tuple[bool, int | None, int | None]:
    """
    Call publish_one_wallet.py for one wallet, score, and optional risk (0-3).
    Returns (success, stored_score, stored_risk).
    """
    cmd = [sys.executable, "publish_one_wallet.py", wallet, str(score)]
    if risk_level is not None:
        cmd.append(str(max(0, min(3, risk_level))))
    logger.info("batch_publish_start", wallet=wallet[:16] + "...", score=score, risk_level=risk_level)
    try:
        result = subprocess.run(
            cmd,
            cwd=os.path.abspath(os.path.dirname(__file__)),
            capture_output=True,
            text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        logger.error("batch_publish_timeout", wallet=wallet[:16] + "...")
        return False, None, None
    except Exception as e:
        logger.exception("batch_publish_subprocess_error", wallet=wallet[:16] + "...", error=str(e))
        return False, None, None

    if result.returncode != 0:
        logger.warning(
            "batch_publish_failed",
            wallet=wallet[:16] + "...",
            returncode=result.returncode,
            stderr=(result.stderr or "")[:200],
        )
        return False, None, None

    # Parse stored_score and stored_risk from stdout (e.g. "stored_score=88 stored_risk=0")
    stored_score: int | None = None
    stored_risk: int | None = None
    for line in (result.stdout or "").splitlines():
        m = re.search(r"stored_score=(\d+)", line)
        if m:
            stored_score = int(m.group(1))
        m = re.search(r"stored_risk=(\d+)", line)
        if m:
            stored_risk = int(m.group(1))

    if stored_score is None:
        stored_score = score
    if stored_risk is None:
        stored_risk = 0
    logger.info(
        "batch_publish_success",
        wallet=wallet[:16] + "...",
        stored_score=stored_score,
        stored_risk=stored_risk,
    )
    return True, stored_score, stored_risk


def run_batch_once() -> tuple[int, int]:
    """
    Load active wallets, run analytics (scan -> risk -> trust) per wallet, publish
    score and risk to oracle, update DB on success. Returns (success_count, fail_count).
    """
    init_db()
    wallets = load_active_wallets()
    if not wallets:
        logger.info("batch_publish_no_wallets", message="No active wallets in tracking DB")
        return 0, 0

    success_count = 0
    fail_count = 0

    for wallet in wallets:
        logger.info("analysis_started", wallet=wallet[:16] + "...")
        try:
            analysis = run_wallet_analysis(wallet)
        except Exception as e:
            logger.warning("analysis_failed", wallet=wallet[:16] + "...", error=str(e))
            fail_count += 1
            time.sleep(BATCH_DELAY_SEC)
            continue

        score = analysis.get("score", _default_score())
        risk_label = analysis.get("risk_label") or "LOW"
        logger.info("analysis_result", wallet=wallet[:16] + "...", score=score, risk_label=risk_label)

        risk_u8 = _risk_label_to_u8(risk_label)
        ok, stored_score, stored_risk = _publish_wallet(wallet, score, risk_u8)
        if ok and stored_score is not None:
            try:
                reason_codes = analysis.get("reason_codes") or []
                update_wallet_score(wallet, stored_score, risk_label, reason_codes=reason_codes)
                db_path = Path((os.getenv("DB_PATH") or "blockid.db").strip() or "blockid.db")
                main_db = get_database(db_path)
                main_db.insert_trust_score(
                    wallet,
                    float(stored_score),
                    int(time.time()),
                    metadata={"risk": risk_u8, "reason_codes": reason_codes},
                )
                logger.debug("batch_publish_score_updated", wallet=wallet[:16] + "...", score=stored_score)
            except Exception as e:
                logger.warning("batch_publish_update_failed", wallet=wallet[:16] + "...", error=str(e))
            success_count += 1
        else:
            fail_count += 1
        time.sleep(BATCH_DELAY_SEC)

    logger.info("batch_publish_done", success=success_count, failed=fail_count, total=len(wallets))
    return success_count, fail_count


def main() -> int:
    """Entrypoint for cron or CLI: run one batch and exit with 0 if all ok, 1 if any failed."""
    try:
        success, failed = run_batch_once()
        return 0 if failed == 0 else 1
    except Exception as e:
        logger.exception("batch_publish_error", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
