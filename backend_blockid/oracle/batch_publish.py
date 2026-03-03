import csv
import io
import os
import sys

try:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")
except Exception:
    pass

from backend_blockid.database import get_database
from backend_blockid.database.connection import get_connection
from backend_blockid.ml.score_decay import run_decay_for_all_wallets
from backend_blockid.oracle.pre_publish_check import check_test_wallets
from backend_blockid.oracle.publish_one_wallet import main as publish_one
from backend_blockid.tools.review_queue_engine import is_pending_review
from backend_blockid.blockid_logging import get_logger
from solders.pubkey import Pubkey

CSV_PATH = "backend_blockid/data/wallets.csv"
MAX_WALLETS = int(os.getenv("BLOCKID_MAX_WALLETS", "1000"))

logger = get_logger(__name__)


def load_wallet_from_csv(row):
    """Load and validate wallet from CSV row. Returns Pubkey or raises ValueError."""
    wallet = row.get("wallet", "").strip()
    if not wallet:
        raise ValueError("Empty wallet in CSV")
    return Pubkey.from_string(wallet)


def load_score_risk(row, default_score=50, default_risk=1):
    try:
        score = int(row.get("score", default_score))
        risk = int(row.get("risk", default_risk))
    except Exception:
        score, risk = default_score, default_risk

    score = max(0, min(score, 100))
    risk = max(0, min(risk, 5))

    return score, risk


TEST_MODE = os.getenv("BLOCKID_TEST_MODE", "0") == "1"
DRY_RUN = os.getenv("BLOCKID_DRY_RUN", "0") == "1"


def run_batch():
    print("[batch_publish] START")
    success = 0
    failed = 0
    db = get_database()

    check_test_wallets()

    # Score decay: recover wallet trust over time (run daily before publish)
    decayed = run_decay_for_all_wallets()
    if decayed > 0:
        print(f"[batch_publish] score_decay updated {decayed} wallet(s)")

    total_wallets = 0
    try:
        with open(CSV_PATH, newline="") as f_count:
            total_wallets = sum(1 for _ in csv.DictReader(f_count))
    except Exception:
        total_wallets = 0

    selected = min(total_wallets, MAX_WALLETS) if MAX_WALLETS > 0 else total_wallets
    logger.info(
        "wallet_selection",
        selected=selected,
        limit=MAX_WALLETS,
        test_mode=TEST_MODE,
    )

    counter = 0
    with open(CSV_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if MAX_WALLETS > 0 and counter >= MAX_WALLETS:
                print(f"[batch_publish] limit reached {MAX_WALLETS}")
                break
            print("CSV reasons:", row.get("reason_codes"))
            try:
                wallet_pubkey = load_wallet_from_csv(row)
            except ValueError as e:
                print(f"[SKIP] Invalid row: {e}")
                continue
            wallet = str(wallet_pubkey)
            conn = get_connection()
            try:
                cur = conn.cursor()
                cur.execute("SELECT is_test_wallet FROM trust_scores WHERE wallet = ? LIMIT 1", (wallet,))
                row_ts = cur.fetchone()
                is_test = (1 if row_ts[0] else 0) if row_ts is not None else (1 if wallet.startswith("TEST_") else 0)
            except Exception:
                is_test = 1 if wallet.startswith("TEST_") else 0
            finally:
                conn.close()
            wallet_meta = {"is_test_wallet": is_test}

            if wallet_meta.get("is_test_wallet"):
                print(f"[SKIP TEST WALLET] {wallet}")
                continue

            # BLOCKID_TEST_MODE: load score/risk from CSV when TEST_MODE=1
            if TEST_MODE:
                try:
                    score = int(row.get("score", 50))
                    risk = int(row.get("risk", 1))
                except Exception:
                    score, risk = 50, 1
            else:
                timeline = db.get_trust_score_timeline(wallet, limit=1)
                if timeline:
                    record = timeline[0]
                    final_score = getattr(record, "final_score", None)
                    score_val = final_score if final_score is not None else getattr(record, "score", 50)
                    score = int(score_val)
                    risk = 1
                    record_risk = getattr(record, "risk_level", None)
                    if record_risk is not None:
                        try:
                            risk = int(record_risk)
                        except Exception:
                            risk = 1
                    if record.metadata_json:
                        try:
                            import json

                            meta = json.loads(record.metadata_json)
                            risk = int(meta.get("risk", 1))
                        except Exception:
                            risk = 1
                else:
                    score, risk = 50, 1

            if DRY_RUN:
                print(f"[DRY RUN] Skip publish for {wallet}")
                continue
            if is_pending_review(wallet):
                print(f"[publish] skipped due to manual review: {wallet}")
                continue
            counter += 1
            # BLOCKID_VALIDATION: strict validation before publish
            try:
                score = int(score)
                risk = int(risk)
            except Exception:
                print(f"[SKIP] Invalid score/risk type for {wallet}: score={score}, risk={risk}")
                continue
            if not (0 <= score <= 100):
                print(f"[SKIP] Invalid score {score} for {wallet}")
                continue
            if not (0 <= risk <= 5):
                print(f"[SKIP] Invalid risk {risk} for {wallet}")
                continue
            # BLOCKID_DEBUG
            print(f"[DEBUG] wallet={wallet} score={score} risk={risk}")
            try:
                publish_one(wallet=wallet, score=score, risk=risk)
                success += 1
            except Exception as e:
                print("FAILED:", wallet, e)
                failed += 1

    print("SUCCESS:", success)
    print("FAILED:", failed)
    print("[batch_publish] END")
    if failed > 0:
        raise Exception("Batch publish failures detected")


if __name__ == "__main__":
    try:
        run_batch()
        print("[batch_publish] Completed successfully")
        sys.exit(0)
    except Exception as e:
        print("[batch_publish] FATAL ERROR:", e)
        sys.exit(1)


def validate_wallet(w):
    try:
        Pubkey.from_string(w)
        return True
    except Exception:
        return False
