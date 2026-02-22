from typing import List, Dict
import sqlite3
import csv

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.config import DB_PATH

logger = get_logger(__name__)


def solscan_link(tx_hash: str | None, network: str = "devnet") -> str | None:
    if not tx_hash:
        return None
    if network == "devnet":
        return f"https://solscan.io/tx/{tx_hash}?cluster=devnet"
    return f"https://solscan.io/tx/{tx_hash}"


def get_wallet_reasons(wallet: str) -> List[Dict]:
    """
    Return all reasons for a wallet including tx proof.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT reason_code, weight, confidence_score, tx_hash, tx_link
        FROM wallet_reasons
        WHERE wallet=?
        ORDER BY id DESC
    """, (wallet,))

    rows = cur.fetchall()
    conn.close()

    reasons = []
    for r in rows:
        tx_hash = r["tx_hash"]
        tx_link = r["tx_link"] or (
            solscan_link(tx_hash, network="devnet") if tx_hash else None
        )

        reasons.append({
            "code": r["reason_code"],
            "weight": r["weight"],
            "confidence": r["confidence_score"],
            "tx_hash": tx_hash,
            "solscan": tx_link,
        })

    logger.info("wallet_reasons_loaded", wallet=wallet, count=len(reasons))
    return reasons


def insert_wallet_reason(
    wallet: str,
    reason_code: str,
    weight: int,
    confidence: float = 1.0,
    tx_hash: str | None = None,
    tx_link: str | None = None,
) -> None:
    """
    Insert wallet reason safely (ignore duplicates).
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute(
        """
        SELECT 1
        FROM wallet_reasons
        WHERE wallet=? AND reason_code=? AND COALESCE(tx_hash,'')=COALESCE(?, '')
        """,
        (wallet, reason_code, tx_hash),
    )
    if cur.fetchone():
        conn.close()
        return

    cur.execute("""
        INSERT OR IGNORE INTO wallet_reasons(
            wallet, reason_code, weight, confidence_score, tx_hash, tx_link
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (wallet, reason_code, weight, confidence, tx_hash, tx_link))

    conn.commit()
    conn.close()


def save_wallet_scores_from_csv(csv_path: str):
    """
    Minimal safe version:
    Read wallet_scores.csv and insert into trust_scores table.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = row.get("wallet")
            score = row.get("score")

            if wallet and score:
                cur.execute(
                    "INSERT OR REPLACE INTO trust_scores(wallet, score) VALUES (?, ?)",
                    (wallet, int(score)),
                )

    conn.commit()
    conn.close()