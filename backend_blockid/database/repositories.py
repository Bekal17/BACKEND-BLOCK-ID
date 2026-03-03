from typing import List, Dict
import csv
import sqlite3
import time

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.config import DB_PATH
from backend_blockid.tools.time_utils import days_since

logger = get_logger(__name__)


def now_ts() -> int:
    return int(time.time())


def solscan_link(tx_hash: str | None, network: str = "devnet") -> str | None:
    if not tx_hash:
        return None
    if network == "devnet":
        return f"https://solscan.io/tx/{tx_hash}?cluster=devnet"
    return f"https://solscan.io/tx/{tx_hash}"


def _default_confidence(reason_code: str | None) -> float:
    if reason_code == "CLEAN_HISTORY":
        return 0.7
    if reason_code == "SCAM_CLUSTER_MEMBER":
        return 0.9
    return 1.0


def _clamp_confidence(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


_DAYS_90_SEC = 90 * 24 * 60 * 60


def _ensure_wallet_reasons_created_at(cur: sqlite3.Cursor) -> None:
    """Ensure created_at column exists for time decay."""
    try:
        cur.execute("ALTER TABLE wallet_reasons ADD COLUMN created_at INTEGER")
    except sqlite3.OperationalError:
        pass  # Column already exists


def _ensure_wallet_reasons_optional_columns(cur: sqlite3.Cursor) -> None:
    """Ensure optional columns exist (for schemas that only have id, wallet, reason_code, weight)."""
    cols = [("confidence_score", "REAL"), ("tx_hash", "TEXT"), ("tx_link", "TEXT")]
    for col, typ in cols:
        try:
            cur.execute(f"ALTER TABLE wallet_reasons ADD COLUMN {col} {typ}")
        except sqlite3.OperationalError:
            pass


def get_wallet_reasons(wallet: str) -> List[Dict]:
    """
    Return all reasons for a wallet including tx proof.
    Time decay: reasons older than 90 days have weight halved.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    _ensure_wallet_reasons_created_at(cur)
    _ensure_wallet_reasons_optional_columns(cur)

    cur.execute("""
        SELECT reason_code, weight, confidence_score, tx_hash, tx_link, created_at
        FROM wallet_reasons
        WHERE wallet=?
        ORDER BY id DESC
    """, (wallet,))

    rows = cur.fetchall()
    conn.close()

    now_ts = int(time.time())

    from backend_blockid.ml.reason_codes import get_reason_weights

    weights = get_reason_weights()
    HIGH_RISK_CODES = {
        "SCAM_CLUSTER_MEMBER",
        "SCAM_CLUSTER_MEMBER_SMALL",
        "SCAM_CLUSTER_MEMBER_LARGE",
        "RUG_PULL_DEPLOYER",
        "BLACKLISTED_CREATOR",
        "DRAINER_FLOW_DETECTED",
        "DRAINER_FLOW",
        "MEGA_DRAINER",
    }

    reasons = []
    for r in rows:
        keys = r.keys()
        tx_hash = r["tx_hash"] if "tx_hash" in keys else None
        tx_link = (r["tx_link"] or (solscan_link(tx_hash, network="devnet") if tx_hash else None)) if "tx_link" in keys else None
        db_weight = r["weight"] or 0

        # Always normalize from reason weights; fallback to DB value
        weight = weights.get(r["reason_code"], db_weight)
        if r["reason_code"] in HIGH_RISK_CODES:
            weight = -abs(weight)

        created_at = r["created_at"] if "created_at" in keys else None
        if created_at is not None and (now_ts - int(created_at)) > _DAYS_90_SEC:
            weight = int(weight / 2)  # Time decay: halve weight if older than 90 days

        days_old_val = days_since(created_at) if created_at is not None else 0

        reasons.append({
            "code": r["reason_code"],
            "weight": weight,
            "confidence": r["confidence_score"] if "confidence_score" in keys else None,
            "tx_hash": tx_hash,
            "solscan": tx_link,
            "days_old": days_old_val,
        })

    for reason in reasons:
        confidence = reason.get("confidence")
        if confidence is None:
            confidence = _default_confidence(reason.get("code"))
        reason["confidence"] = _clamp_confidence(confidence)

    logger.info(
        "reason_weights_normalized",
        wallet=wallet,
        reasons=[(rc["code"], rc["weight"]) for rc in reasons],
    )
    logger.info("wallet_reasons_loaded", wallet=wallet, count=len(reasons))
    return reasons


def _ensure_wallet_reasons_unique_index(cur: sqlite3.Cursor) -> None:
    """Credit-score safety: prevent repeated (wallet, reason_code) inserts."""
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_wallet_reason_unique
        ON wallet_reasons(wallet, reason_code)
    """)


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
    Credit-score safety: skip if (wallet, reason_code) already exists.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    _ensure_wallet_reasons_unique_index(cur)
    _ensure_wallet_reasons_created_at(cur)
    _ensure_wallet_reasons_optional_columns(cur)

    cur.execute(
        "SELECT 1 FROM wallet_reasons WHERE wallet=? AND reason_code=?",
        (wallet, reason_code),
    )
    if cur.fetchone():
        conn.close()
        return

    created_at = int(time.time())
    cur.execute("""
        INSERT OR IGNORE INTO wallet_reasons(
            wallet, reason_code, weight, confidence_score, tx_hash, tx_link, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (wallet, reason_code, weight, confidence, tx_hash, tx_link, created_at))

    conn.commit()
    conn.close()


def add_wallet(wallet: str):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO tracked_wallets(wallet) VALUES (?)",
        (wallet,),
    )
    conn.commit()
    conn.close()


def update_wallet_score(
    wallet: str,
    score: int,
    risk_level: str,
    metadata: str = "{}",
    *,
    wallet_age_days: int = 0,
    last_scam_days: int = 9999,
    decay_adjustment: int = 0,
    graph_distance: int = 999,
    graph_penalty: int = 0,
    time_weighted_penalty: int = 0,
):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    computed_at = now_ts()
    print("DEBUG trust_scores write:", wallet, computed_at)

    # Capture old score for badge change detection
    cur.execute("SELECT score FROM trust_scores WHERE wallet = ?", (wallet,))
    row = cur.fetchone()
    old_score = float(row["score"]) if row and row["score"] is not None else None

    is_test_wallet = 1 if wallet.startswith("TEST_") else 0

    # Ensure columns exist
    for col, default in [
        ("wallet_age_days", 0),
        ("last_scam_days", 9999),
        ("decay_adjustment", 0),
        ("graph_distance", 999),
        ("graph_penalty", 0),
        ("time_weighted_penalty", 0),
        ("is_test_wallet", 0),
    ]:
        try:
            cur.execute(f"ALTER TABLE trust_scores ADD COLUMN {col} INTEGER DEFAULT {default}")
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        INSERT INTO trust_scores(
            wallet,
            score,
            risk_level,
            metadata_json,
            computed_at,
            updated_at,
            wallet_age_days,
            last_scam_days,
            decay_adjustment,
            graph_distance,
            graph_penalty,
            time_weighted_penalty,
            is_test_wallet
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet)
        DO UPDATE SET
            score = excluded.score,
            risk_level = excluded.risk_level,
            metadata_json = excluded.metadata_json,
            computed_at = ?,
            updated_at = CURRENT_TIMESTAMP,
            wallet_age_days = excluded.wallet_age_days,
            last_scam_days = excluded.last_scam_days,
            decay_adjustment = excluded.decay_adjustment,
            graph_distance = excluded.graph_distance,
            graph_penalty = excluded.graph_penalty,
            time_weighted_penalty = excluded.time_weighted_penalty,
            is_test_wallet = excluded.is_test_wallet
        """,
        (
            wallet,
            score,
            risk_level,
            metadata,
            computed_at,
            wallet_age_days,
            last_scam_days,
            decay_adjustment,
            graph_distance,
            graph_penalty,
            time_weighted_penalty,
            is_test_wallet,
            computed_at,
        ),
    )

    # Record badge change when score crosses threshold
    try:
        from backend_blockid.tools.badge_engine import record_badge_if_changed
        import time
        record_badge_if_changed(wallet, old_score, float(score), int(time.time()), conn=conn)
    except Exception:
        pass

    conn.commit()
    conn.close()


def save_wallet_scores_from_csv(csv_path: str):
    """
    Minimal safe version:
    Read wallet_scores.csv and insert into trust_scores table.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    updated = 0

    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            wallet = row.get("wallet")
            score = row.get("final_score") or row.get("score")

            if wallet and score:
                print("Saving wallet:", wallet, "score:", score)
                computed_at = now_ts()
                print("DEBUG trust_scores write:", wallet, computed_at)
                cur.execute("SELECT 1 FROM trust_scores WHERE wallet=?", (wallet,))
                exists = cur.fetchone() is not None
                cur.execute(
                    """
                    INSERT INTO trust_scores(
                        wallet,
                        score,
                        computed_at,
                        updated_at
                    )
                    VALUES (
                        ?, ?, ?, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT(wallet)
                    DO UPDATE SET
                        score = excluded.score,
                        computed_at = ?,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    (wallet, int(score), computed_at, computed_at),
                )
                if exists:
                    updated += 1

    conn.commit()
    conn.close()
    return updated


def save_wallet_risk_probability(wallet: str, prior: float, posterior: float, reasons: list) -> None:
    """
    Save Bayesian risk calculation logs.
    reasons = [{code, likelihood, confidence}]
    """
    from backend_blockid.database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_risk_probabilities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            wallet TEXT,
            prior REAL,
            posterior REAL,
            reason_code TEXT,
            likelihood REAL,
            confidence REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for r in reasons:
        cur.execute("""
            INSERT INTO wallet_risk_probabilities
            (wallet, prior, posterior, reason_code, likelihood, confidence)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            wallet,
            prior,
            posterior,
            r.get("code"),
            r.get("likelihood"),
            r.get("confidence"),
        ))

    conn.commit()
    conn.close()


def save_wallet_meta(meta):
    from backend_blockid.database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wallet_meta (
            wallet TEXT PRIMARY KEY,
            first_tx_ts INTEGER,
            last_tx_ts INTEGER,
            wallet_age_days INTEGER,
            last_scam_tx_ts INTEGER,
            last_scan_time INTEGER,
            cluster_id TEXT,
            is_test_wallet INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        INSERT INTO wallet_meta (
            wallet, first_tx_ts, last_tx_ts,
            wallet_age_days, last_scam_tx_ts,
            last_scan_time, cluster_id, is_test_wallet
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(wallet) DO UPDATE SET
            first_tx_ts=excluded.first_tx_ts,
            last_tx_ts=excluded.last_tx_ts,
            wallet_age_days=excluded.wallet_age_days,
            last_scam_tx_ts=excluded.last_scam_tx_ts,
            last_scan_time=excluded.last_scan_time,
            cluster_id=excluded.cluster_id,
            is_test_wallet=excluded.is_test_wallet
    """, (
        meta["wallet"],
        meta.get("first_tx_ts"),
        meta.get("last_tx_ts"),
        meta.get("wallet_age_days"),
        meta.get("last_scam_tx_ts"),
        meta.get("last_scan_time"),
        meta.get("cluster_id"),
        meta.get("is_test_wallet", 0),
    ))

    conn.commit()
    conn.close()


def get_wallet_meta(wallet):
    from backend_blockid.database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT first_tx_ts, last_tx_ts FROM wallet_meta WHERE wallet=?", (wallet,))
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "first_tx_ts": row[0],
        "last_tx_ts": row[1],
    }


def get_cluster_wallets(cluster_id):
    from backend_blockid.database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT wallet FROM wallet_clusters WHERE cluster_id=?",
        (cluster_id,),
    )

    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_wallet_cluster_id(wallet: str) -> str | None:
    from backend_blockid.database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT cluster_id FROM wallet_clusters WHERE wallet=? LIMIT 1",
        (wallet,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_all_active_clusters() -> list[str]:
    from backend_blockid.database.connection import get_connection

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT cluster_id FROM wallet_clusters")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]