"""
Investigation Explorer Badge Panel API for BlockID.

GET /wallet/{wallet}/investigation_badge — detailed badge explanation for investigators.
Used by app.blockidscore.fun and Phantom plugin warning popup.

Future upgrades:
- Graph mini visualization
- Evidence links (Solscan tx)
- PDF report button
- Confidence indicator
"""

from __future__ import annotations

import csv
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.connection import get_connection
from backend_blockid.tools.badge_engine import get_badge

logger = get_logger(__name__)

router = APIRouter(prefix="/wallet", tags=["investigation"])

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

# Short phrases for badge_reason_summary
_REASON_PHRASES = {
    "SCAM_CLUSTER_MEMBER": "cluster link",
    "SCAM_CLUSTER_MEMBER_SMALL": "cluster link",
    "SCAM_CLUSTER_MEMBER_LARGE": "cluster link",
    "DRAINER_INTERACTION": "drainer interaction",
    "DRAINER_FLOW": "drainer flow",
    "DRAINER_FLOW_DETECTED": "drainer activity",
    "MEGA_DRAINER": "mega drainer",
    "RUG_PULL_DEPLOYER": "rug pull deployment",
    "BLACKLISTED_CREATOR": "blacklisted creator",
    "HIGH_RISK_TOKEN_INTERACTION": "high-risk token interaction",
    "HIGH_VOLUME_TO_SCAM": "high-volume transfers to scam",
}


def _load_scam_wallets(conn) -> set[str]:
    scams = set()
    cur = conn.cursor()
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='scam_wallets'")
        if cur.fetchone():
            cur.execute("SELECT wallet FROM scam_wallets")
            for r in cur.fetchall():
                w = (r["wallet"] if hasattr(r, "keys") else r[0]).strip() if r else ""
                if w:
                    scams.add(w)
    except Exception:
        pass
    if not scams and SCAM_WALLETS_CSV.exists():
        with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                w = (row.get("wallet") or "").strip()
                if w:
                    scams.add(w)
    return scams


def _get_cluster_info(conn, wallet: str) -> dict:
    """Return cluster_id, cluster_size, scam_ratio for wallet."""
    cur = conn.cursor()
    result: dict = {"cluster_id": None, "cluster_size": 0, "scam_ratio": 0.0}
    members: list[str] = []

    for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
            if not cur.fetchone():
                continue
            cur.execute(f"SELECT cluster_id FROM {tbl} WHERE wallet = ? LIMIT 1", (wallet.strip(),))
            row = cur.fetchone()
            if not row:
                continue
            cid = row["cluster_id"] if hasattr(row, "keys") else row[0]
            cur.execute(f"SELECT wallet FROM {tbl} WHERE cluster_id = ?", (cid,))
            members = [r["wallet"] if hasattr(r, "keys") else r[0] for r in cur.fetchall() if r]
            result["cluster_id"] = cid
            result["cluster_size"] = len(members)
            break
        except Exception:
            continue

    if result["cluster_size"] > 0 and members:
        scam_wallets = _load_scam_wallets(conn)
        scam_count = sum(1 for w in members if w in scam_wallets)
        result["scam_ratio"] = round(scam_count / result["cluster_size"], 2)

    return result


def _count_suspicious_tx(conn, wallet: str, scam_wallets: set[str]) -> int:
    """Count transactions involving scam wallets (from or to)."""
    if not scam_wallets:
        return 0
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(transactions)")
    cols = {row[1] for row in cur.fetchall()}
    if "from_wallet" not in cols or "to_wallet" not in cols:
        return 0

    placeholders = ",".join("?" * min(len(scam_wallets), 500))
    sus_list = list(scam_wallets)[:500]
    cur.execute(
        f"""
        SELECT COUNT(*) FROM transactions
        WHERE (from_wallet = ? OR to_wallet = ?)
        AND (from_wallet IN ({placeholders}) OR to_wallet IN ({placeholders}))
        """,
        [wallet.strip(), wallet.strip()] + sus_list + sus_list,
    )
    row = cur.fetchone()
    return row[0] or 0


def _get_score_change_30d(conn, wallet: str, current_score: float) -> float | None:
    """Compare current score with score ~30 days ago. Return delta or None."""
    cur = conn.cursor()
    now = int(time.time())
    cutoff = now - (30 * 86400)

    cur.execute("PRAGMA table_info(wallet_history)")
    cols = {row[1] for row in cur.fetchall()}
    if "posterior" in cols:
        cur.execute(
            """
            SELECT COALESCE(score, (1.0 - COALESCE(posterior, 0.5)) * 100) AS score, snapshot_at
            FROM wallet_history
            WHERE wallet = ? AND snapshot_at <= ?
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
            (wallet.strip(), cutoff),
        )
    else:
        cur.execute(
            """
            SELECT COALESCE(score, 50) AS score, snapshot_at
            FROM wallet_history
            WHERE wallet = ? AND snapshot_at <= ?
            ORDER BY snapshot_at DESC
            LIMIT 1
            """,
            (wallet.strip(), cutoff),
        )
    row = cur.fetchone()
    if not row:
        return None
    old_score = float(row["score"] if hasattr(row, "keys") else row[0] or 50)
    return round(current_score - old_score, 1)


def _build_badge_reason_summary(reasons: list[dict], badge: str) -> str:
    """Build human-readable summary: 'Wallet marked HIGH_RISK due to cluster link and drainer.'"""
    if not reasons:
        return f"Wallet marked {badge}. No specific reasons on file."
    negatives = [r for r in reasons if (r.get("weight") or 0) < 0]
    if not negatives:
        return f"Wallet marked {badge}. No negative indicators."
    phrases = []
    seen = set()
    for r in negatives[:5]:
        code = (r.get("code") or "").strip()
        if not code or code in seen:
            continue
        seen.add(code)
        phrase = _REASON_PHRASES.get(code) or code.replace("_", " ").lower()
        if phrase not in phrases:
            phrases.append(phrase)
    if not phrases:
        return f"Wallet marked {badge}."
    return f"Wallet marked {badge} due to " + " and ".join(phrases) + "."


def _fetch_investigation_badge(wallet: str) -> dict:
    """Fetch all investigation badge data for wallet."""
    wallet = wallet.strip()
    conn = get_connection()

    try:
        cur = conn.cursor()

        # trust_scores
        cur.execute(
            "SELECT score, risk_level FROM trust_scores WHERE wallet = ? LIMIT 1",
            (wallet,),
        )
        row = cur.fetchone()
        if not row:
            raise ValueError(f"No trust score for wallet {wallet[:16]}...")

        score = round(float(row["score"] if hasattr(row, "keys") else row[0] or 50), 2)
        risk = str(row["risk_level"] if hasattr(row, "keys") else row[1] or "1")

        badge = get_badge(score)
        scam_wallets = _load_scam_wallets(conn)
        from backend_blockid.database.repositories import get_wallet_reasons
        reasons = get_wallet_reasons(wallet)
        top_reason_codes = [r.get("code", "") for r in reasons[:5] if r.get("code")]
        badge_reason_summary = _build_badge_reason_summary(reasons, badge)
        cluster_info = _get_cluster_info(conn, wallet)
        recent_suspicious_tx_count = _count_suspicious_tx(conn, wallet, scam_wallets)
        score_change_30d = _get_score_change_30d(conn, wallet, score)

        cluster_id_val = cluster_info.get("cluster_id")
        logger.info(
            "investigation_badge",
            wallet=wallet[:16],
            badge=badge,
            cluster=cluster_id_val or 0,
        )
        print(f"[investigation_badge] wallet={wallet[:16]}... badge={badge} cluster={cluster_id_val or 0}")

        return {
            "wallet": wallet,
            "score": score,
            "badge": badge,
            "risk": risk,
            "badge_reason_summary": badge_reason_summary,
            "top_reason_codes": top_reason_codes,
            "cluster_info": cluster_info,
            "recent_suspicious_tx_count": recent_suspicious_tx_count,
            "score_change_30d": score_change_30d,
        }
    finally:
        conn.close()


@router.get("/{wallet}/investigation_badge")
def get_investigation_badge(wallet: str) -> dict:
    """
    Return detailed badge explanation for investigators.
    For UI card and Phantom plugin warning popup.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    try:
        from solders.pubkey import Pubkey
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    try:
        return _fetch_investigation_badge(wallet)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except Exception as e:
        logger.exception("investigation_badge_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e
