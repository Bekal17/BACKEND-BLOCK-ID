"""
Repository layer — PostgreSQL via asyncpg.
All functions are async.
"""
from typing import List, Dict
import csv
import time

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
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


async def _ensure_wallet_reasons_created_at(conn) -> None:
    """Ensure created_at column exists for time decay."""
    try:
        await conn.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                    WHERE table_name='wallet_reasons' AND column_name='created_at') THEN
                    ALTER TABLE wallet_reasons ADD COLUMN created_at BIGINT;
                END IF;
            END $$;
        """)
    except Exception:
        pass


async def _ensure_wallet_reasons_optional_columns(conn) -> None:
    """Ensure optional columns exist."""
    for col, typ in [("confidence_score", "DOUBLE PRECISION"), ("tx_hash", "TEXT"), ("tx_link", "TEXT")]:
        try:
            await conn.execute(f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                        WHERE table_name='wallet_reasons' AND column_name='{col}') THEN
                        ALTER TABLE wallet_reasons ADD COLUMN {col} {typ};
                    END IF;
                END $$;
            """)
        except Exception:
            pass


async def get_wallet_reasons(wallet: str) -> List[Dict]:
    """
    Return all reasons for a wallet including tx proof.
    Time decay: reasons older than 90 days have weight halved.
    """
    conn = await get_conn()
    try:
        await _ensure_wallet_reasons_created_at(conn)
        await _ensure_wallet_reasons_optional_columns(conn)

        rows = await conn.fetch("""
            SELECT reason_code, weight, confidence_score, tx_hash, tx_link, created_at
            FROM wallet_reasons
            WHERE wallet=$1
            ORDER BY id DESC
        """, wallet)

        now_ts_val = int(time.time())

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
            tx_hash = r.get("tx_hash")
            tx_link = r.get("tx_link") or (solscan_link(tx_hash, network="devnet") if tx_hash else None)
            db_weight = r.get("weight") or 0

            weight = weights.get(r["reason_code"], db_weight)
            if r["reason_code"] in HIGH_RISK_CODES:
                weight = -abs(weight)

            created_at = r.get("created_at")
            if created_at is not None and (now_ts_val - int(created_at)) > _DAYS_90_SEC:
                weight = int(weight / 2)

            days_old_val = days_since(created_at) if created_at is not None else 0

            reasons.append({
                "code": r["reason_code"],
                "weight": weight,
                "confidence": r.get("confidence_score"),
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
    finally:
        await release_conn(conn)


async def _ensure_wallet_reasons_unique_index(conn) -> None:
    """Credit-score safety: prevent repeated (wallet, reason_code) inserts."""
    await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_wallet_reason_unique
        ON wallet_reasons(wallet, reason_code)
    """)


async def insert_wallet_reason(
    wallet: str,
    reason_code: str,
    weight: int,
    confidence: float = 1.0,
    tx_hash: str | None = None,
    tx_link: str | None = None,
) -> None:
    """Insert wallet reason safely (ignore duplicates)."""
    conn = await get_conn()
    try:
        await _ensure_wallet_reasons_unique_index(conn)
        await _ensure_wallet_reasons_created_at(conn)
        await _ensure_wallet_reasons_optional_columns(conn)

        exists = await conn.fetchval(
            "SELECT 1 FROM wallet_reasons WHERE wallet=$1 AND reason_code=$2",
            wallet, reason_code,
        )
        if exists:
            return

        created_at = int(time.time())
        await conn.execute("""
            INSERT INTO wallet_reasons(
                wallet, reason_code, weight, confidence_score, tx_hash, tx_link, created_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, wallet, reason_code, weight, confidence, tx_hash, tx_link, created_at)
    finally:
        await release_conn(conn)


async def add_wallet(wallet: str):
    conn = await get_conn()
    try:
        exists = await conn.fetchval("SELECT 1 FROM tracked_wallets WHERE wallet=$1", wallet)
        if not exists:
            await conn.execute(
                "INSERT INTO tracked_wallets(wallet, is_active) VALUES ($1, true)",
                wallet,
            )
    except Exception:
        pass  # Ignore duplicate/constraint errors
    finally:
        await release_conn(conn)


async def update_wallet_score(
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
    conn = await get_conn()
    try:
        computed_at = now_ts()
        print("DEBUG trust_scores write:", wallet, computed_at)

        row = await conn.fetchrow("SELECT score FROM trust_scores WHERE wallet=$1", wallet)
        old_score = float(row["score"]) if row and row.get("score") is not None else None

        is_test_wallet = 1 if wallet.startswith("TEST_") else 0

        exists = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet=$1", wallet)
        if exists:
            await conn.execute(
                """
                UPDATE trust_scores SET
                    score=$2, risk_level=$3, metadata_json=$4, computed_at=$5, updated_at=CURRENT_TIMESTAMP,
                    wallet_age_days=$6, last_scam_days=$7, decay_adjustment=$8,
                    graph_distance=$9, graph_penalty=$10, time_weighted_penalty=$11, is_test_wallet=$12
                WHERE wallet=$1
                """,
                wallet, score, risk_level, metadata, computed_at,
                wallet_age_days, last_scam_days, decay_adjustment,
                graph_distance, graph_penalty, time_weighted_penalty, is_test_wallet,
            )
        else:
            await conn.execute(
                """
                INSERT INTO trust_scores(
                    wallet, score, risk_level, metadata_json, computed_at, updated_at,
                    wallet_age_days, last_scam_days, decay_adjustment,
                    graph_distance, graph_penalty, time_weighted_penalty, is_test_wallet
                )
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, $6, $7, $8, $9, $10, $11, $12)
                """,
                wallet, score, risk_level, metadata, computed_at,
                wallet_age_days, last_scam_days, decay_adjustment,
                graph_distance, graph_penalty, time_weighted_penalty, is_test_wallet,
            )

        reason_count = await conn.fetchval(
            "SELECT COUNT(*) FROM wallet_reasons WHERE wallet=$1", wallet
        )
        if int(reason_count or 0) == 0:
            raise ValueError(
                f"Integrity error: trust_score written for wallet {wallet} "
                f"but no wallet_reasons found."
            )

        try:
            from backend_blockid.tools.badge_engine import record_badge_if_changed_async
            await record_badge_if_changed_async(wallet, old_score, float(score), int(time.time()))
        except Exception:
            pass
    finally:
        await release_conn(conn)


async def save_wallet_scores_from_csv(csv_path: str):
    """Read wallet_scores.csv and insert into trust_scores table."""
    conn = await get_conn()
    updated = 0
    try:
        with open(csv_path, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                wallet = row.get("wallet")
                score = row.get("final_score") or row.get("score")

                if wallet and score:
                    print("Saving wallet:", wallet, "score:", score)
                    computed_at = now_ts()
                    exists = await conn.fetchval("SELECT 1 FROM trust_scores WHERE wallet=$1", wallet)
                    if exists:
                        await conn.execute(
                            """
                            UPDATE trust_scores SET score=$2, computed_at=$3, updated_at=CURRENT_TIMESTAMP
                            WHERE wallet=$1
                            """,
                            wallet, int(score), computed_at,
                        )
                    else:
                        await conn.execute(
                            """
                            INSERT INTO trust_scores(wallet, score, computed_at, updated_at)
                            VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
                            """,
                            wallet, int(score), computed_at,
                        )
                    reason_count = await conn.fetchval(
                        "SELECT COUNT(*) FROM wallet_reasons WHERE wallet=$1", wallet
                    )
                    if int(reason_count or 0) == 0:
                        raise ValueError(
                            f"Integrity error: trust_score written for wallet {wallet} "
                            f"but no wallet_reasons found."
                        )
                    if exists:
                        updated += 1
    finally:
        await release_conn(conn)
    return updated


async def save_wallet_risk_probability(wallet: str, prior: float, posterior: float, reasons: list) -> None:
    """Save Bayesian risk calculation logs."""
    conn = await get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_risk_probabilities (
                id SERIAL PRIMARY KEY,
                wallet TEXT,
                prior DOUBLE PRECISION,
                posterior DOUBLE PRECISION,
                reason_code TEXT,
                likelihood DOUBLE PRECISION,
                confidence DOUBLE PRECISION,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        for r in reasons:
            await conn.execute("""
                INSERT INTO wallet_risk_probabilities
                (wallet, prior, posterior, reason_code, likelihood, confidence)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, wallet, prior, posterior, r.get("code"), r.get("likelihood"), r.get("confidence"))
    finally:
        await release_conn(conn)


async def save_wallet_meta(meta):
    conn = await get_conn()
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS wallet_meta (
                wallet TEXT PRIMARY KEY,
                first_tx_ts BIGINT,
                last_tx_ts BIGINT,
                wallet_age_days BIGINT,
                last_scam_tx_ts BIGINT,
                last_scan_time BIGINT,
                cluster_id TEXT,
                is_test_wallet INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await conn.execute("""
            INSERT INTO wallet_meta (
                wallet, first_tx_ts, last_tx_ts, wallet_age_days,
                last_scam_tx_ts, last_scan_time, cluster_id, is_test_wallet
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT(wallet) DO UPDATE SET
                first_tx_ts=EXCLUDED.first_tx_ts,
                last_tx_ts=EXCLUDED.last_tx_ts,
                wallet_age_days=EXCLUDED.wallet_age_days,
                last_scam_tx_ts=EXCLUDED.last_scam_tx_ts,
                last_scan_time=EXCLUDED.last_scan_time,
                cluster_id=EXCLUDED.cluster_id,
                is_test_wallet=EXCLUDED.is_test_wallet
        """,
            meta.get("wallet"),
            meta.get("first_tx_ts"),
            meta.get("last_tx_ts"),
            meta.get("wallet_age_days"),
            meta.get("last_scam_tx_ts"),
            meta.get("last_scan_time"),
            meta.get("cluster_id"),
            meta.get("is_test_wallet", 0),
        )
    finally:
        await release_conn(conn)


async def get_wallet_meta(wallet):
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT first_tx_ts, last_tx_ts FROM wallet_meta WHERE wallet=$1", wallet
        )
        if not row:
            return None
        return {"first_tx_ts": row["first_tx_ts"], "last_tx_ts": row["last_tx_ts"]}
    finally:
        await release_conn(conn)


async def get_cluster_wallets(cluster_id):
    conn = await get_conn()
    try:
        rows = await conn.fetch(
            "SELECT wallet FROM wallet_clusters WHERE cluster_id=$1", cluster_id
        )
        return [r["wallet"] for r in rows]
    finally:
        await release_conn(conn)


async def get_wallet_cluster_id(wallet: str) -> str | None:
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT cluster_id FROM wallet_clusters WHERE wallet=$1 LIMIT 1", wallet
        )
        return row["cluster_id"] if row else None
    finally:
        await release_conn(conn)


async def get_all_active_clusters() -> list[str]:
    conn = await get_conn()
    try:
        rows = await conn.fetch("SELECT DISTINCT cluster_id FROM wallet_clusters")
        return [str(r["cluster_id"]) for r in rows if r.get("cluster_id")]
    finally:
        await release_conn(conn)


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def get_wallet_cluster_data(pubkey: str) -> dict:
    """
    Returns cluster graph data for a wallet.
    """
    conn = await get_conn()
    try:
        result: dict = {
            "wallet": pubkey.strip(),
            "cluster_id": None,
            "nodes": [],
            "edges": [],
        }

        cluster_id_val = None
        for table in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, table):
                continue
            col = "cluster_id"
            row = await conn.fetchrow(
                f"SELECT cluster_id FROM {table} WHERE wallet=$1 LIMIT 1", pubkey
            )
            if row and row.get("cluster_id") is not None:
                cluster_id_val = int(row["cluster_id"])
                break

        if cluster_id_val is None:
            logger.info("wallet_cluster_data_no_cluster", wallet=pubkey[:16] + "...")
            return result

        result["cluster_id"] = cluster_id_val

        cluster_wallets: list[str] = []
        for table in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, table):
                continue
            rows = await conn.fetch(
                f"SELECT wallet FROM {table} WHERE cluster_id=$1", cluster_id_val
            )
            cluster_wallets = [str(r["wallet"]).strip() for r in rows if r and r.get("wallet")]
            if cluster_wallets:
                break

        if not cluster_wallets:
            return result

        score_map: dict[str, int] = {w: 0 for w in cluster_wallets}
        placeholders = ",".join(f"${i+1}" for i in range(len(cluster_wallets)))
        rows = await conn.fetch(
            f"SELECT wallet, score FROM trust_scores WHERE wallet IN ({placeholders})",
            *cluster_wallets,
        )
        for row in rows:
            w = row.get("wallet")
            s = row.get("score")
            if w:
                score_map[str(w).strip()] = int(float(s) if s is not None else 0)

        unique_wallets = {}
        for wallet in cluster_wallets:
            score = score_map.get(wallet, 0)
            unique_wallets[wallet] = score
        result["nodes"] = [{"wallet": w, "score": s} for w, s in unique_wallets.items()]

        wallet_set = set(cluster_wallets)
        if not await _table_exists(conn, "transactions"):
            logger.info(
                "wallet_cluster_data_loaded",
                wallet=pubkey[:16] + "...",
                cluster_id=cluster_id_val,
                nodes=len(result["nodes"]),
                edges=0,
            )
            return result

        cols_row = await conn.fetch(
            "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
        )
        cols = {r["column_name"] for r in cols_row}

        n = len(cluster_wallets)
        ph1 = ",".join(f"${i+1}" for i in range(n))
        ph2 = ",".join(f"${i+n+1}" for i in range(n))
        params = list(cluster_wallets) + list(cluster_wallets)
        if "from_wallet" in cols and "to_wallet" in cols:
            rows = await conn.fetch(
                f"""
                SELECT from_wallet, to_wallet, COUNT(*)::int AS tx_count
                FROM transactions
                WHERE from_wallet IN ({ph1}) AND to_wallet IN ({ph2})
                GROUP BY from_wallet, to_wallet
                """,
                *params,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT sender AS from_wallet, receiver AS to_wallet, COUNT(*)::int AS tx_count
                FROM transactions
                WHERE sender IN ({ph1}) AND receiver IN ({ph2})
                GROUP BY sender, receiver
                """,
                *params,
            )

        for row in rows:
            frm = str(row.get("from_wallet") or "").strip()
            to = str(row.get("to_wallet") or "").strip()
            cnt = row.get("tx_count") or 0
            if frm and to and frm in wallet_set and to in wallet_set:
                result["edges"].append({"from": frm, "to": to, "tx_count": int(cnt or 0)})

        logger.info(
            "wallet_cluster_data_loaded",
            wallet=pubkey[:16] + "...",
            cluster_id=cluster_id_val,
            nodes=len(result["nodes"]),
            edges=len(result["edges"]),
        )
        return result
    finally:
        await release_conn(conn)


async def get_trust_score_latest(wallet: str) -> dict | None:
    """Get latest trust score for wallet. Used by API GET /wallet/{address}."""
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT score, computed_at, metadata_json, risk_level
            FROM trust_scores
            WHERE wallet=$1
            ORDER BY computed_at DESC
            LIMIT 1
            """,
            wallet,
        )
        return dict(row) if row else None
    finally:
        await release_conn(conn)


async def get_latest_trust_scores_batch(wallets: list[str]) -> dict[str, dict]:
    """Get latest trust score per wallet for batch lookup. Returns {wallet: {score, computed_at, metadata_json, risk_level}}."""
    if not wallets:
        return {}
    conn = await get_conn()
    try:
        ph = ",".join(f"${i+1}" for i in range(len(wallets)))
        rows = await conn.fetch(
            f"""
            SELECT DISTINCT ON (wallet) wallet, score, computed_at, metadata_json, risk_level
            FROM trust_scores
            WHERE wallet IN ({ph})
            ORDER BY wallet, computed_at DESC
            """,
            *wallets,
        )
        return {r["wallet"]: dict(r) for r in rows if r.get("wallet")}
    finally:
        await release_conn(conn)
