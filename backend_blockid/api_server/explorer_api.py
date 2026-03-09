"""
Explorer API — read-only identity profile and graph stats.

GET /explorer/identity/{wallet}
Returns aggregated identity, cluster, graph, and risk data.
No INSERT, UPDATE, DELETE. SELECT only.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from solders.pubkey import Pubkey

from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.explorer.recommendation_engine import generate_recommended_actions

router = APIRouter(prefix="/explorer", tags=["Explorer"])


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _get_table_columns(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name=$1",
        table,
    )
    return {r["column_name"] for r in rows}


async def _fetch_identity_profile(wallet: str) -> dict:
    """Return identity profile for a wallet. Read-only."""

    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT score FROM trust_scores WHERE wallet = $1", wallet)
        score = float(row["score"]) if row and row["score"] is not None else 0.0

        if score >= 80:
            risk_level = "LOW"
        elif score >= 50:
            risk_level = "MEDIUM"
        else:
            risk_level = "HIGH"

        cluster_id = None
        cluster_size = 0
        for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, tbl):
                continue
            row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
            if row and row["cluster_id"] is not None:
                cluster_id = int(row["cluster_id"])
                count_row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE cluster_id = $1", cluster_id)
                cluster_size = int(count_row["cnt"] or 0)
                break

        has_transactions = await _table_exists(conn, "transactions")
        cols = await _get_table_columns(conn, "transactions") if has_transactions else set()

        if "sender" in cols and "receiver" in cols:
            sender_col, receiver_col = "sender", "receiver"
            amount_col = "amount_lamports" if "amount_lamports" in cols else "amount"
        else:
            sender_col, receiver_col = "from_wallet", "to_wallet"
            amount_col = "amount" if "amount" in cols else "amount_lamports"

        total_transactions = 0
        total_volume_lamports = 0
        graph_degree = 0

        if has_transactions:
            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM transactions WHERE {sender_col} = $1 OR {receiver_col} = $1",
                wallet,
            )
            total_transactions = int(row["cnt"] or 0)

            row = await conn.fetchrow(
                f"SELECT COALESCE(SUM({amount_col}), 0) as total FROM transactions WHERE {sender_col} = $1 OR {receiver_col} = $1",
                wallet,
            )
            total_volume_lamports = int(float(row["total"] or 0))

            row = await conn.fetchrow(
                f"""
                SELECT COUNT(DISTINCT counterparty) as cnt FROM (
                    SELECT {receiver_col} AS counterparty FROM transactions WHERE {sender_col} = $1
                    UNION
                    SELECT {sender_col} AS counterparty FROM transactions WHERE {receiver_col} = $1
                ) sub
                """,
                wallet,
            )
            graph_degree = int(row["cnt"] or 0)

        reason_count = 0
        latest_reason = None
        if await _table_exists(conn, "wallet_reasons"):
            row = await conn.fetchrow("SELECT COUNT(*) as cnt FROM wallet_reasons WHERE wallet = $1", wallet)
            reason_count = int(row["cnt"] or 0)
            row = await conn.fetchrow(
                "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 ORDER BY created_at DESC LIMIT 1",
                wallet,
            )
            latest_reason = str(row["reason_code"]).strip() if row and row["reason_code"] else None
    finally:
        await release_conn(conn)

    return {
        "identity": {
            "primary_wallet": wallet,
            "trust_score": score,
            "risk_level": risk_level,
        },
        "cluster": {
            "cluster_id": cluster_id,
            "size": cluster_size,
        },
        "graph": {
            "degree": graph_degree,
            "total_transactions": total_transactions,
            "total_volume_lamports": total_volume_lamports,
        },
        "risk": {
            "reason_count": reason_count,
            "latest_reason": latest_reason,
        },
    }


async def _fetch_identity_timeline(wallet: str) -> dict:
    """Return activity and risk timeline for a wallet. Read-only."""

    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    conn = await get_conn()
    try:
        first_seen = None
        last_seen = None
        total_transactions = 0

        if await _table_exists(conn, "transactions"):
            cols = await _get_table_columns(conn, "transactions")
            if "sender" in cols and "receiver" in cols:
                sender_col, receiver_col = "sender", "receiver"
            else:
                sender_col, receiver_col = "from_wallet", "to_wallet"

            row = await conn.fetchrow(
                f"SELECT MIN(timestamp) as min_ts FROM transactions WHERE {sender_col} = $1 OR {receiver_col} = $1",
                wallet,
            )
            first_seen = row["min_ts"] if row and row["min_ts"] is not None else None

            row = await conn.fetchrow(
                f"SELECT MAX(timestamp) as max_ts FROM transactions WHERE {sender_col} = $1 OR {receiver_col} = $1",
                wallet,
            )
            last_seen = row["max_ts"] if row and row["max_ts"] is not None else None

            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM transactions WHERE {sender_col} = $1 OR {receiver_col} = $1",
                wallet,
            )
            total_transactions = int(row["cnt"] or 0)

        risk_timeline_list: list[dict] = []
        if await _table_exists(conn, "wallet_reasons"):
            wr_cols = await _get_table_columns(conn, "wallet_reasons")
            conf_col = "confidence_score" if "confidence_score" in wr_cols else "confidence"

            rows = await conn.fetch(
                f"SELECT reason_code, weight, {conf_col}, created_at FROM wallet_reasons WHERE wallet = $1 ORDER BY created_at ASC",
                wallet,
            )
            for row in rows:
                code = str(row["reason_code"] or "").strip()
                weight = float(row["weight"]) if row["weight"] is not None else 0
                confidence = float(row[conf_col]) if row[conf_col] is not None else None
                created_at = row["created_at"]
                risk_timeline_list.append({
                    "timestamp": created_at,
                    "code": code,
                    "weight": weight,
                    "confidence": confidence,
                })
    finally:
        await release_conn(conn)

    return {
        "wallet": wallet,
        "activity": {
            "first_seen": first_seen,
            "last_seen": last_seen,
            "total_transactions": total_transactions,
        },
        "risk_timeline": risk_timeline_list,
    }


async def _fetch_identity_graph_summary(wallet: str) -> dict:
    """Return graph summary for a wallet. Read-only."""

    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    conn = await get_conn()
    try:
        inbound_count = 0
        outbound_count = 0
        inbound_volume = 0
        outbound_volume = 0
        unique_counterparties = 0
        high_risk_neighbors = 0
        cluster_id = None
        cluster_size = 0
        cluster_risk_density = 0.0

        has_transactions = await _table_exists(conn, "transactions")

        if has_transactions:
            cols = await _get_table_columns(conn, "transactions")
            if "sender" in cols and "receiver" in cols:
                sender_col, receiver_col = "sender", "receiver"
                amount_col = "amount_lamports" if "amount_lamports" in cols else "amount"
            else:
                sender_col, receiver_col = "from_wallet", "to_wallet"
                amount_col = "amount" if "amount" in cols else "amount_lamports"

            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt, COALESCE(SUM({amount_col}), 0) as vol FROM transactions WHERE {receiver_col} = $1",
                wallet,
            )
            inbound_count = int(row["cnt"] or 0)
            inbound_volume = int(float(row["vol"] or 0))

            row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt, COALESCE(SUM({amount_col}), 0) as vol FROM transactions WHERE {sender_col} = $1",
                wallet,
            )
            outbound_count = int(row["cnt"] or 0)
            outbound_volume = int(float(row["vol"] or 0))

            row = await conn.fetchrow(
                f"""
                SELECT COUNT(DISTINCT counterparty) as cnt FROM (
                    SELECT {receiver_col} AS counterparty FROM transactions WHERE {sender_col} = $1
                    UNION
                    SELECT {sender_col} AS counterparty FROM transactions WHERE {receiver_col} = $1
                ) sub
                """,
                wallet,
            )
            unique_counterparties = int(row["cnt"] or 0)

            row = await conn.fetchrow(
                f"""
                SELECT COUNT(*) as cnt FROM trust_scores
                WHERE wallet IN (
                    SELECT {receiver_col} FROM transactions WHERE {sender_col} = $1
                    UNION
                    SELECT {sender_col} FROM transactions WHERE {receiver_col} = $1
                )
                AND score < 50
                """,
                wallet,
            )
            high_risk_neighbors = int(row["cnt"] or 0)

        cluster_tbl = None
        for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, tbl):
                continue
            row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
            if row and row["cluster_id"] is not None:
                cluster_id = int(row["cluster_id"])
                cluster_tbl = tbl
                count_row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE cluster_id = $1", cluster_id)
                cluster_size = int(count_row["cnt"] or 0)
                break

        if cluster_id is not None and cluster_tbl and cluster_size > 0:
            row = await conn.fetchrow(
                f"""
                SELECT COUNT(*) as cnt FROM trust_scores
                WHERE wallet IN (SELECT wallet FROM {cluster_tbl} WHERE cluster_id = $1)
                AND score < 50
                """,
                cluster_id,
            )
            high_risk_cluster_members = int(row["cnt"] or 0)
            cluster_risk_density = high_risk_cluster_members / cluster_size
    finally:
        await release_conn(conn)

    return {
        "wallet": wallet,
        "transactions": {
            "inbound": inbound_count,
            "outbound": outbound_count,
            "total": inbound_count + outbound_count,
        },
        "volume": {
            "inbound_lamports": inbound_volume,
            "outbound_lamports": outbound_volume,
        },
        "network": {
            "unique_counterparties": unique_counterparties,
            "high_risk_neighbors": high_risk_neighbors,
        },
        "cluster": {
            "cluster_id": cluster_id,
            "cluster_size": cluster_size,
            "cluster_risk_density": round(cluster_risk_density, 4),
        },
    }


async def _fetch_identity_counterparties(wallet: str) -> dict:
    """Return counterparty list for a wallet. Read-only."""

    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    conn = await get_conn()
    try:
        counterparties: list[dict] = []
        total_counterparties = 0
        high_risk_counterparties = 0

        if not await _table_exists(conn, "transactions"):
            return {
                "wallet": wallet,
                "counterparties": [],
                "summary": {
                    "total_counterparties": 0,
                    "high_risk_counterparties": 0,
                    "exposure_ratio": 0.0,
                },
            }

        cols = await _get_table_columns(conn, "transactions")
        if "sender" in cols and "receiver" in cols:
            sender_col, receiver_col = "sender", "receiver"
            amount_col = "amount_lamports" if "amount_lamports" in cols else "amount"
        else:
            sender_col, receiver_col = "from_wallet", "to_wallet"
            amount_col = "amount" if "amount" in cols else "amount_lamports"

        rows = await conn.fetch(
            f"""
            SELECT
                counterparty,
                SUM(inbound) AS inbound_tx,
                SUM(outbound) AS outbound_tx,
                SUM(amount_val) AS total_volume
            FROM (
                SELECT
                    {receiver_col} AS counterparty,
                    0 AS inbound,
                    1 AS outbound,
                    COALESCE({amount_col}, 0) AS amount_val
                FROM transactions
                WHERE {sender_col} = $1

                UNION ALL

                SELECT
                    {sender_col} AS counterparty,
                    1 AS inbound,
                    0 AS outbound,
                    COALESCE({amount_col}, 0) AS amount_val
                FROM transactions
                WHERE {receiver_col} = $1
            ) sub
            GROUP BY counterparty
            """,
            wallet,
        )

        if not rows:
            return {
                "wallet": wallet,
                "counterparties": [],
                "summary": {
                    "total_counterparties": 0,
                    "high_risk_counterparties": 0,
                    "exposure_ratio": 0.0,
                },
            }

        cp_wallets = [str(r["counterparty"]).strip() for r in rows if r and r["counterparty"]]
        score_map: dict[str, float] = {w: 0.0 for w in cp_wallets}

        if cp_wallets:
            ph = ",".join(f"${i+1}" for i in range(len(cp_wallets)))
            score_rows = await conn.fetch(
                f"SELECT wallet, score FROM trust_scores WHERE wallet IN ({ph})",
                *cp_wallets,
            )
            for row in score_rows:
                w = str(row["wallet"] or "").strip()
                if w:
                    score_map[w] = float(row["score"]) if row["score"] is not None else 0.0

        for row in rows:
            cp = str(row["counterparty"] or "").strip()
            if not cp:
                continue
            inbound_tx = int(row["inbound_tx"] or 0)
            outbound_tx = int(row["outbound_tx"] or 0)
            total_volume = int(float(row["total_volume"] or 0))
            score = score_map.get(cp, 0.0)

            if score >= 80:
                risk_level = "LOW"
            elif score >= 50:
                risk_level = "MEDIUM"
            else:
                risk_level = "HIGH"

            if score < 50:
                high_risk_counterparties += 1

            counterparties.append({
                "wallet": cp,
                "inbound_tx": inbound_tx,
                "outbound_tx": outbound_tx,
                "total_tx": inbound_tx + outbound_tx,
                "total_volume_lamports": total_volume,
                "trust_score": score,
                "risk_level": risk_level,
            })

        total_counterparties = len(counterparties)
        if total_counterparties > 0:
            exposure_ratio = round(
                high_risk_counterparties / total_counterparties,
                4
            )
        else:
            exposure_ratio = 0.0
    finally:
        await release_conn(conn)

    return {
        "wallet": wallet,
        "counterparties": counterparties,
        "summary": {
            "total_counterparties": total_counterparties,
            "high_risk_counterparties": high_risk_counterparties,
            "exposure_ratio": exposure_ratio
        },
    }


async def _fetch_identity_propagation_preview(wallet: str) -> dict:
    """Return propagation preview for a wallet. Read-only."""

    data = await _fetch_identity_counterparties(wallet)
    counterparties = data["counterparties"]

    total_neighbors = len(counterparties)
    total_transactions = sum(cp["total_tx"] for cp in counterparties)
    raw_impact = sum((100 - cp["trust_score"]) * cp["total_tx"] for cp in counterparties)

    if total_transactions > 0:
        normalized_impact = round(raw_impact / total_transactions, 2)
    else:
        normalized_impact = 0.0

    if normalized_impact >= 70:
        risk_signal = "HIGH"
    elif normalized_impact >= 40:
        risk_signal = "MEDIUM"
    else:
        risk_signal = "LOW"

    return {
        "wallet": data["wallet"],
        "propagation_preview": {
            "total_neighbors": total_neighbors,
            "total_transactions": total_transactions,
            "raw_impact": raw_impact,
            "normalized_impact": normalized_impact,
            "risk_signal": risk_signal,
        },
    }


async def _fetch_identity_category(wallet: str) -> dict:
    """Return identity category for a wallet. Read-only."""

    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT score FROM trust_scores WHERE wallet = $1", wallet)
        trust_score = float(row["score"]) if row and row["score"] is not None else 50.0

        cluster_size = 0
        for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, tbl):
                continue
            row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
            if row and row["cluster_id"] is not None:
                cluster_id = int(row["cluster_id"])
                count_row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE cluster_id = $1", cluster_id)
                cluster_size = int(count_row["cnt"] or 0)
                break
    finally:
        await release_conn(conn)

    cp_data = await _fetch_identity_counterparties(wallet)
    prop_data = await _fetch_identity_propagation_preview(wallet)
    exposure_ratio = cp_data["summary"]["exposure_ratio"]
    propagation_signal = prop_data["propagation_preview"]["risk_signal"]

    if trust_score <= 30 and exposure_ratio >= 0.5:
        category = "TOXIC_NODE"
    elif trust_score <= 30 and exposure_ratio == 0:
        category = "SELF_RISK"
    elif trust_score > 30 and exposure_ratio >= 0.5:
        category = "NETWORK_EXPOSED"
    elif exposure_ratio > 0 and trust_score >= 50:
        category = "MONITORED"
    elif trust_score >= 60 and exposure_ratio == 0:
        category = "CLEAN"
    else:
        category = "NEUTRAL"

    if category in ["TOXIC_NODE", "SELF_RISK"]:
        confidence = "HIGH"
    elif category in ["NETWORK_EXPOSED", "MONITORED"]:
        confidence = "MEDIUM"
    elif category == "CLEAN":
        confidence = "LOW"
    else:
        confidence = "LOW"

    return {
        "wallet": wallet,
        "category": category,
        "confidence": confidence,
        "analysis": {
            "trust_score": trust_score,
            "cluster_size": cluster_size,
            "exposure_ratio": exposure_ratio,
            "propagation_signal": propagation_signal,
        },
    }


async def _fetch_identity_flags(wallet: str) -> dict:
    """Return identity flags for a wallet. Read-only."""

    profile = await _fetch_identity_profile(wallet)
    cp_data = await _fetch_identity_counterparties(wallet)
    prop_data = await _fetch_identity_propagation_preview(wallet)

    trust_score = profile["identity"]["trust_score"]
    cluster_id = profile["cluster"]["cluster_id"]
    cluster_size = profile["cluster"]["size"]
    total_transactions = profile["graph"]["total_transactions"]
    exposure_ratio = cp_data["summary"]["exposure_ratio"]
    normalized_impact = prop_data["propagation_preview"]["normalized_impact"]
    risk_signal = prop_data["propagation_preview"]["risk_signal"]

    flags: list[str] = []

    if total_transactions == 0:
        flags.append("ISOLATED")

    if cluster_id is not None and cluster_size > 1:
        flags.append("CLUSTERED")

    if exposure_ratio > 0:
        flags.append("EXPOSED")

    if exposure_ratio >= 0.5 or normalized_impact >= 70:
        flags.append("CONTAMINATED")

    if trust_score < 50:
        flags.append("HIGH_RISK_SELF")

    return {
        "wallet": profile["identity"]["primary_wallet"],
        "flags": flags,
        "analysis": {
            "trust_score": trust_score,
            "cluster_size": cluster_size,
            "exposure_ratio": exposure_ratio,
            "propagation_signal": risk_signal,
        },
    }


async def _fetch_identity_summary_compact(wallet: str) -> dict:
    """Return compact identity summary for a wallet. Read-only."""

    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")
    try:
        Pubkey.from_string(wallet)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid Solana wallet address")

    conn = await get_conn()
    try:
        row = await conn.fetchrow("SELECT score, updated_at FROM trust_scores WHERE wallet = $1", wallet)
        if row and row["score"] is not None:
            trust_score = float(row["score"])
            last_updated = row["updated_at"]
        else:
            trust_score = 50.0
            last_updated = None

        if trust_score <= 30:
            intrinsic_risk_level = "HIGH"
        elif trust_score < 60:
            intrinsic_risk_level = "MEDIUM"
        else:
            intrinsic_risk_level = "LOW"

        cluster_id = None
        cluster_size = 0
        for tbl in ("wallet_clusters", "wallet_graph_clusters", "wallet_cluster_members"):
            if not await _table_exists(conn, tbl):
                continue
            row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
            if row and row["cluster_id"] is not None:
                cluster_id = int(row["cluster_id"])
                count_row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE cluster_id = $1", cluster_id)
                cluster_size = int(count_row["cnt"] or 0)
                break

        primary_risk_driver = None
        if await _table_exists(conn, "wallet_reasons"):
            try:
                row = await conn.fetchrow(
                    """
                    SELECT reason_code, weight
                    FROM wallet_reasons
                    WHERE wallet = $1
                      AND weight < 0
                    ORDER BY weight ASC
                    LIMIT 1
                    """,
                    wallet,
                )
                if row:
                    primary_risk_driver = {
                        "code": str(row["reason_code"] or "").strip(),
                        "weight": int(row["weight"]),
                    }
            except Exception:
                primary_risk_driver = None
    finally:
        await release_conn(conn)

    cat_data = await _fetch_identity_category(wallet)
    flg_data = await _fetch_identity_flags(wallet)
    exposure_ratio = cat_data["analysis"]["exposure_ratio"]

    if exposure_ratio >= 0.5:
        contextual_risk_level = "HIGH"
    elif exposure_ratio > 0:
        contextual_risk_level = "MEDIUM"
    else:
        contextual_risk_level = "LOW"

    if intrinsic_risk_level == "HIGH":
        simple_status = "RISKY"
        summary_message = "This wallet shows direct risk indicators."
    elif contextual_risk_level == "HIGH":
        simple_status = "REVIEW"
        summary_message = "This wallet has exposure to higher-risk entities."
    else:
        simple_status = "SAFE"
        summary_message = "No direct or network risk detected."

    if simple_status == "RISKY":
        risk_tier = "HIGH"
        risk_color = "RED"
    elif simple_status == "REVIEW":
        risk_tier = "MEDIUM"
        risk_color = "AMBER"
    else:
        risk_tier = "LOW"
        risk_color = "GREEN"

    category = cat_data["category"]
    recommended_actions = generate_recommended_actions(
        category=category,
        intrinsic_risk_level=intrinsic_risk_level,
        contextual_risk_level=contextual_risk_level,
    )

    return {
        "wallet": wallet,
        "trust_score": trust_score,
        "intrinsic_risk_level": intrinsic_risk_level,
        "contextual_risk_level": contextual_risk_level,
        "simple_status": simple_status,
        "risk_tier": risk_tier,
        "risk_color": risk_color,
        "summary_message": summary_message,
        "primary_risk_driver": primary_risk_driver,
        "category": category,
        "confidence": cat_data["confidence"],
        "cluster": {
            "cluster_id": cluster_id,
            "size": cluster_size,
        },
        "exposure_ratio": exposure_ratio,
        "propagation_signal": cat_data["analysis"]["propagation_signal"],
        "badges": flg_data["flags"],
        "recommended_actions": recommended_actions,
        "last_updated": last_updated,
    }


@router.get("/identity/{wallet}/summary-compact")
async def get_identity_summary_compact(wallet: str) -> dict:
    """
    Return compact identity summary for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_summary_compact(wallet)


@router.get("/identity/{wallet}/category")
async def get_identity_category(wallet: str) -> dict:
    """
    Return identity category for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_category(wallet)


@router.get("/identity/{wallet}/flags")
async def get_identity_flags(wallet: str) -> dict:
    """
    Return identity flags for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_flags(wallet)


@router.get("/identity/{wallet}/propagation-preview")
async def get_identity_propagation_preview(wallet: str) -> dict:
    """
    Return propagation preview for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_propagation_preview(wallet)


@router.get("/identity/{wallet}/counterparties")
async def get_identity_counterparties(wallet: str) -> dict:
    """
    Return counterparty list for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_counterparties(wallet)


@router.get("/identity/{wallet}/graph-summary")
async def get_identity_graph_summary(wallet: str) -> dict:
    """
    Return graph summary for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_graph_summary(wallet)


@router.get("/identity/{wallet}/timeline")
async def get_identity_timeline(wallet: str) -> dict:
    """
    Return activity and risk timeline for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_timeline(wallet)


@router.get("/identity/{wallet}")
async def get_identity_profile(wallet: str) -> dict:
    """
    Return identity profile for a wallet.
    Read-only. No state changes.
    """
    return await _fetch_identity_profile(wallet)
