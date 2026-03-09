"""
Explorer Identity API — timeline, graph, neighbors, and flow-path endpoints for Trust Analytics / Investigation UI.

GET /explorer/identity/{wallet}/timeline — activity, risk, milestones, propagation events
GET /explorer/identity/{wallet}/graph — network graph (nodes, edges) for Investigation Mode
GET /explorer/identity/{wallet}/neighbors — top interacting wallets for cluster/trust analytics
GET /explorer/identity/{wallet}/flow-path — flow relationships for Fund Flow Visualization
Read-only. Uses wallet_history, wallet_reasons, transactions,
wallet_clusters / wallet_cluster_members, trust_scores.
"""

from __future__ import annotations

from cachetools import TTLCache
from fastapi import APIRouter, HTTPException

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter()

timeline_cache: TTLCache = TTLCache(maxsize=1000, ttl=600)
flow_cache: TTLCache = TTLCache(maxsize=1000, ttl=600)
graph_cache: TTLCache = TTLCache(maxsize=500, ttl=900)

REASON_TO_EVENT = {
    "SCAM_CLUSTER_MEMBER": "Wallet linked to scam cluster",
    "SCAM_CLUSTER_MEMBER_SMALL": "Wallet linked to scam cluster",
    "SCAM_CLUSTER_MEMBER_LARGE": "Wallet linked to scam cluster",
    "HIGH_PROPAGATION_RISK": "High propagation risk detected",
    "RISKY_INTERACTION": "Interacted with risky wallet",
    "DRAINER_INTERACTION": "Interacted with risky wallet",
    "DRAINER_FLOW": "Drainer flow detected",
    "DRAINER_FLOW_DETECTED": "Drainer flow detected",
    "RUG_PULL_DEPLOYER": "Rug pull deployer detected",
    "BLACKLISTED_CREATOR": "Blacklisted creator detected",
    "NEW_WALLET": "New wallet detected",
    "LOW_ACTIVITY": "Low activity wallet",
    "CLEAN_HISTORY": "Clean history confirmed",
    "LONG_TERM_ACTIVE": "Long-term active wallet",
}

RISK_MAP = {
    "SCAM_CLUSTER_MEMBER": "CRITICAL",
    "SCAM_CLUSTER_MEMBER_SMALL": "HIGH",
    "SCAM_CLUSTER_MEMBER_LARGE": "CRITICAL",
    "MEGA_DRAINER": "CRITICAL",
    "RUG_PULL_DEPLOYER": "CRITICAL",
    "DRAINER_FLOW": "CRITICAL",
    "DRAINER_FLOW_DETECTED": "CRITICAL",
    "BLACKLISTED_CREATOR": "CRITICAL",
    "HIGH_PROPAGATION_RISK": "HIGH",
    "HIGH_RISK_TOKEN_INTERACTION": "HIGH",
    "SUSPICIOUS_TOKEN_MINT": "HIGH",
    "DRAINER_INTERACTION": "HIGH",
    "RISKY_INTERACTION": "MEDIUM",
    "HIGH_VALUE_OUTFLOW": "MEDIUM",
    "UNKNOWN_TOKEN": "LOW",
    "NEW_WALLET": "LOW",
    "LOW_ACTIVITY": "LOW",
    "CLEAN_HISTORY": "LOW",
    "LONG_TERM_ACTIVE": "LOW",
}

SEVERITY_MAP = {
    "LOW": 1,
    "MEDIUM": 2,
    "HIGH": 3,
    "CRITICAL": 4,
}


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


async def _wallet_exists(conn, wallet: str) -> bool:
    """Check if wallet exists in trust_scores (legacy) or wallets table."""
    if await _table_exists(conn, "trust_scores"):
        row = await conn.fetchrow("SELECT 1 FROM trust_scores WHERE wallet = $1", wallet)
        if row:
            return True
    if await _table_exists(conn, "wallets"):
        row = await conn.fetchrow("SELECT 1 FROM wallets WHERE address = $1", wallet)
        if row:
            return True
    return False


async def _add_propagation_events(
    conn,
    wallet: str,
    timeline_events: list[dict],
    first_timestamp: int | None,
) -> None:
    """Add propagation events from wallet_clusters / wallet_cluster_members."""
    cluster_id = None
    cluster_size = 0
    prop_ts = None
    cluster_table = None

    for tbl in ("wallet_cluster_members", "wallet_graph_clusters", "wallet_clusters"):
        if not await _table_exists(conn, tbl):
            continue
        cols = await _get_table_columns(conn, tbl)
        if "cluster_id" not in cols or "wallet" not in cols:
            continue

        row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
        if not row:
            continue

        cluster_id = int(row["cluster_id"])
        count_row = await conn.fetchrow(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE cluster_id = $1", cluster_id)
        cluster_size = int(count_row["cnt"] or 0)
        cluster_table = tbl

        ts_col = "added_at" if "added_at" in cols else "created_at" if "created_at" in cols else "updated_at" if "updated_at" in cols else None
        if ts_col:
            r = await conn.fetchrow(f"SELECT {ts_col} FROM {tbl} WHERE wallet = $1 AND cluster_id = $2 LIMIT 1", wallet, cluster_id)
            prop_ts = int(r[ts_col]) if r and r[ts_col] is not None else None
        break

    if cluster_id is None or cluster_size == 0:
        return

    if prop_ts is None:
        r = await conn.fetchrow("SELECT computed_at FROM trust_scores WHERE wallet = $1 ORDER BY computed_at DESC LIMIT 1", wallet)
        prop_ts = int(r["computed_at"]) if r and r["computed_at"] is not None else first_timestamp
    if prop_ts is None:
        prop_ts = 0

    if cluster_size > 3:
        timeline_events.append({
            "date": prop_ts,
            "event": "Wallet linked to risk cluster",
            "type": "propagation",
            "risk_level": "HIGH",
            "severity": 3,
        })

    if cluster_size > 10:
        timeline_events.append({
            "date": prop_ts,
            "event": "Risk propagated across network cluster",
            "type": "propagation",
            "risk_level": "CRITICAL",
            "severity": 4,
        })


@router.get("/explorer/identity/{wallet}/timeline")
async def get_explorer_identity_timeline(wallet: str) -> dict:
    """
    Return timeline events for wallet activity and risk history.
    Read-only. Used by Trust Analytics UI.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    cache_key = f"timeline:{wallet}"
    if cache_key in timeline_cache:
        return timeline_cache[cache_key]

    conn = await get_conn()
    try:
        if not await _wallet_exists(conn, wallet):
            raise HTTPException(status_code=404, detail="Wallet not found")

        timeline_events: list[dict] = []
        first_timestamp = None
        has_wallet_history = False

        if await _table_exists(conn, "wallet_history"):
            wh_cols = await _get_table_columns(conn, "wallet_history")
            ts_col = "snapshot_at" if "snapshot_at" in wh_cols else "timestamp"
            rows = await conn.fetch(
                f"SELECT {ts_col} FROM wallet_history WHERE wallet = $1 ORDER BY {ts_col} ASC LIMIT 50",
                wallet,
            )
            for row in rows:
                ts = row[ts_col]
                if ts is not None:
                    ts_val = int(ts)
                    has_wallet_history = True
                    if first_timestamp is None:
                        first_timestamp = ts_val
                    timeline_events.append({
                        "date": ts_val,
                        "event": "Activity snapshot recorded",
                        "type": "activity",
                        "risk_level": "LOW",
                        "severity": 1,
                    })

        if await _table_exists(conn, "wallet_reasons"):
            wr_cols = await _get_table_columns(conn, "wallet_reasons")
            ts_col = "created_at" if "created_at" in wr_cols else "timestamp"
            rows = await conn.fetch(
                f"SELECT {ts_col}, reason_code FROM wallet_reasons WHERE wallet = $1 ORDER BY {ts_col} ASC LIMIT 50",
                wallet,
            )
            for row in rows:
                ts = row[ts_col]
                code = (row["reason_code"] or "").strip()
                if ts is not None and code:
                    ts_val = int(ts)
                    if first_timestamp is None:
                        first_timestamp = ts_val
                    event_desc = REASON_TO_EVENT.get(code, code.replace("_", " ").title())
                    risk_level = RISK_MAP.get(code, "LOW")
                    severity = SEVERITY_MAP.get(risk_level, 1)
                    ev_type = "risk" if any(x in code for x in ("SCAM", "DRAINER", "RISKY", "BLACKLIST", "RUG")) else "info"
                    timeline_events.append({
                        "date": ts_val,
                        "event": event_desc,
                        "type": ev_type,
                        "risk_level": risk_level,
                        "severity": severity,
                    })

        if await _table_exists(conn, "transactions"):
            tx_cols = await _get_table_columns(conn, "transactions")
            if "from_wallet" in tx_cols and "to_wallet" in tx_cols:
                from_col, to_col = "from_wallet", "to_wallet"
            else:
                from_col, to_col = "sender", "receiver"

            first_tx_row = await conn.fetchrow(
                f"SELECT timestamp FROM transactions WHERE {from_col} = $1 OR {to_col} = $1 ORDER BY COALESCE(timestamp, 0) ASC LIMIT 1",
                wallet,
            )
            first_tx_ts = int(first_tx_row["timestamp"]) if first_tx_row and first_tx_row["timestamp"] is not None else None

            count_row = await conn.fetchrow(
                f"SELECT COUNT(*) as cnt FROM transactions WHERE {from_col} = $1 OR {to_col} = $1",
                wallet,
            )
            tx_count = int(count_row["cnt"] or 0)

            if first_tx_ts is not None:
                timeline_events.append({
                    "date": first_tx_ts,
                    "event": "First transaction detected",
                    "type": "milestone",
                    "risk_level": "LOW",
                    "severity": 1,
                })

            if tx_count >= 10:
                timeline_events.append({
                    "date": first_tx_ts or 0,
                    "event": "Wallet became active (10+ transactions)",
                    "type": "milestone",
                    "risk_level": "LOW",
                    "severity": 1,
                })

            if tx_count >= 100:
                timeline_events.append({
                    "date": first_tx_ts or 0,
                    "event": "High activity wallet detected (100+ transactions)",
                    "type": "milestone",
                    "risk_level": "LOW",
                    "severity": 1,
                })

        await _add_propagation_events(conn, wallet, timeline_events, first_timestamp)

        if has_wallet_history and first_timestamp is not None:
            timeline_events.append({
                "date": first_timestamp,
                "event": "First activity detected",
                "type": "activity",
                "risk_level": "LOW",
                "severity": 1,
            })

        timeline_events = sorted(timeline_events, key=lambda x: x["date"])
    finally:
        await release_conn(conn)

    result = {"wallet": wallet, "timeline": timeline_events}
    timeline_cache[cache_key] = result
    return result


MAX_GRAPH_NODES = 100
MAX_GRAPH_EDGES = 200


@router.get("/explorer/identity/{wallet}/graph")
async def get_explorer_identity_graph(wallet: str) -> dict:
    """
    Return network graph data for Investigation Mode UI.
    Nodes: wallet addresses with risk (trust score).
    Edges: transaction relationships between wallets.
    Read-only. Uses transactions, trust_scores, wallet_clusters.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    cache_key = f"graph:{wallet}"
    if cache_key in graph_cache:
        return graph_cache[cache_key]

    conn = await get_conn()
    try:
        if not await _wallet_exists(conn, wallet):
            raise HTTPException(status_code=404, detail="Wallet not found")

        node_ids: set[str] = {wallet}
        edges_set: set[tuple[str, str]] = set()

        if await _table_exists(conn, "transactions"):
            tx_cols = await _get_table_columns(conn, "transactions")
            if "from_wallet" in tx_cols and "to_wallet" in tx_cols:
                from_col, to_col = "from_wallet", "to_wallet"
            else:
                from_col, to_col = "sender", "receiver"

            rows = await conn.fetch(
                f"SELECT {from_col}, {to_col} FROM transactions WHERE {from_col} = $1 OR {to_col} = $1 LIMIT {MAX_GRAPH_EDGES}",
                wallet,
            )

            for row in rows:
                a, b = (row[from_col] or "").strip(), (row[to_col] or "").strip()
                if not a or not b or a == b:
                    continue
                node_ids.add(a)
                node_ids.add(b)
                edge = (a, b) if a < b else (b, a)
                edges_set.add(edge)
                if len(edges_set) >= MAX_GRAPH_EDGES:
                    break

        for tbl in ("wallet_cluster_members", "wallet_graph_clusters", "wallet_clusters"):
            if not await _table_exists(conn, tbl):
                continue
            cols = await _get_table_columns(conn, tbl)
            if "cluster_id" not in cols or "wallet" not in cols:
                continue
            row = await conn.fetchrow(f"SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1", wallet)
            if row:
                cid = int(row["cluster_id"])
                cluster_rows = await conn.fetch(f"SELECT wallet FROM {tbl} WHERE cluster_id = $1", cid)
                for r in cluster_rows:
                    w = (r["wallet"] or "").strip()
                    if w and len(node_ids) < MAX_GRAPH_NODES:
                        node_ids.add(w)
                        edges_set.add((wallet, w) if wallet < w else (w, wallet))
                    if len(node_ids) >= MAX_GRAPH_NODES or len(edges_set) >= MAX_GRAPH_EDGES:
                        break
                break

        node_ids = set(sorted(node_ids)[:MAX_GRAPH_NODES])
        edges_list: list[dict] = []
        for (a, b) in list(edges_set)[:MAX_GRAPH_EDGES]:
            if a in node_ids and b in node_ids:
                edges_list.append({"source": a, "target": b})

        wallets_list = sorted(node_ids)
        scores: dict[str, float] = {}
        if wallets_list:
            ph = ",".join(f"${i+1}" for i in range(len(wallets_list)))
            rows = await conn.fetch(
                f"SELECT wallet, score FROM trust_scores WHERE wallet IN ({ph})",
                *wallets_list,
            )
            for row in rows:
                w = (row["wallet"] or "").strip()
                if w and w not in scores:
                    scores[w] = float(row["score"] or 0)

        nodes: list[dict] = []
        for w in wallets_list:
            nodes.append({"id": w, "risk": scores.get(w, 50.0)})
    finally:
        await release_conn(conn)

    result = {"wallet": wallet, "nodes": nodes, "edges": edges_list}
    graph_cache[cache_key] = result
    return result


MAX_NEIGHBORS = 20


@router.get("/explorer/identity/{wallet}/neighbors")
async def get_explorer_identity_neighbors(wallet: str) -> dict:
    """
    Return the most relevant wallets that interacted with the given wallet.
    Used by Investigation Mode, cluster visualization, and Trust analytics.
    Read-only. Uses transactions, trust_scores.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    conn = await get_conn()
    try:
        if not await _wallet_exists(conn, wallet):
            raise HTTPException(status_code=404, detail="Wallet not found")

        neighbors_data: list[tuple[str, int]] = []

        if await _table_exists(conn, "transactions"):
            tx_cols = await _get_table_columns(conn, "transactions")
            if "from_wallet" in tx_cols and "to_wallet" in tx_cols:
                from_col, to_col = "from_wallet", "to_wallet"
            else:
                from_col, to_col = "sender", "receiver"

            rows = await conn.fetch(
                f"""
                SELECT counterparty, COUNT(*) as cnt FROM (
                    SELECT {to_col} AS counterparty FROM transactions WHERE {from_col} = $1
                    UNION ALL
                    SELECT {from_col} AS counterparty FROM transactions WHERE {to_col} = $1
                ) sub
                WHERE counterparty IS NOT NULL AND counterparty != $1 AND counterparty != ''
                GROUP BY counterparty
                ORDER BY cnt DESC
                LIMIT {MAX_NEIGHBORS}
                """,
                wallet,
            )
            neighbors_data = [(str(r["counterparty"]).strip(), int(r["cnt"] or 0)) for r in rows if r and r["counterparty"]]

        scores: dict[str, float] = {}
        if neighbors_data:
            wallets_list = [w for w, _ in neighbors_data]
            ph = ",".join(f"${i+1}" for i in range(len(wallets_list)))
            rows = await conn.fetch(
                f"SELECT wallet, score FROM trust_scores WHERE wallet IN ({ph})",
                *wallets_list,
            )
            for row in rows:
                w = (row["wallet"] or "").strip()
                if w and w not in scores:
                    scores[w] = float(row["score"] or 0)

        neighbors: list[dict] = []
        for w, cnt in neighbors_data:
            neighbors.append({
                "wallet": w,
                "interactions": cnt,
                "risk_score": round(scores.get(w, 50.0), 1),
            })
    finally:
        await release_conn(conn)

    return {
        "wallet": wallet,
        "neighbors": neighbors,
    }


MAX_FLOW_EDGES = 50
HIGH_RISK_SCORE_THRESHOLD = 25.0


@router.get("/explorer/identity/{wallet}/flow-path")
async def get_explorer_identity_flow_path(wallet: str) -> dict:
    """
    Return wallet flow relationships for Fund Flow Visualization in Investigation Mode.
    Edges: cluster members and transaction counterparties.
    Read-only. Uses transactions, trust_scores, wallet_clusters.
    """
    wallet = wallet.strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    cache_key = f"flow:{wallet}"
    if cache_key in flow_cache:
        return flow_cache[cache_key]

    conn = await get_conn()
    try:
        if not await _wallet_exists(conn, wallet):
            raise HTTPException(status_code=404, detail="Wallet not found")

        edges_seen: dict[str, str] = {}
        connected_wallets: set[str] = set()

        for tbl in ("wallet_cluster_members", "wallet_graph_clusters", "wallet_clusters"):
            if not await _table_exists(conn, tbl):
                continue
            cols = await _get_table_columns(conn, tbl)
            if "cluster_id" not in cols or "wallet" not in cols:
                continue
            cluster_rows = await conn.fetch(
                f"SELECT wallet FROM {tbl} WHERE cluster_id = (SELECT cluster_id FROM {tbl} WHERE wallet = $1 LIMIT 1)",
                wallet,
            )
            for row in cluster_rows:
                w = (row["wallet"] or "").strip()
                if w and w != wallet and len(edges_seen) < MAX_FLOW_EDGES:
                    edges_seen[w] = "cluster"
                    connected_wallets.add(w)
            if edges_seen:
                break

        if await _table_exists(conn, "transactions") and len(edges_seen) < MAX_FLOW_EDGES:
            tx_cols = await _get_table_columns(conn, "transactions")
            if "from_wallet" in tx_cols and "to_wallet" in tx_cols:
                from_col, to_col = "from_wallet", "to_wallet"
            else:
                from_col, to_col = "sender", "receiver"

            rows = await conn.fetch(
                f"""
                SELECT DISTINCT counterparty FROM (
                    SELECT {to_col} AS counterparty FROM transactions WHERE {from_col} = $1
                    UNION
                    SELECT {from_col} AS counterparty FROM transactions WHERE {to_col} = $1
                ) sub
                WHERE counterparty IS NOT NULL AND counterparty != $1 AND counterparty != ''
                LIMIT {MAX_FLOW_EDGES}
                """,
                wallet,
            )
            for row in rows:
                w = (row["counterparty"] or "").strip()
                if w and len(edges_seen) < MAX_FLOW_EDGES:
                    if w not in edges_seen:
                        edges_seen[w] = "interaction"
                        connected_wallets.add(w)

        scores: dict[str, float] = {}
        if connected_wallets:
            wl = list(connected_wallets)
            ph = ",".join(f"${i+1}" for i in range(len(wl)))
            rows = await conn.fetch(
                f"SELECT wallet, score FROM trust_scores WHERE wallet IN ({ph})",
                *wl,
            )
            for row in rows:
                w = (row["wallet"] or "").strip()
                if w and w not in scores:
                    scores[w] = float(row["score"] or 0)

        flows: list[dict] = []
        for target, rel in list(edges_seen.items())[:MAX_FLOW_EDGES]:
            edge: dict = {"source": wallet, "target": target, "relationship": rel}
            if scores.get(target, 50.0) < HIGH_RISK_SCORE_THRESHOLD:
                edge["risk"] = "HIGH"
            flows.append(edge)
    finally:
        await release_conn(conn)

    result = {"wallet": wallet, "flows": flows}
    flow_cache[cache_key] = result
    return result


@router.get("/explorer/identity/{wallet}/activity-heatmap")
async def get_activity_heatmap(wallet: str) -> dict:
    """
    Return transaction activity grouped by day of week and hour.
    Used for Money Flow Heatmap visualization.
    """
    wallet = wallet.strip()
    try:
        conn = await get_conn()
    except Exception:
        return {"heatmap": []}

    try:
        rows = await conn.fetch(
            """
            SELECT
                EXTRACT(DOW FROM to_timestamp(timestamp))::int AS day,
                EXTRACT(HOUR FROM to_timestamp(timestamp))::int AS hour,
                COUNT(*) AS tx_count
            FROM transactions
            WHERE wallet = $1 AND timestamp IS NOT NULL
            GROUP BY day, hour
            ORDER BY day, hour
            """,
            wallet,
        )
        heatmap = [[0] * 24 for _ in range(7)]
        for row in rows:
            d = int(row["day"])
            h = int(row["hour"])
            heatmap[d][h] = int(row["tx_count"])
        return {"heatmap": heatmap}
    except Exception as e:
        return {"heatmap": []}
    finally:
        await release_conn(conn)
