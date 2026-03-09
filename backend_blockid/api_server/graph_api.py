"""
BlockID Graph JSON API — export wallet interaction graph for D3.js visualization.

Investigation Explorer Graph Panel: GET /wallet/{wallet}/graph
- Nodes: id, label, badge, risk, cluster_id, color, distance
- Edges: source, target, amount, timestamp
- Uses PostgreSQL (asyncpg).
"""
from __future__ import annotations

import csv
import time
from collections import deque
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn
from backend_blockid.tools.badge_engine import get_badge

logger = get_logger(__name__)

router = APIRouter(prefix="/graph", tags=["graph"])
investigation_router = APIRouter(prefix="/wallet", tags=["investigation-graph"])

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

MAX_EDGES = 5000
MIN_AMOUNT_DEFAULT = 0.001


async def _table_exists(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _load_cluster_wallets(conn, cluster_id: int) -> set[str]:
    for table, col in [
        ("wallet_cluster_members", "wallet"),
        ("wallet_graph_clusters", "wallet"),
        ("wallet_clusters", "wallet"),
    ]:
        if not await _table_exists(conn, table):
            continue
        try:
            rows = await conn.fetch(f"SELECT {col} FROM {table} WHERE cluster_id=$1", cluster_id)
            wallets = {(r.get(col) or "").strip() for r in rows if r}
            if wallets:
                return {w for w in wallets if w}
        except Exception:
            continue
    return set()


async def _get_cluster_for_wallet(conn, wallet: str) -> int | None:
    for table in ["wallet_cluster_members", "wallet_graph_clusters", "wallet_clusters"]:
        if not await _table_exists(conn, table):
            continue
        try:
            row = await conn.fetchrow(f"SELECT cluster_id FROM {table} WHERE wallet=$1 LIMIT 1", wallet)
            if row and row.get("cluster_id") is not None:
                return int(row["cluster_id"])
        except Exception:
            continue
    return None


async def _load_transactions(
    conn,
    cluster_wallets: set[str],
    min_amount: float,
    max_edges: int,
    days_back: int | None,
) -> list[tuple[str, str, float]]:
    if not cluster_wallets:
        return []
    wl = list(cluster_wallets)
    n = len(wl)
    ph1 = ",".join(f"${i+1}" for i in range(n))
    ph2 = ",".join(f"${i+n+1}" for i in range(n))
    params_base = wl + wl
    cutoff = int(time.time()) - (days_back * 86400) if days_back else 0

    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
    )
    cols = {r["column_name"] for r in rows}

    if "from_wallet" in cols and "to_wallet" in cols:
        if days_back:
            rows = await conn.fetch(
                f"""
                SELECT from_wallet, to_wallet, amount
                FROM transactions
                WHERE from_wallet IN ({ph1}) AND to_wallet IN ({ph2})
                  AND amount >= ${n*2+1} AND timestamp >= ${n*2+2}
                LIMIT ${n*2+3}
                """,
                *params_base, min_amount, cutoff, max_edges,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT from_wallet, to_wallet, amount
                FROM transactions
                WHERE from_wallet IN ({ph1}) AND to_wallet IN ({ph2})
                  AND amount >= ${n*2+1}
                LIMIT ${n*2+2}
                """,
                *params_base, min_amount, max_edges,
            )
    else:
        if days_back:
            rows = await conn.fetch(
                f"""
                SELECT sender AS from_wallet, receiver AS to_wallet, amount_lamports / 1e9 AS amount
                FROM transactions
                WHERE sender IN ({ph1}) AND receiver IN ({ph2})
                  AND amount_lamports / 1e9 >= ${n*2+1} AND timestamp >= ${n*2+2}
                LIMIT ${n*2+3}
                """,
                *params_base, min_amount, cutoff, max_edges,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT sender AS from_wallet, receiver AS to_wallet, amount_lamports / 1e9 AS amount
                FROM transactions
                WHERE sender IN ({ph1}) AND receiver IN ({ph2})
                  AND amount_lamports / 1e9 >= ${n*2+1}
                LIMIT ${n*2+2}
                """,
                *params_base, min_amount, max_edges,
            )
    out = []
    for r in rows:
        frm = (r.get("from_wallet") or "").strip()
        to = (r.get("to_wallet") or "").strip()
        amt = float(r.get("amount") or 0)
        if frm and to and frm != to:
            out.append((frm, to, amt))
    return out


async def _load_scores(conn, wallets: set[str]) -> dict[str, tuple[float, str]]:
    if not wallets:
        return {}
    wl = list(wallets)
    ph = ",".join(f"${i+1}" for i in range(len(wl)))
    try:
        rows = await conn.fetch(
            f"SELECT wallet, score, risk_level FROM trust_scores WHERE wallet IN ({ph})",
            *wl,
        )
    except Exception:
        rows = await conn.fetch(
            f"SELECT wallet, score FROM trust_scores WHERE wallet IN ({ph})",
            *wl,
        )
    out = {}
    for r in rows:
        w = (r.get("wallet") or "").strip()
        s = float(r.get("score") or 50)
        risk = str(r.get("risk_level") or "1") if "risk_level" in r else "1"
        if w:
            out[w] = (s, risk)
    return out


def _build_graph_json(
    cluster_wallets: set[str],
    tx_rows: list[tuple[str, str, float]],
    scores: dict[str, tuple[float, str]],
    max_nodes: int,
) -> dict[str, Any]:
    node_ids: set[str] = set(cluster_wallets)
    for frm, to, _ in tx_rows:
        node_ids.add(frm)
        node_ids.add(to)
    if max_nodes and len(node_ids) > max_nodes:
        node_ids = set(list(node_ids)[:max_nodes])
    nodes = []
    for nid in node_ids:
        s, risk = scores.get(nid, (50.0, "1"))
        nodes.append({"id": nid, "score": round(s, 2), "risk": risk})
    seen_edges: set[tuple[str, str]] = set()
    links = []
    for frm, to, amt in tx_rows:
        if frm not in node_ids or to not in node_ids:
            continue
        key = (frm, to)
        if key in seen_edges:
            continue
        seen_edges.add(key)
        links.append({"source": frm, "target": to, "amount": round(amt, 4)})
    return {"nodes": nodes, "links": links}


@router.get("/cluster/{cluster_id}")
async def get_cluster_graph(
    cluster_id: int,
    min_amount: float = Query(MIN_AMOUNT_DEFAULT, ge=0),
    max_nodes: int = Query(0, ge=0),
    days_back: int = Query(0, ge=0),
) -> dict[str, Any]:
    """Return D3.js-compatible graph JSON for a cluster."""
    conn = await get_conn()
    try:
        cluster_wallets = await _load_cluster_wallets(conn, cluster_id)
        if not cluster_wallets:
            raise HTTPException(status_code=404, detail=f"Cluster {cluster_id} not found")
        tx_rows = await _load_transactions(
            conn, cluster_wallets, min_amount, MAX_EDGES, days_back if days_back else None
        )
        all_wallets = cluster_wallets | {r[0] for r in tx_rows} | {r[1] for r in tx_rows}
        scores = await _load_scores(conn, all_wallets)
        data = _build_graph_json(cluster_wallets, tx_rows, scores, max_nodes or 0)
        logger.info("graph_api", cluster=cluster_id, nodes=len(data["nodes"]), links=len(data["links"]))
        return data
    finally:
        await release_conn(conn)


@router.get("/wallet/{wallet}")
async def get_wallet_graph(
    wallet: str,
    depth: int = Query(2, ge=1, le=5),
    min_amount: float = Query(MIN_AMOUNT_DEFAULT, ge=0),
    max_nodes: int = Query(200, ge=0),
    days_back: int = Query(30, ge=0),
) -> dict[str, Any]:
    """Return subgraph of neighbors for a wallet (by cluster)."""
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    conn = await get_conn()
    try:
        cluster_id = await _get_cluster_for_wallet(conn, wallet)
        if cluster_id is None:
            raise HTTPException(status_code=404, detail="Wallet not in any cluster")
        cluster_wallets = await _load_cluster_wallets(conn, cluster_id)
        if wallet not in cluster_wallets:
            cluster_wallets.add(wallet)
        tx_rows = await _load_transactions(
            conn, cluster_wallets, min_amount, MAX_EDGES, days_back if days_back else None
        )

        import networkx as nx
        G = nx.Graph()
        for frm, to, amt in tx_rows:
            G.add_edge(frm, to, weight=amt)
        if wallet not in G:
            G.add_node(wallet)
        neighbors = set(nx.ego_graph(G, wallet, radius=depth).nodes())
        sub_tx = [(f, t, a) for f, t, a in tx_rows if f in neighbors and t in neighbors]
        scores = await _load_scores(conn, neighbors)
        data = _build_graph_json(neighbors, sub_tx, scores, max_nodes or 0)
        logger.info("graph_api", wallet=wallet[:16] + "...", nodes=len(data["nodes"]), links=len(data["links"]))
        return data
    finally:
        await release_conn(conn)


async def _load_scam_wallets(conn) -> set[str]:
    scams: set[str] = set()
    if await _table_exists(conn, "scam_wallets"):
        try:
            rows = await conn.fetch("SELECT wallet FROM scam_wallets")
            for r in rows:
                w = (r.get("wallet") or "").strip()
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


async def _load_tx_edges(
    conn, wallet_set: set[str], min_amount: float, days_back: int, max_edges: int
) -> list[tuple[str, str, float, int]]:
    if not wallet_set:
        return []
    wl = list(wallet_set)[:1000]
    n = len(wl)
    ph1 = ",".join(f"${i+1}" for i in range(n))
    ph2 = ",".join(f"${i+n+1}" for i in range(n))
    params = list(wl) + list(wl)
    cutoff = int(time.time()) - (days_back * 86400) if days_back else 0

    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name='transactions'"
    )
    cols = {r["column_name"] for r in rows}
    has_ts = "timestamp" in cols

    if "from_wallet" in cols and "to_wallet" in cols:
        if has_ts and days_back:
            rows = await conn.fetch(
                f"""
                SELECT from_wallet, to_wallet, amount, COALESCE(timestamp, 0) AS ts
                FROM transactions
                WHERE (from_wallet IN ({ph1}) OR to_wallet IN ({ph2}))
                  AND amount >= ${n*2+1} AND timestamp >= ${n*2+2}
                LIMIT ${n*2+3}
                """,
                *params, min_amount, cutoff, max_edges,
            )
        else:
            rows = await conn.fetch(
                f"""
                SELECT from_wallet, to_wallet, amount, 0 AS ts
                FROM transactions
                WHERE (from_wallet IN ({ph1}) OR to_wallet IN ({ph2}))
                  AND amount >= ${n*2+1}
                LIMIT ${n*2+2}
                """,
                *params, min_amount, max_edges,
            )
    else:
        rows = await conn.fetch(
            f"""
            SELECT sender AS from_wallet, receiver AS to_wallet, amount_lamports/1e9 AS amount, COALESCE(timestamp,0) AS ts
            FROM transactions
            WHERE (sender IN ({ph1}) OR receiver IN ({ph2}))
              AND amount_lamports/1e9 >= ${n*2+1}
            LIMIT ${n*2+2}
            """,
            *params, min_amount, max_edges,
        )
    out = []
    for r in rows:
        frm = (r.get("from_wallet") or "").strip()
        to = (r.get("to_wallet") or "").strip()
        amt = float(r.get("amount") or 0)
        ts = int(r.get("ts") or 0)
        if frm and to and frm != to:
            out.append((frm, to, amt, ts))
    return out


def _compute_distances_from_scam(
    node_set: set[str],
    edge_list: list[tuple[str, str, float, int]],
    scam_wallets: set[str],
) -> dict[str, int]:
    import networkx as nx
    G = nx.Graph()
    for frm, to, _, _ in edge_list:
        G.add_edge(frm, to)
    for n in node_set:
        if n not in G:
            G.add_node(n)
    dist: dict[str, int] = {}
    for scam in scam_wallets:
        if scam not in G:
            continue
        queue = deque([(scam, 0)])
        visited = {scam}
        while queue:
            node, d = queue.popleft()
            dist[node] = min(dist.get(node, 999), d)
            if d >= 5:
                continue
            for nbr in G.neighbors(node):
                if nbr not in visited:
                    visited.add(nbr)
                    queue.append((nbr, d + 1))
    for n in node_set:
        if n not in dist:
            dist[n] = 999
    return dist


async def _fetch_investigation_graph(
    wallet: str,
    depth: int,
    mode: str,
    min_amount: float,
    days_back: int,
    max_nodes: int,
) -> dict[str, Any]:
    conn = await get_conn()
    try:
        scam_wallets = await _load_scam_wallets(conn)
        cluster_id = await _get_cluster_for_wallet(conn, wallet)
        cluster_wallets = await _load_cluster_wallets(conn, cluster_id) if cluster_id is not None else {wallet}
        if wallet not in cluster_wallets:
            cluster_wallets.add(wallet)

        tx_rows = await _load_tx_edges(conn, cluster_wallets, min_amount, days_back, MAX_EDGES)

        import networkx as nx
        G = nx.Graph()
        for frm, to, amt, ts in tx_rows:
            G.add_edge(frm, to, amount=amt, timestamp=ts)
        if wallet not in G:
            G.add_node(wallet)
        neighbors = set(nx.ego_graph(G, wallet, radius=min(depth, 3)).nodes())
        sub_edges = [(f, t, a, ts) for f, t, a, ts in tx_rows if f in neighbors and t in neighbors]

        if mode == "scam_only":
            scam_or_neighbor = scam_wallets | neighbors
            sub_edges = [(f, t, a, ts) for f, t, a, ts in sub_edges if f in scam_or_neighbor and t in scam_or_neighbor]
            node_ids = {f for f, _, _, _ in sub_edges} | {t for _, t, _, _ in sub_edges} | {wallet}
            neighbors = neighbors & node_ids
        else:
            node_ids = neighbors

        if max_nodes and len(node_ids) > max_nodes:
            node_ids = set(list(node_ids)[:max_nodes])
            sub_edges = [(f, t, a, ts) for f, t, a, ts in sub_edges if f in node_ids and t in node_ids]

        scores = await _load_scores(conn, node_ids)
        distances = _compute_distances_from_scam(node_ids, sub_edges, scam_wallets)

        nodes = []
        for nid in node_ids:
            s, risk = scores.get(nid, (50.0, "1"))
            badge = get_badge(s)
            risk_int = int(risk) if str(risk).isdigit() else 1
            is_scam = nid in scam_wallets or risk_int >= 3
            color = "red" if is_scam else "blue"
            nodes.append({
                "id": nid,
                "label": nid[:8] + "..." if len(nid) > 8 else nid,
                "badge": badge,
                "risk": risk,
                "cluster_id": cluster_id,
                "color": color,
                "distance": distances.get(nid, 999),
                "score": round(s, 2),
            })

        seen: set[tuple[str, str]] = set()
        edges = []
        for frm, to, amt, ts in sub_edges:
            if frm not in node_ids or to not in node_ids:
                continue
            key = (frm, to) if frm < to else (to, frm)
            if key in seen:
                continue
            seen.add(key)
            edges.append({"source": frm, "target": to, "amount": round(amt, 4), "timestamp": ts})

        return {"nodes": nodes, "edges": edges}
    finally:
        await release_conn(conn)


@investigation_router.get("/{wallet}/graph")
async def get_investigation_graph(
    wallet: str,
    mode: str = Query("all", description="scam_only or all"),
    depth: int = Query(2, ge=1, le=5),
    min_amount: float = Query(MIN_AMOUNT_DEFAULT, ge=0),
    days_back: int = Query(30, ge=0),
    max_nodes: int = Query(200, ge=0),
) -> dict[str, Any]:
    """
    Investigation Explorer Graph — wallet cluster and scam propagation.
    Nodes: id, label, badge, risk, cluster_id, color, distance.
    Edges: source, target, amount, timestamp.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet must be non-empty")

    try:
        data = await _fetch_investigation_graph(wallet, depth, mode, min_amount, days_back, max_nodes)
    except Exception as e:
        logger.exception("investigation_graph_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e

    n_nodes = len(data["nodes"])
    n_edges = len(data["edges"])
    logger.info("graph_api", wallet=wallet[:16] + "...", nodes=n_nodes, edges=n_edges)
    return data
