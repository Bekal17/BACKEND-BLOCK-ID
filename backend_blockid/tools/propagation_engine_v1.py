"""
Scam propagation scoring engine using graph distance, volume, and time decay.

BFS from scam wallets up to max_depth; applies distance penalty, volume factor,
and time decay. Updates wallet_reasons and trust_scores.

Usage:
    py -m backend_blockid.tools.propagation_engine_v1
    py -m backend_blockid.tools.propagation_engine_v1 --days-back 30 --max-depth 3

Config: loads backend_blockid/models/propagation_config.json when present.
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import math
import sys
import time
from collections import deque
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import networkx as nx

from backend_blockid.database.pg_connection import get_conn, release_conn

_DATA_DIR = Path(__file__).resolve().parent.parent / "data"
_CONFIG_PATH = Path(__file__).resolve().parent.parent / "models" / "propagation_config.json"
SCAM_WALLETS_CSV = _DATA_DIR / "scam_wallets.csv"

DEFAULT_DISTANCE_PENALTY = {1: -40, 2: -20, 3: -5}
MIN_AMOUNT_SOL = 0.001


def _load_config() -> dict:
    """Load propagation config from JSON. Returns defaults if missing."""
    cfg = {
        "distance_penalty": dict(DEFAULT_DISTANCE_PENALTY),
        "volume_scale": 1.0,
        "time_decay_days": 30,
        "max_depth": 3,
    }
    if _CONFIG_PATH.exists():
        try:
            with open(_CONFIG_PATH, encoding="utf-8") as f:
                loaded = json.load(f)
            if "distance_penalty" in loaded:
                cfg["distance_penalty"] = {int(k): int(v) for k, v in loaded["distance_penalty"].items()}
            if "volume_scale" in loaded:
                cfg["volume_scale"] = float(loaded["volume_scale"])
            if "time_decay_days" in loaded:
                cfg["time_decay_days"] = int(loaded["time_decay_days"])
            if "max_depth" in loaded:
                cfg["max_depth"] = int(loaded["max_depth"])
        except Exception:
            pass
    return cfg


async def _table_exists_async(conn, table: str) -> bool:
    row = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name=$1)",
        table,
    )
    return bool(row)


async def _get_table_columns_async(conn, table: str) -> set[str]:
    rows = await conn.fetch(
        "SELECT column_name FROM information_schema.columns WHERE table_name=$1",
        table,
    )
    return {r["column_name"] for r in rows}


async def _load_scam_wallets_async(conn) -> set[str]:
    scams: set[str] = set()
    try:
        if await _table_exists_async(conn, "scam_wallets"):
            rows = await conn.fetch("SELECT wallet FROM scam_wallets")
            for r in rows:
                w = (r["wallet"] or "").strip()
                if w:
                    scams.add(w)
    except Exception:
        pass

    if not scams and SCAM_WALLETS_CSV.exists():
        try:
            with open(SCAM_WALLETS_CSV, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    w = (row.get("wallet") or "").strip()
                    if w:
                        scams.add(w)
        except Exception:
            pass
    return scams


async def _load_transactions_async(conn, days_back: int, min_amount: float) -> list:
    if not await _table_exists_async(conn, "transactions"):
        return []

    cols = await _get_table_columns_async(conn, "transactions")
    cutoff = int(time.time()) - (days_back * 86400)

    if "from_wallet" in cols and "to_wallet" in cols and "timestamp" in cols:
        rows = await conn.fetch(
            """
            SELECT from_wallet, to_wallet, amount, timestamp
            FROM transactions
            WHERE timestamp >= $1 AND from_wallet IS NOT NULL AND to_wallet IS NOT NULL
            """,
            cutoff,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT sender AS from_wallet, receiver AS to_wallet,
                   amount_lamports / 1e9 AS amount, timestamp
            FROM transactions
            WHERE timestamp >= $1 AND sender IS NOT NULL AND receiver IS NOT NULL
            """,
            cutoff,
        )
    return rows


def _build_graph(rows, min_amount: float) -> nx.Graph:
    G = nx.Graph()
    for r in rows:
        frm = (r["from_wallet"] or "").strip()
        to = (r["to_wallet"] or "").strip()
        amt = float(r["amount"] or 0)
        ts = int(r["timestamp"] or 0)
        if not frm or not to or frm == to or amt < min_amount:
            continue
        if G.has_edge(frm, to):
            G[frm][to]["amount"] += amt
            G[frm][to]["timestamp"] = max(G[frm][to].get("timestamp", 0), ts)
        else:
            G.add_edge(frm, to, amount=amt, timestamp=ts)
    return G


def _volume_factor(amount: float, volume_scale: float = 1.0) -> float:
    base = min(1.0, math.log10(amount + 1) / 3)
    return base * volume_scale


def _time_factor(timestamp: int, time_decay_days: float = 30) -> float:
    now = int(time.time())
    days_old = (now - timestamp) / 86400
    return math.exp(-days_old / max(time_decay_days, 1))


def _bfs_penalties(
    G: nx.Graph,
    scam_wallets: set[str],
    max_depth: int,
    distance_penalty: dict[int, int] | None = None,
    volume_scale: float = 1.0,
    time_decay_days: float = 30,
) -> dict[str, float]:
    dist_pen = distance_penalty or DEFAULT_DISTANCE_PENALTY
    total_penalty: dict[str, float] = {}
    for scam in scam_wallets:
        if scam not in G:
            continue
        visited: set[str] = set()
        queue: deque[tuple[str, int, float, int]] = deque([(scam, 0, 0.0, 0)])
        while queue:
            node, depth, edge_amount, edge_ts = queue.popleft()
            if depth > 0:
                if node in visited:
                    continue
                visited.add(node)
                if node not in scam_wallets and depth <= max_depth and depth in dist_pen:
                    pen = float(dist_pen[depth])
                    pen *= _volume_factor(edge_amount, volume_scale)
                    pen *= _time_factor(edge_ts, time_decay_days)
                    total_penalty[node] = total_penalty.get(node, 0.0) + pen
            if depth >= max_depth:
                continue
            for nbr in G.neighbors(node):
                if nbr in visited:
                    continue
                data = G[node][nbr]
                amt = data.get("amount", 0.0)
                ts = data.get("timestamp", 0)
                queue.append((nbr, depth + 1, amt, ts))
    return total_penalty


async def run_propagation_simulation_async(
    days_back: int = 30,
    config: dict | None = None,
) -> dict[str, float]:
    """
    Run propagation in simulation mode. Returns {wallet: penalty}.
    Does NOT write to DB. Used by optimize_propagation_weights.
    """
    cfg = config or _load_config()
    conn = await get_conn()
    try:
        scam_wallets = await _load_scam_wallets_async(conn)
        if not scam_wallets:
            return {}
        rows = await _load_transactions_async(conn, days_back, MIN_AMOUNT_SOL)
    finally:
        await release_conn(conn)

    G = _build_graph(rows, MIN_AMOUNT_SOL)
    dist_pen = cfg.get("distance_penalty") or {}
    dist_pen = {int(k): int(v) for k, v in dist_pen.items()} if dist_pen else None
    total_penalty = _bfs_penalties(
        G,
        scam_wallets,
        cfg.get("max_depth", 3),
        distance_penalty=dist_pen,
        volume_scale=cfg.get("volume_scale", 1.0),
        time_decay_days=cfg.get("time_decay_days", 30),
    )
    return total_penalty


def run_propagation_simulation(
    conn=None,  # deprecated parameter
    days_back: int = 30,
    config: dict | None = None,
) -> dict[str, float]:
    """Sync wrapper for run_propagation_simulation_async."""
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(run_propagation_simulation_async(days_back, config))


async def update_propagation_for_wallets_async(
    target_wallets: set[str],
    days_back: int = 30,
    config: dict | None = None,
) -> dict[str, float]:
    """
    Run propagation and update wallet_reasons + trust_scores only for target wallets.
    Returns {wallet: penalty} for wallets that were updated.
    Used by realtime risk engine.
    """
    total_penalty = await run_propagation_simulation_async(days_back, config)
    affected = {w for w in target_wallets if w in total_penalty and total_penalty[w] != 0}
    if not affected:
        return {}

    conn = await get_conn()
    try:
        now_ts = int(time.time())

        for col, typ in [
            ("confidence_score", "DOUBLE PRECISION"),
            ("tx_hash", "TEXT"),
            ("tx_link", "TEXT"),
            ("created_at", "BIGINT"),
        ]:
            try:
                await conn.execute(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='wallet_reasons' AND column_name='{col}'
                        ) THEN
                            ALTER TABLE wallet_reasons ADD COLUMN {col} {typ};
                        END IF;
                    END $$;
                """)
            except Exception:
                pass

        for wallet in affected:
            pen = total_penalty[wallet]
            try:
                await conn.execute(
                    """
                    INSERT INTO wallet_reasons
                    (wallet, reason_code, weight, confidence_score, tx_hash, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (wallet, reason_code) DO UPDATE SET
                        weight = $3,
                        confidence_score = $4,
                        created_at = $6
                    """,
                    wallet, "SCAM_CLUSTER_MEMBER", int(round(pen)), min(1.0, 0.5), None, now_ts,
                )
            except Exception as e:
                print(f"[propagation] WARNING: wallet_reasons insert failed for {wallet[:8]}...: {e}")

        rows = await conn.fetch("SELECT wallet, score FROM trust_scores")
        existing = {r["wallet"]: float(r["score"] or 50) for r in rows}
        for wallet in affected:
            base = existing.get(wallet, 50.0)
            final = max(0.0, base + total_penalty[wallet])
            try:
                result = await conn.execute(
                    "UPDATE trust_scores SET score = $1, updated_at = $2 WHERE wallet = $3",
                    final, now_ts, wallet,
                )
                if "UPDATE 0" in result:
                    await conn.execute(
                        "INSERT INTO trust_scores (wallet, score, updated_at) VALUES ($1, $2, $3)",
                        wallet, final, now_ts,
                    )
            except Exception as e:
                print(f"[propagation] WARNING: trust_scores update failed for {wallet[:8]}...: {e}")
    finally:
        await release_conn(conn)

    return {w: total_penalty[w] for w in affected}


def update_propagation_for_wallets(
    conn=None,  # deprecated parameter
    target_wallets: set[str] | None = None,
    days_back: int = 30,
    config: dict | None = None,
) -> dict[str, float]:
    """Sync wrapper for update_propagation_for_wallets_async."""
    if target_wallets is None:
        target_wallets = set()
    loop = asyncio.get_event_loop()
    return loop.run_until_complete(update_propagation_for_wallets_async(target_wallets, days_back, config))


async def main_async() -> int:
    ap = argparse.ArgumentParser(description="Scam propagation scoring engine.")
    ap.add_argument("--days-back", type=int, default=30, help="Include txs within last N days")
    ap.add_argument("--max-depth", type=int, default=None, help="BFS max depth (default from config)")
    args = ap.parse_args()

    cfg = _load_config()
    max_depth = args.max_depth if args.max_depth is not None else cfg["max_depth"]
    cfg["max_depth"] = max_depth

    conn = await get_conn()
    try:
        scam_wallets = await _load_scam_wallets_async(conn)
        if not scam_wallets:
            print("[propagation] No scam wallets (scam_wallets table or scam_wallets.csv)")
            return 0

        rows = await _load_transactions_async(conn, args.days_back, MIN_AMOUNT_SOL)
    finally:
        await release_conn(conn)

    G = _build_graph(rows, MIN_AMOUNT_SOL)

    total_penalty = _bfs_penalties(
        G,
        scam_wallets,
        max_depth,
        distance_penalty=cfg.get("distance_penalty"),
        volume_scale=cfg.get("volume_scale", 1.0),
        time_decay_days=cfg.get("time_decay_days", 30),
    )
    affected = {w for w in total_penalty if total_penalty[w] != 0}
    if not affected:
        print("[propagation] scam_wallets=", len(scam_wallets), " affected_wallets=0")
        return 0

    penalties_list = [total_penalty[w] for w in affected]
    avg_penalty = sum(penalties_list) / len(penalties_list)

    conn = await get_conn()
    try:
        now_ts = int(time.time())

        for col, typ in [
            ("confidence_score", "DOUBLE PRECISION"),
            ("tx_hash", "TEXT"),
            ("tx_link", "TEXT"),
            ("created_at", "BIGINT"),
        ]:
            try:
                await conn.execute(f"""
                    DO $$ BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_name='wallet_reasons' AND column_name='{col}'
                        ) THEN
                            ALTER TABLE wallet_reasons ADD COLUMN {col} {typ};
                        END IF;
                    END $$;
                """)
            except Exception:
                pass

        for wallet in affected:
            pen = total_penalty[wallet]
            interaction_count = G.degree(wallet) if wallet in G else 0
            confidence = min(1.0, interaction_count / 10.0)
            try:
                await conn.execute(
                    """
                    INSERT INTO wallet_reasons
                    (wallet, reason_code, weight, confidence_score, tx_hash, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (wallet, reason_code) DO UPDATE SET
                        weight = $3,
                        confidence_score = $4,
                        created_at = $6
                    """,
                    wallet, "SCAM_CLUSTER_MEMBER", int(round(pen)), confidence, None, now_ts,
                )
            except Exception as e:
                print(f"[propagation] WARNING: wallet_reasons insert failed for {wallet[:8]}...: {e}")
                continue

        rows = await conn.fetch("SELECT wallet, score FROM trust_scores")
        existing = {r["wallet"]: float(r["score"] or 50) for r in rows}
        for wallet in affected:
            base = existing.get(wallet, 50.0)
            final = max(0.0, base + total_penalty[wallet])
            try:
                await conn.execute(
                    "UPDATE trust_scores SET score = $1, updated_at = $2 WHERE wallet = $3",
                    final, now_ts, wallet,
                )
            except Exception as e:
                print(f"[propagation] WARNING: trust_scores update failed for {wallet[:8]}...: {e}")
    finally:
        await release_conn(conn)

    print(f"[propagation] scam_wallets={len(scam_wallets)}")
    print(f"[propagation] affected_wallets={len(affected)}")
    print(f"[propagation] avg_penalty={avg_penalty:.1f}")
    return 0


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    sys.exit(main())
