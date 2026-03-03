"""
Trust Badge History Chart — visualize trust score over time.

Uses matplotlib only. One wallet per chart.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from backend_blockid.database.connection import get_connection

_CHARTS_DIR = Path(__file__).resolve().parent.parent / "charts"

THRESHOLDS = [(80, "Trusted"), (50, "Caution"), (20, "Risky")]


def plot_wallet(wallet: str) -> Path | None:
    """
    Load history, plot score over time, save to charts/{wallet}.png.
    Returns path to saved image or None if empty history.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT timestamp, score
        FROM wallet_score_history
        WHERE wallet = ?
        ORDER BY timestamp
        """,
        (wallet.strip(),),
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        print(f"[chart] wallet={wallet[:16]}... points=0 (no history)")
        return None

    timestamps = []
    scores = []
    for r in rows:
        ts = int(r["timestamp"] if hasattr(r, "keys") else r[0])
        sc = float(r["score"] if hasattr(r, "keys") else r[1])
        timestamps.append(datetime.fromtimestamp(ts))
        scores.append(sc)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(timestamps, scores, marker="o", markersize=4, linestyle="-")

    for thresh, label in THRESHOLDS:
        ax.axhline(y=thresh, color="gray", linestyle="--", alpha=0.6)
        ax.text(timestamps[0], thresh + 1, label, fontsize=8, alpha=0.7)

    ax.set_ylim(0, 105)
    ax.set_xlabel("Time")
    ax.set_ylabel("Trust Score")
    ax.set_title(f"BlockID Trust Score History - {wallet[:16]}...")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.xticks(rotation=45)
    plt.tight_layout()

    _CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_name = wallet.replace("/", "_").replace("\\", "_")[:64]
    out_path = _CHARTS_DIR / f"{safe_name}.png"
    plt.savefig(out_path, dpi=100)
    plt.close()

    print(f"[chart] wallet={wallet[:16]}... points={len(scores)} saved={out_path}")
    return out_path


def plot_top_risky_wallets(limit: int = 10) -> list[Path]:
    """Load wallets with lowest latest score, plot each."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT h.wallet
        FROM wallet_score_history h
        INNER JOIN (
            SELECT wallet, MAX(timestamp) AS max_ts
            FROM wallet_score_history
            GROUP BY wallet
        ) latest ON h.wallet = latest.wallet AND h.timestamp = latest.max_ts
        ORDER BY h.score ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()

    wallets = [(r["wallet"] if hasattr(r, "keys") else r[0]).strip() for r in rows if r]
    paths = []
    for w in wallets:
        if w:
            p = plot_wallet(w)
            if p:
                paths.append(p)
    return paths


def main() -> int:
    ap = argparse.ArgumentParser(description="Plot trust score history for a wallet.")
    ap.add_argument(
        "wallet",
        nargs="?",
        default=None,
        help="Wallet address to plot",
    )
    ap.add_argument(
        "--top-risky",
        type=int,
        default=0,
        help="Plot top N lowest-scoring wallets instead",
    )
    args = ap.parse_args()

    if args.top_risky > 0:
        paths = plot_top_risky_wallets(limit=args.top_risky)
        return 0 if paths else 1

    wallet = (args.wallet or "").strip()
    if not wallet:
        print("Usage: py -m backend_blockid.tools.plot_wallet_history <wallet>")
        print("   or: py -m backend_blockid.tools.plot_wallet_history --top-risky 10")
        return 1

    path = plot_wallet(wallet)
    return 0 if path else 1


if __name__ == "__main__":
    sys.exit(main())
