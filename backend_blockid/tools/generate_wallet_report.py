"""
BlockID Wallet Investigation Report Generator.

Produces PDF report for compliance and exchange review.
"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from backend_blockid.database.connection import get_connection

_REPORTS_DIR = Path(__file__).resolve().parent.parent / "reports"


def generate_wallet_report(
    wallet: str, output_path: str | Path | None = None
) -> tuple[Path | None, dict | None]:
    """
    Load wallet data from DB and generate PDF report.
    Returns path to saved PDF or None if wallet not found.
    """
    wallet = (wallet or "").strip()
    if not wallet:
        return None, None

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT score, risk_level FROM trust_scores WHERE wallet = ? LIMIT 1", (wallet,))
    ts_row = cur.fetchone()
    if not ts_row:
        conn.close()
        return None, None

    score = round(float(ts_row[0] or 50), 2)
    risk = str(ts_row[1] or "1")

    cur.execute(
        """
        SELECT reason_code, weight, confidence_score
        FROM wallet_reasons
        WHERE wallet = ? AND reason_code IS NOT NULL
        ORDER BY ABS(weight) DESC
        LIMIT 5
        """,
        (wallet,),
    )
    reasons = []
    try:
        for r in cur.fetchall():
            code = (r[0] or "").strip()
            wt = int(r[1] or 0)
            conf = float(r[2] or 0) if len(r) > 2 else 0.0
            if code:
                reasons.append({"code": code, "weight": wt, "confidence": round(conf, 2)})
    except Exception:
        cur.execute(
            """
            SELECT reason_code, weight FROM wallet_reasons
            WHERE wallet = ? AND reason_code IS NOT NULL
            ORDER BY ABS(weight) DESC
            LIMIT 5
            """,
            (wallet,),
        )
        for r in cur.fetchall():
            code = (r[0] or "").strip()
            wt = int(r[1] or 0)
            if code:
                reasons.append({"code": code, "weight": wt, "confidence": 0.0})

    cluster_id = None
    cluster_size = 0
    for table in ["wallet_cluster_members", "wallet_graph_clusters", "wallet_clusters"]:
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not cur.fetchone():
                continue
            cur.execute(f"SELECT cluster_id FROM {table} WHERE wallet = ? LIMIT 1", (wallet,))
            r = cur.fetchone()
            if r:
                cluster_id = int(r[0])
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE cluster_id = ?", (cluster_id,))
                cluster_size = cur.fetchone()[0] or 0
                break
        except Exception:
            continue

    cur.execute("PRAGMA table_info(transactions)")
    tx_cols = {row[1] for row in cur.fetchall()}
    if "from_wallet" in tx_cols and "to_wallet" in tx_cols:
        cur.execute(
            """
            SELECT from_wallet, to_wallet, amount, timestamp
            FROM transactions
            WHERE from_wallet = ? OR to_wallet = ?
            ORDER BY timestamp DESC
            LIMIT 10
            """,
            (wallet, wallet),
        )
    else:
        cur.execute(
            """
            SELECT sender AS from_wallet, receiver AS to_wallet,
                   amount_lamports / 1e9 AS amount, timestamp
            FROM transactions
            WHERE sender = ? OR receiver = ?
            ORDER BY timestamp DESC
            LIMIT 10
            """,
            (wallet, wallet),
        )
    txs = []
    for r in cur.fetchall():
        frm = (r[0] or "").strip()
        to = (r[1] or "").strip()
        amt = float(r[2] or 0)
        ts = r[3]
        dt = datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M") if ts else "-"
        txs.append({"from": frm[:16] + "...", "to": to[:16] + "...", "amount": round(amt, 4), "time": dt})

    history = []
    for tbl, ts_col in [("wallet_score_history", "timestamp"), ("wallet_history", "snapshot_at")]:
        try:
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (tbl,))
            if not cur.fetchone():
                continue
            cur.execute(
                f"SELECT {ts_col}, score FROM {tbl} WHERE wallet = ? ORDER BY {ts_col} DESC LIMIT 10",
                (wallet,),
            )
            for r in cur.fetchall():
                ts_val = r[0]
                sc = round(float(r[1] or 50), 2)
                dt = datetime.fromtimestamp(ts_val).strftime("%Y-%m-%d %H:%M") if ts_val else "-"
                history.append({"time": dt, "score": sc})
            break
        except Exception:
            continue

    conn.close()

    out = Path(output_path) if output_path else _REPORTS_DIR / f"{wallet[:48].replace('/', '_')}.pdf"
    out.parent.mkdir(parents=True, exist_ok=True)

    doc = SimpleDocTemplate(str(out), pagesize=A4, rightMargin=inch, leftMargin=inch, topMargin=inch, bottomMargin=inch)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph("BlockID Wallet Investigation Report", styles["Title"]))
    story.append(Spacer(1, 0.3 * inch))

    story.append(Paragraph("Wallet Info", styles["Heading2"]))
    story.append(Paragraph(f"<b>Wallet:</b> {wallet}", styles["Normal"]))
    story.append(Paragraph(f"<b>Trust Score:</b> {score} | <b>Risk Level:</b> {risk}", styles["Normal"]))
    story.append(Spacer(1, 0.2 * inch))

    story.append(Paragraph("Top Risk Reasons", styles["Heading2"]))
    if reasons:
        reason_data = [["Code", "Weight", "Confidence"]] + [
            [r["code"], str(r["weight"]), str(r["confidence"])] for r in reasons
        ]
        t1 = Table(reason_data)
        t1.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(t1)
    else:
        story.append(Paragraph("No reason data available.", styles["Normal"]))
    story.append(Spacer(1, 0.2 * inch))

    story.append(Paragraph("Cluster Analysis", styles["Heading2"]))
    if cluster_id is not None:
        story.append(Paragraph(f"<b>Cluster ID:</b> {cluster_id} | <b>Size:</b> {cluster_size} wallets", styles["Normal"]))
    else:
        story.append(Paragraph("Wallet not in any cluster.", styles["Normal"]))
    story.append(Spacer(1, 0.2 * inch))

    story.append(Paragraph("Recent Transactions (last 10)", styles["Heading2"]))
    if txs:
        tx_data = [["From", "To", "Amount", "Time"]] + [
            [r["from"], r["to"], str(r["amount"]), r["time"]] for r in txs
        ]
        t2 = Table(tx_data)
        t2.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(t2)
    else:
        story.append(Paragraph("No transaction data available.", styles["Normal"]))
    story.append(Spacer(1, 0.2 * inch))

    story.append(Paragraph("Score History (last 10 snapshots)", styles["Heading2"]))
    if history:
        hist_data = [["Time", "Score"]] + [[r["time"], str(r["score"])] for r in history]
        t3 = Table(hist_data)
        t3.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.grey),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
        ]))
        story.append(t3)
    else:
        story.append(Paragraph("No score history available.", styles["Normal"]))

    story.append(Spacer(1, 0.5 * inch))
    story.append(Paragraph("Generated by BlockID Trust Oracle", styles["Normal"]))

    doc.build(story)
    meta = {"score": score, "cluster": cluster_id, "cluster_size": cluster_size, "tx_count": len(txs)}
    return out, meta


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Generate wallet investigation PDF report.")
    ap.add_argument("wallet", help="Wallet address")
    ap.add_argument("-o", "--output", default=None, help="Output PDF path")
    args = ap.parse_args()

    path, meta = generate_wallet_report(args.wallet, args.output)
    if not path:
        print("[report] Wallet not found")
        return 1
    s = meta.get("score", "?")
    c = meta.get("cluster", "?")
    tx = meta.get("tx_count", 0)
    print(f"[report] wallet={args.wallet[:16]}... score={s} cluster={c} tx={tx} saved={path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
