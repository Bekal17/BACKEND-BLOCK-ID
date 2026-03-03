"""
Trust badge generator for BlockID.

Converts trust_scores and wallet_reasons into badge info for frontend.
"""
from __future__ import annotations

from backend_blockid.ai_engine.badge_rules import (
    RISK_LEVEL_TEXT,
    get_badge_for_score,
)
from backend_blockid.ai_engine.reason_templates import get_template


def generate_badge(wallet: str, *, _conn=None) -> dict:
    """
    Load score/risk from trust_scores, pick badge, load top reasons.
    Does NOT recompute score. Uses trust_scores table only.
    """
    from backend_blockid.database.connection import get_connection

    conn = _conn or get_connection()
    cur = conn.cursor()

    cur.execute(
        "SELECT score, risk_level FROM trust_scores WHERE wallet = ? LIMIT 1",
        (wallet.strip(),),
    )
    row = cur.fetchone()
    if not row:
        score = 50.0
        risk = "1"
    else:
        score = round(float((row["score"] if hasattr(row, "keys") else row[0]) or 50), 2)
        risk = str((row["risk_level"] if hasattr(row, "keys") else row[1]) or "1")

    badge = get_badge_for_score(score)
    risk_level_text = RISK_LEVEL_TEXT.get(risk, "Unknown")

    cur.execute(
        """
        SELECT reason_code, weight
        FROM wallet_reasons
        WHERE wallet = ? AND reason_code IS NOT NULL
        ORDER BY ABS(weight) DESC
        LIMIT 3
        """,
        (wallet.strip(),),
    )
    reason_rows = cur.fetchall()
    if not _conn:
        conn.close()

    top_reasons: list[str] = []
    reason_texts: list[str] = []
    for r in reason_rows:
        code = (r["reason_code"] if hasattr(r, "keys") else r[0] or "").strip()
        if code and code != "NO_RISK_DETECTED":
            top_reasons.append(code)
            placeholders = {"distance": "1-3"} if code == "SCAM_DISTANCE" else {}
            text = get_template(code, **placeholders)
            reason_texts.append(text)

    def _shorten(t: str) -> str:
        return t.replace("This wallet ", "").replace("This wallet", "").strip()

    if badge["name"] == "Trusted" and not reason_texts:
        message = "No significant risk detected."
        summary = "No significant risk detected."
    elif reason_texts:
        message = reason_texts[0]
        parts = [_shorten(t) for t in reason_texts[:2]]
        summary = f"{badge['name']} — " + " and ".join(parts)
    else:
        message = f"Score {score}. Risk level: {risk_level_text}."
        summary = f"Score {score}. Risk level: {risk_level_text}."

    return {
        "wallet": wallet,
        "score": score,
        "risk": risk,
        "risk_level_text": risk_level_text,
        "badge": badge["name"],
        "color": badge["color"],
        "top_reasons": top_reasons,
        "message": message,
        "summary": summary,
    }


def generate_svg_badge(score: float, badge_name: str, color: str, size: str = "medium") -> str:
    """Return simple SVG badge. Frontend can render directly."""
    w = 160 if size == "large" else 120 if size == "medium" else 100
    h = 36 if size == "large" else 28 if size == "medium" else 24
    fs = 12 if size == "large" else 10 if size == "medium" else 9
    colors = {"green": "#22c55e", "yellow": "#eab308", "orange": "#f97316", "red": "#ef4444"}
    fill = colors.get(color, "#6b7280")
    check = "✔" if badge_name == "Trusted" else "●"
    label = f"{badge_name} {check} {int(round(score))}"
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
        f'<rect width="{w}" height="{h}" rx="6" fill="{fill}" opacity="0.2"/>'
        f'<rect width="{w}" height="{h}" rx="6" fill="none" stroke="{fill}" stroke-width="1.5"/>'
        f'<text x="{w//2}" y="{h//2 + 4}" text-anchor="middle" font-family="sans-serif" font-size="{fs}" font-weight="600" fill="{fill}">{label}</text>'
        "</svg>"
    )
