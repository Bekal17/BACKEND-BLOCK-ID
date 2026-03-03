"""
Explainable reason generator for BlockID.

Converts reason_codes into human-readable explanations. Rule-based, no ML.
"""
from __future__ import annotations

from backend_blockid.ai_engine.reason_templates import DEFAULT_LANG, get_template

NO_RISK_CODE = "NO_RISK_DETECTED"


def generate_explanation(
    wallet: str,
    lang: str = DEFAULT_LANG,
    *,
    _conn=None,
) -> dict:
    """
    Load reasons from DB, convert to template text, return structured explanation.

    Returns:
        wallet, score, risk, explanations: [{code, text, confidence}]
    """
    from backend_blockid.database.connection import get_connection

    conn = _conn or get_connection()
    cur = conn.cursor()

    try:
        cur.execute(
            """
            SELECT reason_code, weight, confidence_score
            FROM wallet_reasons
            WHERE wallet = ? AND reason_code IS NOT NULL
            """,
            (wallet.strip(),),
        )
        rows = cur.fetchall()
    except Exception:
        cur.execute(
            """
            SELECT reason_code, weight
            FROM wallet_reasons
            WHERE wallet = ? AND reason_code IS NOT NULL
            """,
            (wallet.strip(),),
        )
        rows = [(*r, 0.8) for r in cur.fetchall()]

    cur.execute(
        "SELECT score, risk_level FROM trust_scores WHERE wallet = ? LIMIT 1",
        (wallet.strip(),),
    )
    ts_row = cur.fetchone()
    score = round(float((ts_row["score"] if hasattr(ts_row, "keys") else ts_row[0]) or 50), 2) if ts_row else 50.0
    risk = str((ts_row["risk_level"] if hasattr(ts_row, "keys") else ts_row[1]) or "1") if ts_row else "1"

    reasons: list[dict] = []
    for r in rows:
        code = (r["reason_code"] if hasattr(r, "keys") else r[0] or "").strip()
        weight = int(r["weight"] if hasattr(r, "keys") else r[1] or 0)
        conf = float(r["confidence_score"] if hasattr(r, "keys") else r[2] or 0)
        reasons.append({"code": code, "weight": weight, "confidence": conf})

    if not _conn:
        conn.close()

    if not reasons:
        reasons = [{"code": NO_RISK_CODE, "weight": 0, "confidence": 1.0}]

    if len(reasons) > 1:
        reasons = [r for r in reasons if r["code"] != NO_RISK_CODE]

    reasons.sort(key=lambda x: abs(x["weight"]), reverse=True)

    explanations: list[dict] = []
    for r in reasons:
        code = r["code"]
        placeholders = {"distance": "1-3"} if code == "SCAM_DISTANCE" else {}
        text = get_template(code, lang, **placeholders)
        explanations.append({
            "code": code,
            "text": text,
            "confidence": round(r["confidence"], 2),
        })

    return {
        "wallet": wallet,
        "score": score,
        "risk": risk,
        "explanations": explanations,
    }


def generate_summary(explanations: list[dict], top_n: int = 2) -> str:
    """Combine top N explanation texts into one sentence."""
    if not explanations:
        return "No risk indicators available."
    texts = []
    for e in explanations[:top_n]:
        t = e["text"]
        if t.startswith("This wallet "):
            t = "Wallet " + t[12:]
        texts.append(t)
    if len(texts) == 1:
        return texts[0]
    second = texts[1]
    if second[0].isupper():
        second = second[0].lower() + second[1:]
    return texts[0] + " and " + second
