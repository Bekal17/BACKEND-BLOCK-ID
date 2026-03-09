"""
Explainable reason generator for BlockID.

Converts reason_codes into human-readable explanations. Rule-based, no ML.
"""
from __future__ import annotations

import asyncio

from backend_blockid.ai_engine.reason_templates import DEFAULT_LANG, get_template

NO_RISK_CODE = "NO_RISK_DETECTED"


async def generate_explanation_async(
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
    from backend_blockid.database.pg_connection import get_conn, release_conn

    own_conn = _conn is None
    conn = _conn if _conn else await get_conn()

    try:
        try:
            rows = await conn.fetch(
                """
                SELECT reason_code, weight, confidence_score
                FROM wallet_reasons
                WHERE wallet = $1 AND reason_code IS NOT NULL
                """,
                wallet.strip(),
            )
        except Exception:
            rows = await conn.fetch(
                """
                SELECT reason_code, weight
                FROM wallet_reasons
                WHERE wallet = $1 AND reason_code IS NOT NULL
                """,
                wallet.strip(),
            )
            rows = [{"reason_code": r["reason_code"], "weight": r["weight"], "confidence_score": 0.8} for r in rows]

        ts_row = await conn.fetchrow(
            "SELECT score, risk_level FROM trust_scores WHERE wallet = $1 LIMIT 1",
            wallet.strip(),
        )
        score = round(float(ts_row["score"] or 50), 2) if ts_row else 50.0
        risk = str(ts_row["risk_level"] or "1") if ts_row else "1"

        reasons: list[dict] = []
        for r in rows:
            code = (r["reason_code"] or "").strip()
            weight = int(r["weight"] or 0)
            conf = float(r["confidence_score"] or 0)
            reasons.append({"code": code, "weight": weight, "confidence": conf})
    finally:
        if own_conn:
            await release_conn(conn)

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


def generate_explanation(
    wallet: str,
    lang: str = DEFAULT_LANG,
    *,
    _conn=None,
) -> dict:
    """Sync wrapper for generate_explanation_async."""
    return asyncio.get_event_loop().run_until_complete(
        generate_explanation_async(wallet, lang, _conn=_conn)
    )


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
