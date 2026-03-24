"""
Daemon-AI client — wallet analysis explanation layer.
OpenAI-compatible API via Daemon Protocol.
Base URL: https://daemon-ai-production.up.railway.app
"""
from __future__ import annotations
import os
import httpx
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

DAEMON_AI_BASE = "https://daemon-ai-production.up.railway.app"
DAEMON_AI_KEY = (os.getenv("DAEMON_AI_KEY") or "").strip()
DAEMON_AI_TIMEOUT = float(os.getenv("DAEMON_AI_TIMEOUT", "15"))
DAEMON_AI_MODEL = os.getenv("DAEMON_AI_MODEL", "arcee-ai/trinity-large-preview:free")


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {DAEMON_AI_KEY}",
        "Content-Type": "application/json",
    }


async def explain_wallet_risk(
    wallet: str,
    trust_score: float,
    risk_tier: str,
    reasons: list[str],
    cyclops_risk_level: str | None = None,
    cyclops_risk_score: float | None = None,
    is_sanctioned: bool = False,
) -> str | None:
    """
    Generate natural language explanation of wallet risk using Daemon-AI.
    Returns explanation string or None if failed.
    """
    if not DAEMON_AI_KEY:
        logger.debug("daemon_ai_skip", reason="no API key")
        return None

    # Build context
    reasons_str = ", ".join(reasons[:8]) if reasons else "No specific reasons detected"
    cyclops_str = ""
    if cyclops_risk_level:
        cyclops_str = f"\n- Cyclops Risk Level: {cyclops_risk_level} ({cyclops_risk_score:.1f}/100)"

    sanctions_str = "\n- SANCTIONED by OFAC" if is_sanctioned else "\n- Sanctions: Clean (OFAC)"

    prompt = f"""Analyze this Solana wallet and provide a brief 2-3 sentence risk assessment.

Wallet Data:
- BlockID Trust Score: {trust_score:.0f}/100
- Risk Tier: {risk_tier}{cyclops_str}{sanctions_str}
- Behavioral Signals: {reasons_str}

Write a concise, professional risk assessment. Focus on what the data shows about this wallet's behavior and risk profile. Do not mention wallet address. Be direct and factual."""

    try:
        async with httpx.AsyncClient(timeout=DAEMON_AI_TIMEOUT) as client:
            res = await client.post(
                f"{DAEMON_AI_BASE}/v1/chat/completions",
                headers=_headers(),
                json={
                    "model": DAEMON_AI_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a blockchain risk analyst. Provide concise, accurate wallet risk assessments based on on-chain data."
                        },
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "stream": False,
                    "max_tokens": 150,
                    "temperature": 0.3,
                },
            )
            if res.status_code == 200:
                data = res.json()
                content = (
                    data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    or ""
                ).strip()
                if content:
                    logger.info(
                        "daemon_ai_explanation_done",
                        wallet=wallet[:16],
                        chars=len(content),
                    )
                    return content
            logger.warning(
                "daemon_ai_non_200",
                wallet=wallet[:16],
                status=res.status_code,
            )
    except httpx.TimeoutException:
        logger.warning("daemon_ai_timeout", wallet=wallet[:16])
    except Exception as e:
        logger.warning("daemon_ai_error", wallet=wallet[:16], error=str(e))
    return None
