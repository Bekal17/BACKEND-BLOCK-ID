"""
Sage mention processor for BlockID social feed.
Detects @sage mentions and auto-replies based on intent or cashtag.
"""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend_blockid.blockid_logging import get_logger
from backend_blockid.database.pg_connection import get_conn, release_conn


logger = get_logger(__name__)
router = APIRouter(prefix="/sage", tags=["sage"])

SAGE_HANDLE = "sage"
SAGE_WALLET_FALLBACK = os.getenv("SAGE_WALLET", "")
INTERNAL_BASE_URL = os.getenv("INTERNAL_API_BASE", "http://localhost:8000")
BACKEND_URL = os.environ.get(
    "BACKEND_URL",
    "https://blockid-backend-production.up.railway.app",
)


class SageProcessRequest(BaseModel):
    post_id: int
    content: str
    author_wallet: str
    author_handle: str = ""


def _clean_handle(handle: str | None) -> str:
    return (handle or "").strip().lstrip("@").lower()


def _extract_ticker(content: str) -> str | None:
    m = re.search(r"\$([A-Z]{2,10})", (content or "").upper())
    return m.group(1) if m else None


def _format_market_cap(value: Any) -> str:
    try:
        num = float(value)
    except (TypeError, ValueError):
        return "N/A"
    if num >= 1_000_000_000:
        return f"${num / 1_000_000_000:.1f}B"
    if num >= 1_000_000:
        return f"${num / 1_000_000:.1f}M"
    if num >= 1_000:
        return f"${num / 1_000:.1f}K"
    return f"${num:,.0f}"


async def _get_sage_identity() -> tuple[str, str]:
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT wallet, handle FROM social_profiles WHERE LOWER(handle) = $1 LIMIT 1",
            SAGE_HANDLE,
        )
        if row and row.get("wallet"):
            return row["wallet"], row.get("handle") or SAGE_HANDLE
        if SAGE_WALLET_FALLBACK:
            return SAGE_WALLET_FALLBACK, SAGE_HANDLE
        raise HTTPException(status_code=500, detail="SAGE_WALLET not configured")
    finally:
        await release_conn(conn)


async def _fetch_intent(author_wallet: str, content: str) -> dict[str, Any]:
    url = f"{BACKEND_URL.rstrip('/')}/router/parse"
    payload = {"wallet": author_wallet, "input": content}
    async with httpx.AsyncClient(timeout=15.0) as client:
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, dict) else {}


async def _get_handle_trust(handle: str) -> str:
    handle_clean = handle.lstrip("@").lower()
    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            """
            SELECT ts.final_score
            FROM handle_registry hr
            JOIN trust_scores ts ON ts.wallet = hr.owner_wallet
            WHERE LOWER(hr.handle) = $1
            LIMIT 1
            """,
            handle_clean,
        )
        if row and row["final_score"]:
            return str(round(row["final_score"]))
        return "N/A"
    finally:
        await release_conn(conn)


async def _build_intent_reply(intent_data: dict[str, Any]) -> str | None:
    intent = str(intent_data.get("intent") or "").lower()
    if intent not in {"send", "swap"}:
        return None

    amount = intent_data.get("amount")
    token = str(intent_data.get("token") or "").upper() or "SOL"
    output_token = str(intent_data.get("output_token") or "").upper()
    handle_clean = _clean_handle(str(intent_data.get("handle") or ""))

    params: dict[str, str] = {
        "intent": intent,
        "amount": str(amount if amount is not None else ""),
        "token": token,
        "to": handle_clean,
    }
    if intent == "swap" and output_token:
        params["output_token"] = output_token

    url = f"https://app.blockidscore.fun/router?{urlencode(params)}"

    if intent == "send":
        trust_score = await _get_handle_trust(handle_clean)
        recipient_handle = handle_clean or "unknown"
        return (
            "✓ Ready to execute\n\n"
            f"Send: {amount} {token}"
            f" -> {recipient_handle} (Trust {trust_score})"
            f"\n\nTap to confirm:\n{url}\n\n"
            "Powered by BlockID Smart Router"
        )

    return (
        "✓ Ready to execute\n\n"
        f"Swap: {amount} {token}"
        f" -> {output_token or '?'}"
        f"\n\nTap to confirm:\n{url}\n\n"
        "Powered by BlockID Smart Router"
    )


async def _build_ticker_reply(content: str) -> str | None:
    ticker = _extract_ticker(content)
    if not ticker:
        return None

    jup_url = f"https://api.jup.ag/tokens/v2/search?query={ticker}"
    stats_url = f"{INTERNAL_BASE_URL.rstrip('/')}/social/cashtag/{ticker}/stats"

    token_row: dict[str, Any] = {}
    stats_row: dict[str, Any] = {}

    async with httpx.AsyncClient(timeout=20.0) as client:
        try:
            jup_resp = await client.get(jup_url)
            jup_resp.raise_for_status()
            jup_data = jup_resp.json()
            if isinstance(jup_data, dict):
                if isinstance(jup_data.get("data"), list) and jup_data["data"]:
                    token_row = jup_data["data"][0]
                elif isinstance(jup_data.get("tokens"), list) and jup_data["tokens"]:
                    token_row = jup_data["tokens"][0]
            elif isinstance(jup_data, list) and jup_data:
                token_row = jup_data[0]
        except Exception as e:
            logger.warning("sage_jupiter_fetch_failed", ticker=ticker, error=str(e))

        try:
            stats_resp = await client.get(stats_url)
            stats_resp.raise_for_status()
            stats_data = stats_resp.json()
            if isinstance(stats_data, dict):
                stats_row = stats_data
        except Exception as e:
            logger.warning("sage_cashtag_stats_failed", ticker=ticker, error=str(e))

    price = token_row.get("usdPrice") or token_row.get("price") or token_row.get("priceUsd")
    mcap = token_row.get("fdv") or token_row.get("marketCap") or token_row.get("market_cap")
    change = (
        token_row.get("priceChange24h")
        or token_row.get("price_change_24h")
        or token_row.get("dailyChange")
        or 0
    )
    is_verified = bool(token_row.get("verified") or token_row.get("strict"))
    trusted_count = int(stats_row.get("trusted_wallet_count") or 0)

    price_text = f"${float(price):,.6f}".rstrip("0").rstrip(".") if price is not None else "N/A"
    try:
        change_text = f"{float(change):.2f}%"
    except (TypeError, ValueError):
        change_text = "N/A"

    return (
        f"${ticker} on-chain stats:\n\n"
        f"Price: {price_text}\n"
        f"Market Cap: {_format_market_cap(mcap)}\n"
        f"24h Change: {change_text}\n"
        f"Verified: {'Yes' if is_verified else 'No'}\n\n"
        "BlockID Trust Signal:\n"
        f"{trusted_count} wallet(s) with Trust Score >50 discussing ${ticker}\n\n"
        "app.blockidscore.fun"
    )


def _build_help_reply() -> str:
    return (
        "Hi! I'm Sage, BlockID's on-chain AI agent.\n\n"
        "Here's what I can do:\n"
        "• @sage send 1 SOL to @handle\n"
        "• @sage swap 10 USDC to JUP\n"
        "• @sage $SOL stats\n\n"
        "Powered by BlockID"
    )


async def process_sage_mention(
    post_id: int,
    content: str,
    author_wallet: str,
    author_handle: str,
) -> bool:
    """Process one @sage mention and post an auto-reply. Returns True when replied."""
    conn = await get_conn()
    try:
        already = await conn.fetchrow(
            "SELECT id FROM social_posts WHERE parent_id = $1 AND handle = $2 LIMIT 1",
            post_id,
            SAGE_HANDLE,
        )
        if already:
            return False
    finally:
        await release_conn(conn)

    try:
        intent_data = await _fetch_intent(author_wallet, content)
    except Exception as e:
        logger.warning("sage_parse_failed", post_id=post_id, error=str(e))
        intent_data = {"intent": "unknown", "handle": author_handle}

    reply_text = await _build_intent_reply(intent_data)
    if not reply_text:
        reply_text = await _build_ticker_reply(content)
    if not reply_text:
        reply_text = _build_help_reply()

    sage_wallet, sage_handle = await _get_sage_identity()

    conn = await get_conn()
    try:
        await conn.execute(
            """
            INSERT INTO social_posts
              (wallet, handle, content, post_type, parent_id, trust_score, risk_level)
            VALUES
              ($1, $2, $3, 'PUBLIC', $4, 85, 'LOW')
            """,
            sage_wallet,
            sage_handle,
            reply_text,
            post_id,
        )
        await conn.execute(
            "UPDATE social_posts SET reply_count = reply_count + 1 WHERE id = $1",
            post_id,
        )
        return True
    finally:
        await release_conn(conn)


@router.post("/process")
async def process_sage(request: SageProcessRequest):
    """Internal endpoint to process @sage mentions."""
    replied = await process_sage_mention(
        post_id=request.post_id,
        content=request.content,
        author_wallet=request.author_wallet,
        author_handle=request.author_handle,
    )
    return {"success": True, "replied": replied}
