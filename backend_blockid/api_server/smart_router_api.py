"""
BlockID Smart Router API
- Resolve @handle → wallet + profile + trust score
- Quote: estimate fees, auto-swap via Jupiter if needed
- Prepare unsigned transaction for frontend to sign via Phantom
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
import time

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter(prefix="/router", tags=["Smart Router"])

# ── Constants ──
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SUPPORTED_TOKENS = {
    "SOL": {"mint": SOL_MINT, "decimals": 9, "symbol": "SOL"},
    "USDC": {"mint": USDC_MINT, "decimals": 6, "symbol": "USDC"},
}
JUPITER_QUOTE_URL = "https://quote-api.jup.ag/v6/quote"
JUPITER_SWAP_URL = "https://quote-api.jup.ag/v6/swap"

BADGE_LABELS = {
    "NO_SCAM_HISTORY": "Clean Record",
    "CLEAN_HISTORY": "Clean History",
    "DEX_TRADER": "DEX Trader",
    "DEX_TRADER_10_PLUS": "Active Trader",
    "DEX_TRADER_50_PLUS": "Pro Trader",
    "NFT_COLLECTOR": "NFT Collector",
    "NFT_10_PLUS": "NFT Enthusiast",
    "LONG_HISTORY": "OG Wallet",
    "MULTI_YEAR_ACTIVITY": "Multi-Year Active",
    "AGE_3Y": "3Y Veteran",
    "AGE_5Y": "5Y Legend",
    "LOW_RISK_CLUSTER": "Safe Network",
    "FAR_FROM_SCAM_CLUSTER": "Scam-Free Zone",
    "DAO_MEMBER": "DAO Member",
    "VERIFIED_WALLET_LINK": "Multi-Wallet",
    "MULTI_WALLET_IDENTITY": "Unified Identity",
    "LOW_ACTIVITY": "Early Explorer",
    "NO_RISK_DETECTED": "No Risk Detected",
}


# ── Models ──
class QuoteRequest(BaseModel):
    sender_wallet: str
    recipient_handle: str
    amount: float
    input_token: str  # "SOL" or "USDC"
    output_token: str  # "SOL" or "USDC"


class SwapRequest(BaseModel):
    sender_wallet: str
    recipient_wallet: str
    amount: float
    input_token: str
    output_token: str
    slippage_bps: int = 50  # 0.5% default


# ── 1. Resolve @handle ──
@router.get("/resolve/{handle}")
async def resolve_handle(handle: str):
    """
    Resolve @handle to wallet address + full profile preview.
    This is the core of Smart Router — before sending money,
    user sees who they're sending to.
    """
    handle = handle.strip().lower().lstrip("@")
    if not handle or len(handle) < 2:
        raise HTTPException(status_code=400, detail="Handle too short")

    conn = await get_conn()
    try:
        row = await conn.fetchrow(
            "SELECT owner_wallet, handle FROM handle_registry "
            "WHERE LOWER(handle) = $1 AND status = 'ACTIVE' LIMIT 1",
            handle,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Handle not found")

        wallet = row["owner_wallet"]

        ts = await conn.fetchrow(
            "SELECT score AS trust_score, risk_level FROM trust_scores WHERE wallet = $1",
            wallet,
        )

        reason_rows = await conn.fetch(
            "SELECT reason_code FROM wallet_reasons WHERE wallet = $1 AND weight > 0",
            wallet,
        )
        badges_raw = [r["reason_code"] for r in reason_rows] if reason_rows else []

        profile_row = await conn.fetchrow(
            "SELECT displayed_badges FROM social_profiles WHERE wallet = $1",
            wallet,
        )
        displayed = []
        if profile_row and profile_row["displayed_badges"]:
            displayed = list(profile_row["displayed_badges"])

        final_badges = displayed if displayed else badges_raw[:5]
        badge_labels = [BADGE_LABELS.get(b, b) for b in final_badges]

        avatar_row = await conn.fetchrow(
            """SELECT avatar_type, avatar_url, avatar_nft_mint, avatar_nft_name,
                      display_name, display_name_source, bio
               FROM social_profiles WHERE wallet = $1""",
            wallet,
        )

        sub = await conn.fetchrow(
            """SELECT COALESCE(tier, plan, 'free') AS plan FROM subscriptions
               WHERE user_id = $1 AND status = 'active'
               ORDER BY created_at DESC NULLS LAST LIMIT 1""",
            wallet,
        )

        nft = await conn.fetchrow(
            "SELECT mint_address FROM identity_nft WHERE wallet = $1 "
            "AND (mint_status = 'MINTED' OR mint_address IS NOT NULL)",
            wallet,
        )

        return {
            "found": True,
            "handle": row["handle"],
            "wallet": wallet,
            "trust_score": float(ts["trust_score"]) if ts and ts["trust_score"] is not None else None,
            "risk_level": ts["risk_level"] if ts else None,
            "badges": badge_labels,
            "badges_raw": final_badges,
            "plan": sub["plan"] if sub else "free",
            "display_name": avatar_row["display_name"] if avatar_row else None,
            "bio": avatar_row["bio"] if avatar_row else None,
            "avatar_url": avatar_row["avatar_url"] if avatar_row else None,
            "avatar_type": avatar_row["avatar_type"] if avatar_row else None,
            "has_identity_nft": nft is not None,
            "risk_warning": (ts["risk_level"] in ("HIGH", "CRITICAL")) if ts and ts["risk_level"] else False,
        }

    finally:
        await release_conn(conn)


# ── 2. Quote (fee estimation + auto-swap) ──
@router.post("/quote")
async def get_quote(req: QuoteRequest):
    """
    Get transfer quote including fees.
    If input_token != output_token, get Jupiter swap quote.
    """
    if req.input_token not in SUPPORTED_TOKENS:
        raise HTTPException(status_code=400, detail=f"Unsupported input token: {req.input_token}")
    if req.output_token not in SUPPORTED_TOKENS:
        raise HTTPException(status_code=400, detail=f"Unsupported output token: {req.output_token}")
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")

    conn = await get_conn()
    try:
        handle = req.recipient_handle.strip().lower().lstrip("@")
        row = await conn.fetchrow(
            "SELECT owner_wallet FROM handle_registry "
            "WHERE LOWER(handle) = $1 AND status = 'ACTIVE' LIMIT 1",
            handle,
        )
        if not row:
            raise HTTPException(status_code=404, detail="Handle not found")
        recipient_wallet = row["owner_wallet"]
    finally:
        await release_conn(conn)

    input_info = SUPPORTED_TOKENS[req.input_token]
    output_info = SUPPORTED_TOKENS[req.output_token]

    needs_swap = req.input_token != req.output_token
    quote_data = None
    output_amount = req.amount
    price_impact = 0.0
    swap_fee = 0.0

    if needs_swap:
        amount_raw = int(req.amount * (10 ** input_info["decimals"]))
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    JUPITER_QUOTE_URL,
                    params={
                        "inputMint": input_info["mint"],
                        "outputMint": output_info["mint"],
                        "amount": str(amount_raw),
                        "slippageBps": "50",
                    },
                )
                if resp.status_code != 200:
                    raise HTTPException(status_code=502, detail="Jupiter quote failed")
                quote_data = resp.json()

            out_raw = int(quote_data.get("outAmount", 0))
            output_amount = out_raw / (10 ** output_info["decimals"])
            price_impact = float(quote_data.get("priceImpactPct", 0))
            swap_fee = req.amount * 0.003

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Jupiter quote timeout")

    network_fee_sol = 0.000005

    return {
        "sender_wallet": req.sender_wallet,
        "recipient_wallet": recipient_wallet,
        "recipient_handle": handle,
        "input_token": req.input_token,
        "input_amount": req.amount,
        "output_token": req.output_token,
        "output_amount": round(output_amount, 6),
        "needs_swap": needs_swap,
        "price_impact_pct": round(price_impact, 4),
        "swap_fee_estimate": round(swap_fee, 6),
        "network_fee_sol": network_fee_sol,
        "jupiter_quote": quote_data if needs_swap else None,
        "timestamp": int(time.time()),
    }


# ── 3. Prepare swap transaction (unsigned, for Phantom to sign) ──
@router.post("/swap")
async def prepare_swap(req: SwapRequest):
    """
    Get unsigned swap transaction from Jupiter.
    Frontend will sign this with Phantom and broadcast.
    Only needed when input_token != output_token.
    """
    if req.input_token == req.output_token:
        raise HTTPException(
            status_code=400,
            detail="Same token — use direct transfer, no swap needed",
        )

    input_info = SUPPORTED_TOKENS.get(req.input_token)
    output_info = SUPPORTED_TOKENS.get(req.output_token)
    if not input_info or not output_info:
        raise HTTPException(status_code=400, detail="Unsupported token")

    amount_raw = int(req.amount * (10 ** input_info["decimals"]))

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            quote_resp = await client.get(
                JUPITER_QUOTE_URL,
                params={
                    "inputMint": input_info["mint"],
                    "outputMint": output_info["mint"],
                    "amount": str(amount_raw),
                    "slippageBps": str(req.slippage_bps),
                },
            )
            if quote_resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Jupiter quote failed")

            quote_data = quote_resp.json()

            swap_resp = await client.post(
                JUPITER_SWAP_URL,
                json={
                    "quoteResponse": quote_data,
                    "userPublicKey": req.sender_wallet,
                    "destinationTokenAccount": req.recipient_wallet,
                    "wrapAndUnwrapSol": True,
                },
            )
            if swap_resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Jupiter swap tx failed")

            swap_data = swap_resp.json()

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Jupiter timeout")

    return {
        "swap_transaction": swap_data.get("swapTransaction"),
        "input_token": req.input_token,
        "input_amount": req.amount,
        "output_token": req.output_token,
        "output_amount": int(quote_data.get("outAmount", 0)) / (10 ** output_info["decimals"]),
        "recipient_wallet": req.recipient_wallet,
    }


# ── 4. Supported tokens list ──
@router.get("/tokens")
async def get_supported_tokens():
    """Return list of supported tokens for Smart Router."""
    return {
        "tokens": [
            {"symbol": "SOL", "name": "Solana", "mint": SOL_MINT, "decimals": 9, "icon": "◎"},
            {"symbol": "USDC", "name": "USD Coin", "mint": USDC_MINT, "decimals": 6, "icon": "$"},
        ]
    }
