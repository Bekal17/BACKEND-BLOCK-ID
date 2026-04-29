"""
BlockID Smart Router API
- Resolve @handle → wallet + profile + trust score
- Quote: estimate fees, auto-swap via Jupiter if needed
- Prepare unsigned transaction for frontend to sign via Phantom
"""

from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import httpx
import json
import os
import time
import openai

from backend_blockid.database.pg_connection import get_conn, release_conn

router = APIRouter(prefix="/router", tags=["Smart Router"])

# ── Constants ──
SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SUPPORTED_TOKENS = {
    "SOL": {"mint": SOL_MINT, "decimals": 9, "symbol": "SOL"},
    "USDC": {"mint": USDC_MINT, "decimals": 6, "symbol": "USDC"},
}
JUPITER_BASE_URL = "https://api.jup.ag/swap/v2"
JUPITER_ORDER_URL = f"{JUPITER_BASE_URL}/order"
JUPITER_EXECUTE_URL = f"{JUPITER_BASE_URL}/execute"
JUPITER_API_KEY = os.getenv("JUPITER_API_KEY", "")
JUPITER_REFERRAL_ACCOUNT = "8SVPrMMD5kL7yxtE8Rk8h5DTK4AvWD5QSGNQnEH4fFaY"
JUPITER_REFERRAL_FEE_BPS = "95"

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

# ── OpenAI for natural language parsing ──
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
openai_client = openai.AsyncOpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

PARSE_SYSTEM_PROMPT = """You are BlockID Smart Router intent parser. Parse user input into a structured JSON action.

SUPPORTED INTENTS:
1. "send" — transfer SOL or USDC to a @handle
2. "check" — look up a @handle's profile/trust score
3. "balance" — check sender's own balance
4. "swap" — swap between SOL and USDC (no recipient)
5. "unknown" — cannot determine intent

RULES:
- Handle: extract @handle without the @ prefix. If no @ symbol, still detect handle-like words after "to", "ke", "for".
- Token: detect SOL, USDC, or dollar/dolar/usd (map to USDC). Default to SOL if ambiguous for send. For swap, detect both input and output token.
- Amount: extract numeric value. Support "half"→"HALF", "all"/"semua"/"max"→"MAX", "quarter"→"QUARTER".
- Language: support English and Bahasa Indonesia equally.
- If intent is "check" or "balance", amount and token can be null.
- If intent is "swap", there is no handle, but there must be input_token and output_token.
- For "send" intent: if user says dollar/dolar/usd, map token to USDC.

CASHTAG NOTATION:
Users may specify tokens using the $TICKER cashtag notation. When you see a $TICKER pattern:
- Strip the $ prefix — return just the ticker symbol in the "token" or "output_token" field
- Example: "$SOL" → token = "SOL"
- Example: "$USDC" → token = "USDC"
- The amount before a cashtag refers to USD value if the token is a stablecoin, or token quantity otherwise

Additional few-shot examples:
Input: "send $10 $USDC to @bee17"
Output: {"intent": "send", "handle": "@bee17", "handle_resolved": null, "amount": 10, "token": "USDC", "output_token": null, "confidence": 0.98, "raw_input": "send $10 $USDC to @bee17", "needs_more_info": false}

Input: "swap $50 $BONK ke $JUP"
Output: {"intent": "swap", "handle": null, "handle_resolved": null, "amount": 50, "token": "BONK", "output_token": "JUP", "confidence": 0.96, "raw_input": "swap $50 $BONK ke $JUP", "needs_more_info": false}

Input: "kirim 0.5 $SOL ke @ana"
Output: {"intent": "send", "handle": "@ana", "handle_resolved": null, "amount": 0.5, "token": "SOL", "output_token": null, "confidence": 0.97, "raw_input": "kirim 0.5 $SOL ke @ana", "needs_more_info": false}

RESPOND WITH ONLY valid JSON, no markdown, no explanation:
{
  "intent": "send" | "check" | "balance" | "swap" | "unknown",
  "handle": "string or null",
  "amount": number or "HALF" or "MAX" or "QUARTER" or null,
  "token": "SOL" or "USDC" or null,
  "output_token": "SOL" or "USDC" or null,
  "confidence": 0.0 to 1.0,
  "raw_input": "original user input"
}

EXAMPLES:
Input: "send 1 SOL to @blockid"
Output: {"intent":"send","handle":"blockid","amount":1,"token":"SOL","output_token":null,"confidence":0.95,"raw_input":"send 1 SOL to @blockid"}

Input: "kirim 100 USDC ke @bee121"
Output: {"intent":"send","handle":"bee121","amount":100,"token":"USDC","output_token":null,"confidence":0.95,"raw_input":"kirim 100 USDC ke @bee121"}

Input: "bayar @blockid 50 dolar"
Output: {"intent":"send","handle":"blockid","amount":50,"token":"USDC","output_token":null,"confidence":0.9,"raw_input":"bayar @blockid 50 dolar"}

Input: "send half my SOL to @blockid"
Output: {"intent":"send","handle":"blockid","amount":"HALF","token":"SOL","output_token":null,"confidence":0.9,"raw_input":"send half my SOL to @blockid"}

Input: "kirim semua USDC ke @bee121"
Output: {"intent":"send","handle":"bee121","amount":"MAX","token":"USDC","output_token":null,"confidence":0.9,"raw_input":"kirim semua USDC ke @bee121"}

Input: "siapa @blockid?"
Output: {"intent":"check","handle":"blockid","amount":null,"token":null,"output_token":null,"confidence":0.9,"raw_input":"siapa @blockid?"}

Input: "cek score @bee121"
Output: {"intent":"check","handle":"bee121","amount":null,"token":null,"output_token":null,"confidence":0.95,"raw_input":"cek score @bee121"}

Input: "berapa saldo aku?"
Output: {"intent":"balance","handle":null,"amount":null,"token":null,"output_token":null,"confidence":0.9,"raw_input":"berapa saldo aku?"}

Input: "my balance"
Output: {"intent":"balance","handle":null,"amount":null,"token":null,"output_token":null,"confidence":0.95,"raw_input":"my balance"}

Input: "tukar 1 SOL ke USDC"
Output: {"intent":"swap","handle":null,"amount":1,"token":"SOL","output_token":"USDC","confidence":0.95,"raw_input":"tukar 1 SOL ke USDC"}

Input: "swap 100 USDC to SOL"
Output: {"intent":"swap","handle":null,"amount":100,"token":"USDC","output_token":"SOL","confidence":0.95,"raw_input":"swap 100 USDC to SOL"}

Input: "hello"
Output: {"intent":"unknown","handle":null,"amount":null,"token":null,"output_token":null,"confidence":0.3,"raw_input":"hello"}"""


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


class SwapQuoteRequest(BaseModel):
    sender_wallet: str
    amount: float
    input_token: str  # e.g. "SOL"
    output_token: str  # e.g. "USDC"
    slippage_bps: int = 50  # 0.5% default


class ExecuteRequest(BaseModel):
    signed_transaction: str
    request_id: str


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
            raise HTTPException(status_code=404, detail="Handle, SNS or DNS not found")

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
    If input_token != output_token, get Jupiter Swap API v2 order (quote + tx in one call).
    Based on: https://dev.jup.ag/docs/swap/order-and-execute
    """
    if req.input_token not in SUPPORTED_TOKENS:
        raise HTTPException(status_code=400, detail=f"Unsupported input token: {req.input_token}")
    if req.output_token not in SUPPORTED_TOKENS:
        raise HTTPException(status_code=400, detail=f"Unsupported output token: {req.output_token}")
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")

    # Resolve handle → wallet
    conn = await get_conn()
    try:
        handle = req.recipient_handle.strip().lower().lstrip("@")
        row = await conn.fetchrow(
            "SELECT owner_wallet FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE' LIMIT 1",
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
    output_amount = req.amount
    price_impact = 0.0
    order_data = None

    if needs_swap:
        amount_raw = int(req.amount * (10 ** input_info["decimals"]))
        headers = {}
        if JUPITER_API_KEY:
            headers["x-api-key"] = JUPITER_API_KEY
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # GET /order returns quote + assembled transaction in one call
                # Without "taker", it returns quote only (no transaction)
                # We omit taker here for quote-only to save resources
                resp = await client.get(
                    JUPITER_ORDER_URL,
                    params={
                        "inputMint": input_info["mint"],
                        "outputMint": output_info["mint"],
                        "amount": str(amount_raw),
                        "slippageBps": "50",
                        "referralAccount": JUPITER_REFERRAL_ACCOUNT,
                        "referralFee": JUPITER_REFERRAL_FEE_BPS,
                    },
                    headers=headers,
                )
                if resp.status_code != 200:
                    error_detail = resp.text[:300] if resp.text else "Unknown error"
                    raise HTTPException(status_code=502, detail=f"Jupiter order failed: {error_detail}")
                order_data = resp.json()

            out_raw = int(order_data.get("outAmount", 0))
            output_amount = out_raw / (10 ** output_info["decimals"])
            price_impact = float(order_data.get("priceImpactPct", 0))

        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="Jupiter timeout")

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
        "network_fee_sol": network_fee_sol,
        "router": order_data.get("router") if order_data else None,
        "swap_api_version": "v2",
        "timestamp": int(time.time()),
    }


@router.post("/swap-quote")
async def swap_quote(req: SwapQuoteRequest):
    """
    Get a swap quote without handle resolution.
    For self-swap operations (token A → token B in same wallet).
    Calls Jupiter /order without taker (quote-only mode).
    """
    input_token = req.input_token.upper()
    output_token = req.output_token.upper()

    if input_token not in SUPPORTED_TOKENS:
        raise HTTPException(status_code=400, detail=f"Unsupported input token: {req.input_token}")
    if output_token not in SUPPORTED_TOKENS:
        raise HTTPException(status_code=400, detail=f"Unsupported output token: {req.output_token}")
    if req.amount <= 0:
        raise HTTPException(status_code=400, detail="Amount must be positive")
    if req.slippage_bps < 0:
        raise HTTPException(status_code=400, detail="slippage_bps must be non-negative")

    input_info = SUPPORTED_TOKENS[input_token]
    output_info = SUPPORTED_TOKENS[output_token]
    amount_raw = int(req.amount * (10 ** input_info["decimals"]))

    headers = {}
    if JUPITER_API_KEY:
        headers["x-api-key"] = JUPITER_API_KEY

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                JUPITER_ORDER_URL,
                params={
                    "inputMint": input_info["mint"],
                    "outputMint": output_info["mint"],
                    "amount": str(amount_raw),
                    "slippageBps": str(req.slippage_bps),
                    "referralAccount": JUPITER_REFERRAL_ACCOUNT,
                    "referralFee": JUPITER_REFERRAL_FEE_BPS,
                },
                headers=headers,
            )
            if resp.status_code != 200:
                error_detail = resp.text[:300] if resp.text else "Unknown error"
                raise HTTPException(status_code=502, detail=f"Jupiter order failed: {error_detail}")
            order_data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Jupiter timeout")

    out_raw = int(order_data.get("outAmount", 0))
    output_amount = out_raw / (10 ** output_info["decimals"])
    price_impact = float(order_data.get("priceImpactPct", 0))

    return {
        "input_amount": req.amount,
        "input_token": input_token,
        "output_amount": round(output_amount, 6),
        "output_token": output_token,
        "price_impact_pct": round(price_impact, 4),
        "router": order_data.get("router"),
        "slippage_bps": req.slippage_bps,
    }


# ── 3. Assembled swap tx (v2 /order) + managed execute ──
@router.post("/swap")
async def prepare_swap(req: SwapRequest):
    """
    Get assembled swap transaction from Jupiter Swap API v2 /order endpoint.
    This time we pass "taker" to get the full assembled transaction.
    Frontend signs with Phantom, then submits via /router/execute.
    Based on: https://dev.jup.ag/docs/swap/order-and-execute

    /order required params: inputMint, outputMint, amount, taker
    /order response: { transaction, requestId, outAmount, router, mode, feeBps, feeMint }
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

    recipient_wallet = (req.recipient_wallet or "").strip()
    is_self_swap = (not recipient_wallet) or (recipient_wallet == req.sender_wallet)
    taker_wallet = req.sender_wallet if is_self_swap else recipient_wallet
    recipient_wallet_effective = req.sender_wallet if is_self_swap else recipient_wallet

    amount_raw = int(req.amount * (10 ** input_info["decimals"]))
    headers = {}
    if JUPITER_API_KEY:
        headers["x-api-key"] = JUPITER_API_KEY

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # GET /order with taker = returns quote + assembled unsigned transaction
            order_resp = await client.get(
                JUPITER_ORDER_URL,
                params={
                    "inputMint": input_info["mint"],
                    "outputMint": output_info["mint"],
                    "amount": str(amount_raw),
                    "slippageBps": str(req.slippage_bps),
                    "taker": taker_wallet,
                    "referralAccount": JUPITER_REFERRAL_ACCOUNT,
                    "referralFee": JUPITER_REFERRAL_FEE_BPS,
                },
                headers=headers,
            )
            if order_resp.status_code != 200:
                error_detail = order_resp.text[:300] if order_resp.text else "Unknown error"
                raise HTTPException(status_code=502, detail=f"Jupiter order failed: {error_detail}")

            order_data = order_resp.json()

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Jupiter timeout")

    if not order_data.get("transaction"):
        raise HTTPException(status_code=502, detail="Jupiter returned no transaction")

    out_raw = int(order_data.get("outAmount", 0))

    return {
        "transaction": order_data["transaction"],
        "request_id": order_data.get("requestId"),
        "input_token": req.input_token,
        "input_amount": req.amount,
        "output_token": req.output_token,
        "output_amount": out_raw / (10 ** output_info["decimals"]),
        "recipient_wallet": recipient_wallet_effective,
        "is_self_swap": is_self_swap,
        "router": order_data.get("router", "unknown"),
        "mode": order_data.get("mode", "unknown"),
        "fee_bps": order_data.get("feeBps", 0),
        "swap_api_version": "v2",
    }


@router.post("/execute")
async def execute_swap(req: ExecuteRequest):
    """
    Submit signed swap transaction to Jupiter /execute for managed landing.
    Jupiter handles: optimized slippage (RTSE), priority fees, transaction landing, confirmation.

    Required: signedTransaction (base64) + requestId (from /order response)
    Based on: https://dev.jup.ag/docs/swap/order-and-execute

    Response: { status, signature, code, inputAmountResult, outputAmountResult }
    """
    if not req.signed_transaction:
        raise HTTPException(status_code=400, detail="signed_transaction is required")
    if not req.request_id:
        raise HTTPException(status_code=400, detail="request_id is required")

    headers = {"Content-Type": "application/json"}
    if JUPITER_API_KEY:
        headers["x-api-key"] = JUPITER_API_KEY

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                JUPITER_EXECUTE_URL,
                json={
                    "signedTransaction": req.signed_transaction,
                    "requestId": req.request_id,
                },
                headers=headers,
            )
            if resp.status_code != 200:
                error_detail = resp.text[:300] if resp.text else "Unknown error"
                raise HTTPException(status_code=502, detail=f"Jupiter execute failed: {error_detail}")

            result = resp.json()

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Jupiter execute timeout")

    return {
        "status": result.get("status"),
        "signature": result.get("signature"),
        "code": result.get("code", -1),
        "input_amount_result": result.get("inputAmountResult"),
        "output_amount_result": result.get("outputAmountResult"),
        "error": result.get("error"),
        "swap_api_version": "v2",
    }


# ── 5. Natural Language Intent Parser (LLM) ──
class ParseRequest(BaseModel):
    input: str
    sender_wallet: Optional[str] = None


@router.post("/parse")
async def parse_intent(req: ParseRequest):
    """
    Parse natural language input into structured transaction intent.
    Uses GPT-4o-mini for multi-language support (EN + ID).

    Examples:
      "send 1 SOL to @blockid"
      "kirim 100 USDC ke @bee121"
      "bayar @blockid 50 dolar"
      "siapa @blockid?"
      "berapa saldo aku?"
      "tukar 1 SOL ke USDC"
      "send half my SOL to @blockid"
    """
    if not req.input or not req.input.strip():
        raise HTTPException(status_code=400, detail="Input cannot be empty")

    if not openai_client:
        raise HTTPException(status_code=503, detail="OpenAI not configured")

    user_input = req.input.strip()

    try:
        response = await openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": PARSE_SYSTEM_PROMPT},
                {"role": "user", "content": user_input},
            ],
            temperature=0.1,
            max_tokens=200,
            response_format={"type": "json_object"},
        )

        raw = (response.choices[0].message.content or "").strip()
        parsed = json.loads(raw)

        intent = parsed.get("intent", "unknown")
        handle = parsed.get("handle")
        amount = parsed.get("amount")
        token = parsed.get("token")
        output_token = parsed.get("output_token")
        confidence = parsed.get("confidence", 0.5)

        if handle:
            handle = handle.strip().lower().lstrip("@")

        if isinstance(amount, str) and amount not in ("HALF", "MAX", "QUARTER"):
            try:
                amount = float(amount)
            except ValueError:
                amount = None

        resolved = None
        if handle and intent in ("send", "check"):
            conn = await get_conn()
            try:
                row = await conn.fetchrow(
                    "SELECT owner_wallet FROM handle_registry WHERE LOWER(handle) = $1 AND status = 'ACTIVE' LIMIT 1",
                    handle,
                )
                if row:
                    resolved = row["owner_wallet"]
            finally:
                await release_conn(conn)

        return {
            "intent": intent,
            "handle": handle,
            "handle_resolved": resolved,
            "amount": amount,
            "token": token,
            "output_token": output_token,
            "confidence": confidence,
            "raw_input": user_input,
            "needs_more_info": (
                (intent == "send" and (not handle or amount is None))
                or (intent == "swap" and (amount is None or not token or not output_token))
            ),
        }

    except json.JSONDecodeError:
        return {
            "intent": "unknown",
            "handle": None,
            "handle_resolved": None,
            "amount": None,
            "token": None,
            "output_token": None,
            "confidence": 0.0,
            "raw_input": user_input,
            "needs_more_info": True,
            "error": "Failed to parse LLM response",
        }
    except openai.APIError as e:
        raise HTTPException(status_code=502, detail=f"OpenAI API error: {str(e)}")


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
