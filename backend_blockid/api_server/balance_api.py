"""
Wallet balance API — fetch SOL + token balances via Helius.
"""
from __future__ import annotations

import os

import httpx
from fastapi import APIRouter, HTTPException

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["Balance"])

HELIUS_API_KEY = (os.getenv("HELIUS_API_KEY") or "").strip()
HELIUS_BASE = (os.getenv("HELIUS_BASE") or "https://api.helius.xyz").rstrip("/")
HELIUS_RPC = (
    os.getenv("HELIUS_RPC_URL")
    or f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
)

SOL_MINT = "So11111111111111111111111111111111111111112"


@router.get("/wallet/{wallet}/balance")
async def get_wallet_balance(wallet: str):
    """
    Get wallet SOL balance and top token balances.
    Uses Helius Enhanced API.

    Returns:
    {
        "wallet": str,
        "sol_balance": float,      # SOL balance
        "sol_usd_value": float,    # USD value (if available)
        "tokens": [
            {
                "symbol": str,
                "name": str,
                "balance": float,
                "decimals": int,
                "mint": str,
                "logo_uri": str | None,
                "usd_value": float | None,
            }
        ],
        "total_usd_value": float,
    }
    """
    wallet = (wallet or "").strip()
    if not wallet:
        raise HTTPException(status_code=400, detail="wallet required")

    if not HELIUS_API_KEY:
        raise HTTPException(status_code=503, detail="Helius API not configured")

    sol_balance = 0.0
    sol_usd = 0.0
    total_usd = 0.0
    tokens: list[dict] = []

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            # 1. Get SOL balance via Helius RPC
            sol_resp = await client.post(
                HELIUS_RPC,
                json={
                    "jsonrpc": "2.0",
                    "id": "blockid-balance",
                    "method": "getBalance",
                    "params": [wallet],
                },
            )
            sol_data = sol_resp.json()
            sol_result = sol_data.get("result")
            if isinstance(sol_result, dict):
                sol_lamports = sol_result.get("value", 0) or 0
            else:
                sol_lamports = sol_result if sol_result is not None else 0
            sol_balance = float(sol_lamports or 0) / 1e9

            # 2. Get token balances via Helius API (v1 wallet balances)
            token_resp = await client.get(
                f"{HELIUS_BASE}/v1/wallet/{wallet}/balances"
                f"?api-key={HELIUS_API_KEY}&showNative=true"
            )

            if token_resp.status_code == 200:
                token_data = token_resp.json()
                raw_balances = token_data.get("balances", [])
                total_usd = float(token_data.get("totalUsdValue", 0) or 0)

                for t in raw_balances:
                    mint = t.get("mint") or ""
                    bal = float(t.get("balance", 0) or 0)
                    decimals = int(t.get("decimals", 0) or 0)
                    # Helius v1 returns balance already adjusted for decimals
                    actual_bal = bal

                    if mint == SOL_MINT:
                        sol_usd = float(t.get("usdValue", 0) or 0)
                        continue

                    if actual_bal <= 0:
                        continue

                    usd_val = float(t.get("usdValue", 0) or 0)
                    if usd_val <= 0:
                        usd_val = float(t.get("pricePerToken", 0) or 0) * actual_bal

                    tokens.append({
                        "symbol": t.get("symbol") or "Unknown",
                        "name": t.get("name") or "",
                        "balance": round(actual_bal, 6),
                        "decimals": decimals,
                        "mint": mint,
                        "logo_uri": t.get("logoUri") or None,
                        "usd_value": round(usd_val, 2) if usd_val > 0 else None,
                    })

                # Sort by USD value descending
                tokens.sort(key=lambda x: x.get("usd_value") or 0, reverse=True)
                # Limit to top 10 tokens
                tokens = tokens[:10]

                # If sol_usd not from SOL token in balances, estimate from total
                if sol_usd <= 0 and sol_balance > 0:
                    # Fallback: use price from first token or leave 0
                    pass

        return {
            "wallet": wallet,
            "sol_balance": round(sol_balance, 6),
            "sol_usd_value": round(sol_usd, 2),
            "tokens": tokens,
            "total_usd_value": round(total_usd, 2),
        }

    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Helius API timeout")
    except Exception as e:
        logger.warning("wallet_balance_error", wallet=wallet[:16], error=str(e))
        raise HTTPException(status_code=500, detail=f"Failed to fetch balance: {str(e)}")
