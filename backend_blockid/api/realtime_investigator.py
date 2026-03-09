"""
Realtime Investigator Mode — streams pipeline progress to the UI via Server-Sent Events (SSE).

When a wallet is analyzed, the backend sends live progress updates:
  Step 1: fetch_tx — Fetching wallet transactions
  Step 2: build_network — Building wallet network
  Step 3: detect_drainer — Detecting drainer patterns
  Step 4: compute_score — Computing trust score
  done — Analysis complete

Endpoint: GET /investigate/{wallet}
"""

from __future__ import annotations

import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from backend_blockid.oracle.realtime_wallet_pipeline import run_realtime_wallet_pipeline_streaming

router = APIRouter()


async def _sse_generator(wallet: str):
    """Yield SSE-formatted events as the pipeline progresses."""
    try:
        async for step_id, message, extra in run_realtime_wallet_pipeline_streaming(wallet):
            payload = {"step": step_id, "message": message, **extra}
            if step_id == "done":
                payload["status"] = "done"
            yield f"data: {json.dumps(payload)}\n\n"
    except Exception as e:
        yield f"data: {json.dumps({'status': 'error', 'message': str(e)})}\n\n"


@router.get("/investigate/{wallet}")
async def investigate_wallet(wallet: str):
    """Stream live pipeline progress via Server-Sent Events."""
    return StreamingResponse(
        _sse_generator(wallet.strip()),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
