"""
Billing middleware: usage tracking and rate limit enforcement.

When Authorization: Bearer <api_key> is present:
- Checks plan limits before request
- Increments usage after successful response
- Returns 429 when over limit
"""
from __future__ import annotations

import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

from backend_blockid.api_server.billing_service import (
    get_customer,
    get_or_create_usage,
    check_limit,
    increment_usage,
    get_plan_limits,
)

BILLING_ENABLED = (os.getenv("BILLING_ENABLED") or "0").strip().lower() in ("1", "true", "yes")


def _extract_api_key(request: Request) -> str | None:
    auth = request.headers.get("Authorization") or ""
    if auth.startswith("Bearer "):
        return auth[7:].strip()
    return None


def _is_billable_path(path: str) -> tuple[bool, str]:
    if "/wallet/" in path and "/report" not in path and "/graph" not in path and "/investigation" not in path:
        if "/batch" in path or "batch_check" in path:
            return True, "batch_checks"
        return True, "wallet_checks"
    if "/report" in path and path.endswith("/report"):
        return True, "reports"
    return False, ""


class BillingMiddleware(BaseHTTPMiddleware):
    """Track API usage and enforce rate limits when BILLING_ENABLED=1."""

    async def dispatch(self, request: Request, call_next) -> Response:
        if not BILLING_ENABLED:
            return await call_next(request)

        api_key = _extract_api_key(request)
        path = request.url.path
        billable, metric = _is_billable_path(path)

        if not api_key or not billable:
            return await call_next(request)

        customer = get_customer(api_key)
        plan = (customer.get("plan") or "starter") if customer else "starter"
        usage = get_or_create_usage(api_key)

        if metric == "wallet_checks":
            current = usage.get("wallet_checks", 0)
            batch = 1
        elif metric == "batch_checks":
            current = usage.get("batch_checks", 0)
            batch = 1
        else:
            current = usage.get("reports_generated", 0)
            batch = 1

        limits = get_plan_limits(plan)
        if metric == "batch_checks" and not limits.get("batch_enabled", False):
            return JSONResponse(
                status_code=403,
                content={"error": "batch_not_enabled", "detail": "Upgrade to Growth plan for batch check."},
            )

        limit_metric = "reports" if metric == "reports" else "wallet_checks"
        ok, remaining = check_limit(plan, limit_metric, current, batch)
        if not ok:
            return JSONResponse(
                status_code=429,
                content={
                    "error": "rate_limit",
                    "detail": f"Plan limit exceeded for {metric}. Upgrade at blockidscore.fun/billing.",
                    "remaining": 0,
                },
            )

        response = await call_next(request)

        if 200 <= response.status_code < 300:
            if metric == "wallet_checks":
                increment_usage(api_key, wallet_checks=batch)
            elif metric == "batch_checks":
                increment_usage(api_key, batch_checks=batch)
            else:
                increment_usage(api_key, reports=batch)

        return response
