"""
BlockID Billing API — Stripe checkout, webhooks, usage.
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, Header
from pydantic import BaseModel, Field

from backend_blockid.blockid_logging import get_logger
from backend_blockid.config.stripe_settings import (
    STRIPE_SECRET_KEY,
    STRIPE_WEBHOOK_SECRET,
    BILLING_SUCCESS_URL,
    BILLING_CANCEL_URL,
)
from backend_blockid.api_server.billing_service import (
    get_customer,
    get_or_create_usage,
    get_plan_limits,
    increment_usage,
    update_customer_plan,
    upsert_customer,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/billing", tags=["billing"])


class CreateCheckoutRequest(BaseModel):
    plan: str = Field(..., description="starter | growth | enterprise")
    api_key: str = Field(..., description="API key to associate with subscription")
    success_url: str | None = None
    cancel_url: str | None = None


@router.post("/create_checkout_session")
def create_checkout_session(req: CreateCheckoutRequest) -> dict:
    """
    Create Stripe Checkout session. Returns checkout URL.
    """
    if not STRIPE_SECRET_KEY:
        raise HTTPException(status_code=503, detail="Stripe not configured")

    plan = (req.plan or "").strip().lower()
    if plan not in ("starter", "growth", "enterprise"):
        raise HTTPException(status_code=400, detail="Invalid plan. Use starter, growth, or enterprise.")

    limits = get_plan_limits(plan)
    price_id = limits.get("stripe_price_id")
    if plan == "starter" or not price_id:
        raise HTTPException(status_code=400, detail="Starter is free. Use growth or enterprise for checkout.")

    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY

        customer = get_customer(req.api_key)
        stripe_customer_id = customer.get("stripe_customer_id") if customer else None

        session = stripe.checkout.Session.create(
            mode="subscription",
            customer=stripe_customer_id,
            line_items=[{"price": price_id, "quantity": 1}],
            success_url=req.success_url or BILLING_SUCCESS_URL,
            cancel_url=req.cancel_url or BILLING_CANCEL_URL,
            metadata={"api_key": req.api_key, "plan": plan},
        )
        return {
            "checkout_url": session.url,
            "session_id": session.id,
        }
    except Exception as e:
        logger.warning("billing_checkout_error", plan=plan, error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post("/webhook")
async def stripe_webhook(request: Request, stripe_signature: str | None = Header(None)) -> dict:
    """
    Stripe webhook. Handles checkout.session.completed, invoice.paid, customer.subscription.deleted.
    """
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(status_code=503, detail="Webhook secret not configured")

    payload = await request.body()
    sig = stripe_signature or ""

    try:
        import stripe
        stripe.api_key = STRIPE_SECRET_KEY
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK_SECRET)
    except Exception as e:
        logger.warning("billing_webhook_signature_error", error=str(e))
        raise HTTPException(status_code=400, detail="Invalid signature") from e

    if event.type == "checkout.session.completed":
        sess = event.data.object
        meta = getattr(sess, "metadata", None) or {}
        api_key = meta.get("api_key") or ""
        plan = meta.get("plan") or "growth"
        cust_id = getattr(sess, "customer", None)
        if api_key:
            upsert_customer(api_key, cust_id, plan)
            logger.info("billing_subscription_created", api_key=api_key[:16] + "...", plan=plan)

    elif event.type == "invoice.paid":
        inv = event.data.object
        cust_id = getattr(inv, "customer", None)
        if cust_id:
            update_customer_plan(None, cust_id, "growth")
            logger.info("billing_invoice_paid", customer=cust_id[:20] if cust_id else "")

    elif event.type == "customer.subscription.deleted":
        sub = event.data.object
        cust_id = getattr(sub, "customer", None)
        if cust_id:
            update_customer_plan(None, cust_id, "starter")
            logger.info("billing_subscription_deleted", customer=cust_id[:20] if cust_id else "")

    return {"received": True}


@router.get("/usage/{api_key}")
def get_usage(api_key: str) -> dict:
    """
    Admin dashboard: plan, usage, remaining.
    """
    customer = get_customer(api_key)
    plan = (customer.get("plan") or "starter") if customer else "starter"
    usage = get_or_create_usage(api_key)
    limits = get_plan_limits(plan)

    wallet_limit = limits.get("wallet_checks_limit", 10000)
    reports_limit = limits.get("reports_limit", 0)

    wallet_remaining = -1 if wallet_limit < 0 else max(0, wallet_limit - (usage.get("wallet_checks") or 0))
    reports_remaining = -1 if reports_limit < 0 else max(0, reports_limit - (usage.get("reports_generated") or 0))

    return {
        "plan": plan,
        "usage": {
            "wallet_checks": usage.get("wallet_checks", 0),
            "batch_checks": usage.get("batch_checks", 0),
            "reports_generated": usage.get("reports_generated", 0),
        },
        "remaining": {
            "wallet_checks": wallet_remaining,
            "reports": reports_remaining,
        },
        "limits": {
            "wallet_checks": wallet_limit,
            "reports": reports_limit,
        },
    }
