"""
Stripe configuration for BlockID billing.
"""
from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
except Exception:
    pass

STRIPE_SECRET_KEY = (os.getenv("STRIPE_SECRET_KEY") or "").strip()
STRIPE_WEBHOOK_SECRET = (os.getenv("STRIPE_WEBHOOK_SECRET") or "").strip()
STRIPE_PUBLISHABLE_KEY = (os.getenv("STRIPE_PUBLISHABLE_KEY") or "").strip()

# For checkout session success/cancel URLs
BILLING_SUCCESS_URL = (os.getenv("BILLING_SUCCESS_URL") or "https://app.blockidscore.fun/billing/success").strip()
BILLING_CANCEL_URL = (os.getenv("BILLING_CANCEL_URL") or "https://app.blockidscore.fun/billing").strip()
