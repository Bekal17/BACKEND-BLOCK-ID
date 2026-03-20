"""Google Vision API integration for image moderation."""
from __future__ import annotations

import base64
import os

import httpx

from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

VISION_API_KEY = os.getenv("GOOGLE_VISION_API_KEY", "")
VISION_API_URL = "https://vision.googleapis.com/v1/images:annotate"

BLOCKED_LIKELIHOOD = {"LIKELY", "VERY_LIKELY"}


async def check_image_safe(image_bytes: bytes) -> dict:
    """
    Check image safety using Google Vision API Safe Search.

    Returns:
    {
        "safe": bool,
        "reason": str | None,
        "details": dict  -- raw safe search scores
    }

    Safe Search categories checked:
    - adult: explicit sexual content
    - violence: violent content
    - racy: suggestive content
    - medical: medical/gory imagery

    If GOOGLE_VISION_API_KEY not set -> return {"safe": True} (skip check)
    If API call fails -> return {"safe": True} (fail open, log warning)
    """
    if not VISION_API_KEY:
        logger.warning("vision_api_key_not_set, skipping moderation")
        return {"safe": True, "reason": None, "details": {}}

    try:
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")

        payload = {
            "requests": [{
                "image": {"content": image_b64},
                "features": [{"type": "SAFE_SEARCH_DETECTION"}],
            }]
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(
                f"{VISION_API_URL}?key={VISION_API_KEY}",
                json=payload,
            )
            res.raise_for_status()
            data = res.json()

        annotation = (
            data.get("responses", [{}])[0]
            .get("safeSearchAnnotation", {})
        )

        details = {
            "adult": annotation.get("adult", "UNKNOWN"),
            "violence": annotation.get("violence", "UNKNOWN"),
            "racy": annotation.get("racy", "UNKNOWN"),
            "medical": annotation.get("medical", "UNKNOWN"),
        }

        # Check if any category is blocked
        blocked_categories = [
            cat for cat, score in details.items()
            if score in BLOCKED_LIKELIHOOD
        ]

        if blocked_categories:
            reason = f"Image contains inappropriate content: {', '.join(blocked_categories)}"
            logger.warning("vision_moderation_blocked", categories=blocked_categories)
            return {"safe": False, "reason": reason, "details": details}

        return {"safe": True, "reason": None, "details": details}

    except httpx.HTTPStatusError as e:
        logger.warning("vision_api_http_error", status=e.response.status_code)
        return {"safe": True, "reason": None, "details": {}}
    except Exception as e:
        logger.warning("vision_api_error", error=str(e))
        return {"safe": True, "reason": None, "details": {}}


async def check_image_safe_from_url(image_url: str) -> dict:
    """
    Check image safety from URL instead of bytes.
    Used for checking existing uploaded images.
    """
    if not VISION_API_KEY:
        return {"safe": True, "reason": None, "details": {}}

    try:
        payload = {
            "requests": [{
                "image": {"source": {"imageUri": image_url}},
                "features": [{"type": "SAFE_SEARCH_DETECTION"}],
            }]
        }

        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(
                f"{VISION_API_URL}?key={VISION_API_KEY}",
                json=payload,
            )
            res.raise_for_status()
            data = res.json()

        annotation = (
            data.get("responses", [{}])[0]
            .get("safeSearchAnnotation", {})
        )

        details = {
            "adult": annotation.get("adult", "UNKNOWN"),
            "violence": annotation.get("violence", "UNKNOWN"),
            "racy": annotation.get("racy", "UNKNOWN"),
            "medical": annotation.get("medical", "UNKNOWN"),
        }

        blocked_categories = [
            cat for cat, score in details.items()
            if score in BLOCKED_LIKELIHOOD
        ]

        if blocked_categories:
            reason = f"Image contains inappropriate content: {', '.join(blocked_categories)}"
            return {"safe": False, "reason": reason, "details": details}

        return {"safe": True, "reason": None, "details": details}

    except Exception as e:
        logger.warning("vision_api_url_error", error=str(e))
        return {"safe": True, "reason": None, "details": {}}
