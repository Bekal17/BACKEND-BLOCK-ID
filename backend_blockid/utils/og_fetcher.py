"""
Fetch Open Graph metadata from a URL.
Used to generate link preview cards for social posts.
"""

import re
from typing import Optional, Dict
from urllib.parse import urlparse

import httpx
from backend_blockid.blockid_logging import get_logger

logger = get_logger(__name__)

# Domains we refuse to fetch (privacy/security)
BLOCKED_DOMAINS = {
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "10.",
    "192.168.",
    "172.16.",
}

URL_REGEX = re.compile(
    r'https?://[^\s<>"\')\]]+',
    re.IGNORECASE,
)


def extract_first_url(text: str) -> Optional[str]:
    """Extract the first HTTP/HTTPS URL from text."""
    match = URL_REGEX.search(text)
    if not match:
        return None
    url = match.group(0)
    # Clean trailing punctuation
    while url and url[-1] in ".,;:!?)>]}":
        url = url[:-1]
    return url


def _is_blocked(url: str) -> bool:
    """Check if URL points to a blocked/private domain."""
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        for blocked in BLOCKED_DOMAINS:
            if host == blocked or host.startswith(blocked):
                return True
        return False
    except Exception:
        return True


def _parse_og_tags(html: str) -> Dict[str, Optional[str]]:
    """Parse OG meta tags from HTML without BeautifulSoup (lightweight)."""
    result: Dict[str, Optional[str]] = {
        "title": None,
        "description": None,
        "image": None,
    }

    # Try og:title
    m = re.search(
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']*)["\']',
        html,
        re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+property=["\']og:title["\']',
            html,
            re.IGNORECASE,
        )
    if m:
        result["title"] = m.group(1).strip()[:200]

    # Try og:description
    m = re.search(
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']*)["\']',
        html,
        re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+property=["\']og:description["\']',
            html,
            re.IGNORECASE,
        )
    if m:
        result["description"] = m.group(1).strip()[:500]

    # Try og:image
    m = re.search(
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']*)["\']',
        html,
        re.IGNORECASE,
    )
    if not m:
        m = re.search(
            r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+property=["\']og:image["\']',
            html,
            re.IGNORECASE,
        )
    if m:
        result["image"] = m.group(1).strip()[:1000]

    # Fallback: <title> tag if no og:title
    if not result["title"]:
        m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.IGNORECASE)
        if m:
            result["title"] = m.group(1).strip()[:200]

    # Fallback: meta description if no og:description
    if not result["description"]:
        m = re.search(
            r'<meta[^>]+name=["\']description["\'][^>]+content=["\']([^"\']*)["\']',
            html,
            re.IGNORECASE,
        )
        if not m:
            m = re.search(
                r'<meta[^>]+content=["\']([^"\']*)["\'][^>]+name=["\']description["\']',
                html,
                re.IGNORECASE,
            )
        if m:
            result["description"] = m.group(1).strip()[:500]

    return result


async def fetch_og_metadata(url: str) -> Optional[Dict[str, Optional[str]]]:
    """
    Fetch OG metadata from a URL.
    Returns dict with keys: url, title, description, image
    Returns None if fetch fails or URL is blocked.
    """
    if not url or not url.startswith("http"):
        return None

    if _is_blocked(url):
        return None

    try:
        async with httpx.AsyncClient(
            timeout=8.0,
            follow_redirects=True,
            headers={
                "User-Agent": "BlockID-Bot/1.0 (link preview)",
                "Accept": "text/html",
            },
        ) as client:
            resp = await client.get(url)

        content_type = resp.headers.get("content-type", "")
        if "text/html" not in content_type:
            return None

        # Only parse first 50KB to avoid huge pages
        html = resp.text[:50_000]
        og = _parse_og_tags(html)

        if not og["title"] and not og["description"]:
            return None

        parsed = urlparse(url)
        return {
            "url": url,
            "title": og["title"],
            "description": og["description"],
            "image": og["image"],
            "domain": parsed.hostname or "",
        }

    except Exception as e:
        logger.debug("og_fetch_failed", url=url[:100], error=str(e))
        return None
