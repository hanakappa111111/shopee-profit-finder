"""Shared scraper utilities — resilience layer for all HTTP scrapers.

Provides:

* **User-Agent rotation** — a pool of realistic browser UAs cycled per request
  to reduce fingerprinting.
* **CAPTCHA / block detection** — lightweight heuristics that check response
  bodies for common challenge keywords.
* **Adaptive back-off** — automatically increases delay when throttled or
  blocked and resets after successful requests.
* **Proxy support** — optional proxy configuration via settings.
* **Resilient session factory** — pre-configured ``requests.Session`` with
  retries, timeouts, and randomised UA.
"""

from __future__ import annotations

import random
import time
from typing import Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config.settings import settings
from src.utils.logger import logger


# ── User-Agent pool ──────────────────────────────────────────────────────────

_USER_AGENTS: List[str] = [
    # Chrome (Win)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    # Chrome (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    # Firefox (Win)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Firefox (Mac)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:125.0) Gecko/20100101 Firefox/125.0",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]


def random_ua() -> str:
    """Return a random User-Agent string from the pool."""
    return random.choice(_USER_AGENTS)


# ── CAPTCHA / block detection ────────────────────────────────────────────────

_BLOCK_SIGNALS: List[str] = [
    "captcha",
    "robot",
    "automated",
    "blocked",
    "access denied",
    "please verify",
    "are you a human",
    "security check",
    "cf-browser-verification",
    "just a moment",
    "checking your browser",
    "unusual traffic",
]


def is_blocked(response: requests.Response) -> bool:
    """Heuristic check for CAPTCHA / bot-block pages.

    Returns ``True`` if the response body contains common block signals
    AND the response is suspiciously short (< 50 KB) — real search result
    pages are typically much larger.
    """
    if response.status_code in (403, 429, 503):
        return True

    content_length = len(response.content)
    if content_length > 50_000:
        # Large pages are unlikely to be block pages.
        return False

    body_lower = response.text[:5_000].lower()
    hits = sum(1 for signal in _BLOCK_SIGNALS if signal in body_lower)
    return hits >= 2


# ── Adaptive delay ───────────────────────────────────────────────────────────


class AdaptiveDelay:
    """Auto-adjusting delay that backs off on errors and resets on success.

    Parameters
    ----------
    base_delay:
        Normal inter-request delay in seconds.
    max_delay:
        Upper bound on delay after repeated failures.
    backoff_factor:
        Multiplier applied to the current delay on each failure.
    """

    def __init__(
        self,
        base_delay: float = 2.0,
        max_delay: float = 60.0,
        backoff_factor: float = 2.0,
    ) -> None:
        self._base = base_delay
        self._max = max_delay
        self._factor = backoff_factor
        self._current = base_delay
        self._consecutive_failures = 0

    def wait(self) -> None:
        """Sleep for the current delay (with ±20% jitter)."""
        jitter = self._current * random.uniform(-0.2, 0.2)
        actual = max(0.5, self._current + jitter)
        time.sleep(actual)

    def on_success(self) -> None:
        """Reset delay after a successful request."""
        self._consecutive_failures = 0
        self._current = self._base

    def on_failure(self) -> None:
        """Increase delay after a failed / blocked request."""
        self._consecutive_failures += 1
        self._current = min(self._current * self._factor, self._max)
        logger.debug(
            f"[AdaptiveDelay] Failure #{self._consecutive_failures} — "
            f"delay increased to {self._current:.1f}s"
        )

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures


# ── Resilient session factory ────────────────────────────────────────────────


def create_session(
    *,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    timeout: float = 15.0,
    proxy: Optional[str] = None,
) -> requests.Session:
    """Create a ``requests.Session`` with retry, timeout, and random UA.

    Parameters
    ----------
    max_retries:
        Automatic retries on 429/500/502/503/504.
    backoff_factor:
        urllib3 exponential back-off factor between retries.
    timeout:
        Default request timeout in seconds.
    proxy:
        Optional HTTP/HTTPS proxy URL.  Falls back to
        ``settings.SCRAPER_PROXY`` if set.

    Returns
    -------
    requests.Session
        Ready-to-use session.
    """
    session = requests.Session()

    # Retry policy
    retry = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Headers
    session.headers.update({
        "User-Agent": random_ua(),
        "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    # Proxy
    proxy_url = proxy or getattr(settings, "SCRAPER_PROXY", "")
    if proxy_url:
        session.proxies = {"http": proxy_url, "https": proxy_url}

    return session


# ── Rakuten API helper ───────────────────────────────────────────────────────


def search_rakuten_api(
    query: str,
    *,
    limit: int = 5,
    app_id: Optional[str] = None,
) -> List[Dict]:
    """Search Rakuten Ichiba via the official API (free tier: 30 req/sec).

    Returns raw API item dicts.  Returns an empty list on error so callers
    can fall back to HTML scraping transparently.

    Requires ``RAKUTEN_APP_ID`` in settings / environment.  If not set,
    returns an empty list immediately.
    """
    api_key = app_id or getattr(settings, "RAKUTEN_APP_ID", "")
    if not api_key:
        return []

    url = "https://app.rakuten.co.jp/services/api/IchibaItem/Search/20220601"
    params = {
        "format": "json",
        "keyword": query,
        "applicationId": api_key,
        "hits": min(limit, 30),
        "sort": "+itemPrice",
        "genreId": 0,
    }

    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("Items", [])
    except Exception as exc:
        logger.debug(f"[RakutenAPI] search failed: {exc}")
        return []
