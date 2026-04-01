"""Lightweight in-memory cache with TTL for scraping results.

Prevents redundant Shopee scrapes when the same keyword is researched
multiple times within a short window.  Also caches exchange rates
and supplier search results.

The cache is process-scoped — it resets when the process exits.
Thread-safe via ``threading.Lock``.
"""

from __future__ import annotations

import hashlib
import threading
import time
from typing import Any, Dict, Optional

from src.utils.logger import logger


class TTLCache:
    """Simple thread-safe in-memory cache with per-key TTL.

    Parameters
    ----------
    default_ttl:
        Default time-to-live in seconds for cached entries.
        Individual ``put()`` calls can override this.
    max_entries:
        Maximum number of entries before the oldest are evicted.
    """

    def __init__(
        self,
        default_ttl: float = 600.0,
        max_entries: int = 500,
    ) -> None:
        self._ttl = default_ttl
        self._max = max_entries
        self._store: Dict[str, tuple[float, Any]] = {}  # key → (expires_at, value)
        self._lock = threading.Lock()

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a cached value.  Returns ``None`` on miss or expiry."""
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if time.time() > expires_at:
                del self._store[key]
                return None
            return value

    def put(self, key: str, value: Any, *, ttl: Optional[float] = None) -> None:
        """Store a value with optional custom TTL."""
        with self._lock:
            # Evict oldest if at capacity
            if len(self._store) >= self._max and key not in self._store:
                oldest_key = min(self._store, key=lambda k: self._store[k][0])
                del self._store[oldest_key]

            expires = time.time() + (ttl if ttl is not None else self._ttl)
            self._store[key] = (expires, value)

    def invalidate(self, key: str) -> None:
        """Remove a specific key."""
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        """Remove all entries."""
        with self._lock:
            self._store.clear()
            logger.debug("[Cache] Cleared all entries")

    @property
    def size(self) -> int:
        return len(self._store)


def make_cache_key(*parts: str) -> str:
    """Deterministic cache key from arbitrary string parts."""
    raw = "|".join(str(p).strip().lower() for p in parts)
    return hashlib.md5(raw.encode()).hexdigest()


# ── Global caches (module-level singletons) ──────────────────────────────────

# Shopee scrape results — 10 min TTL
scrape_cache = TTLCache(default_ttl=600.0, max_entries=100)

# Japan supplier search results — 30 min TTL
supplier_cache = TTLCache(default_ttl=1800.0, max_entries=500)

# Exchange rate — 1 hour TTL
fx_cache = TTLCache(default_ttl=3600.0, max_entries=10)
