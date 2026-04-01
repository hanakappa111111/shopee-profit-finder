"""Japan Supplier Search AI — Yahoo Shopping marketplace adapter.

Wraps ``src.japan_source.yahoo_scraper.YahooShoppingScraper`` and adds
product_key enrichment.  Yahoo Shopping Japan often has auction-style
listings with lower prices for in-demand collectibles.
"""

from __future__ import annotations

from typing import Any

from src.supplier_search.base_scraper import BaseMarketplaceAdapter


class YahooAdapter(BaseMarketplaceAdapter):
    """Yahoo Shopping Japan marketplace adapter."""

    marketplace_name = "Yahoo Shopping"

    def _get_inner_scraper(self) -> Any:
        from src.japan_source.yahoo_scraper import YahooShoppingScraper
        return YahooShoppingScraper()

    def _transform_query(self, query: str) -> str:
        """Yahoo Shopping handles both English and Japanese queries well.

        Cap length to avoid truncation.
        """
        q = query.strip()
        if len(q) > 120:
            q = q[:120].rsplit(" ", 1)[0]
        return q
