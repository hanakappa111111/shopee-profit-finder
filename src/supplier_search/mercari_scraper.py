"""Japan Supplier Search AI — Mercari marketplace adapter.

Wraps ``src.japan_source.mercari_scraper.MercariScraper`` and adds
product_key enrichment.  Mercari is a secondary-market source; products
are typically listed as used/like-new condition.

Note: The underlying MercariScraper has limited effectiveness with static
HTML parsing due to Mercari's React-rendered frontend.  Results may be
sparse until a Playwright-based scraper is implemented.
"""

from __future__ import annotations

from typing import Any

from src.supplier_search.base_scraper import BaseMarketplaceAdapter


class MercariAdapter(BaseMarketplaceAdapter):
    """Mercari Japan marketplace adapter."""

    marketplace_name = "Mercari"

    def _get_inner_scraper(self) -> Any:
        from src.japan_source.mercari_scraper import MercariScraper
        return MercariScraper()

    def _transform_query(self, query: str) -> str:
        """Mercari's search works best with short, precise Japanese keywords.

        Strip English marketing phrases that won't match Mercari listings.
        """
        q = query.strip()
        if len(q) > 80:
            q = q[:80].rsplit(" ", 1)[0]
        return q
