"""Japan Supplier Search AI — Amazon Japan marketplace adapter.

Wraps ``src.japan_source.amazon_scraper.AmazonJapanScraper`` and adds
product_key enrichment.  Amazon is the primary source for new/sealed TCG
and figure products with the most reliable stock and pricing data.
"""

from __future__ import annotations

from typing import Any

from src.supplier_search.base_scraper import BaseMarketplaceAdapter


class AmazonAdapter(BaseMarketplaceAdapter):
    """Amazon Japan marketplace adapter."""

    marketplace_name = "Amazon JP"

    def _get_inner_scraper(self) -> Any:
        from src.japan_source.amazon_scraper import AmazonJapanScraper
        return AmazonJapanScraper()

    def _transform_query(self, query: str) -> str:
        """Amazon JP works best with concise queries.

        Strip excessive length — Amazon's search box truncates beyond ~100
        chars and returns worse results with very long queries.
        """
        q = query.strip()
        if len(q) > 100:
            q = q[:100].rsplit(" ", 1)[0]
        return q
